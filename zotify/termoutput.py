from __future__ import annotations
from contextlib import contextmanager
from enum import Enum
from functools import partial
from itertools import cycle
from mutagen import FileType
from os import get_terminal_size, system
from platform import system
from pprint import pformat
from re import split, escape
from tabulate import tabulate
from threading import Thread
from time import sleep
from tqdm import tqdm
from tqdm.auto import tqdm as tqdmauto
from traceback import TracebackException
from urllib3 import HTTPResponse

from zotify.const import *


UP_ONE_LINE = "\033[A"
DOWN_ONE_LINE = "\033[B"
RIGHT_ONE_COL = "\033[C"
LEFT_ONE_COL = "\033[D"
START_OF_PREV_LINE = "\033[F"
CLEAR_LINE = "\033[K"


class PrintChannel(Enum):
    MANDATORY = MANDATORY
    DEBUG = DEBUG
    SPLASH = PRINT_SPLASH
    
    WARNING = PRINT_WARNINGS
    ERROR = PRINT_ERRORS
    API_ERROR = PRINT_API_ERRORS
    
    LOADER = PRINT_PROGRESS_INFO
    SKIPPING = PRINT_SKIPS
    DOWNLOADS = PRINT_DOWNLOADS


class PrintCategory(Enum):
    NONE = ""
    MANDATORY = "\n"
    LOADER = "\n\t"
    LOADER_CYCLE = f"{START_OF_PREV_LINE*2}\t"
    HASHTAG = "\n###   "
    JSON = "\n#"
    DEBUG = "\nDEBUG\n"


class Printer:
    LAST_PRINT: PrintCategory = PrintCategory.NONE
    ACTIVE_LOADER: Loader | None = None
    ACTIVE_PBARS: list[tqdm] = []
    
    # Helpers
    @staticmethod
    def _term_cols() -> int:
        try:
            columns, _ = get_terminal_size()
        except OSError:
            columns = 80
        return columns
    
    @staticmethod
    def _prefixes(msg: str, category: PrintCategory, channel: PrintChannel) -> tuple[str, PrintCategory]:
        if category is PrintCategory.HASHTAG:
            if channel in {PrintChannel.WARNING, PrintChannel.ERROR, PrintChannel.API_ERROR,
                           PrintChannel.SKIPPING,}:
                msg = channel.name + ":  " + msg
            msg =  msg.replace("\n", "   ###\n###   ") + "   ###"
            if channel is PrintChannel.DEBUG:
                msg = category.value.replace("\n", "", 1) + msg
                category = PrintCategory.DEBUG
        elif category is PrintCategory.JSON:
            msg = "#" * (Printer._term_cols()-1) + "\n" + msg + "\n" + "#" * Printer._term_cols()
        
        if Printer.LAST_PRINT is PrintCategory.DEBUG and category is PrintCategory.DEBUG:
            pass
        elif Printer.LAST_PRINT in {PrintCategory.LOADER, PrintCategory.LOADER_CYCLE} and category is PrintCategory.LOADER:
            msg = "\n" + PrintCategory.LOADER_CYCLE.value + msg
        elif Printer.LAST_PRINT in {PrintCategory.LOADER, PrintCategory.LOADER_CYCLE} and "LOADER" not in category.name:
            msg = category.value.replace("\n", "", 1) + msg
        else:
            msg = category.value + msg
        
        return msg, category
    
    @staticmethod
    def _obj_shrink(obj: list | tuple | dict) -> dict:
        """ Shrinks API objects to remove data unnecessary data for debugging """
        
        def shrink(k: str) -> str:
            if k in {AVAIL_MARKETS, IMAGES}:
                return "LIST REMOVED FOR BREVITY"
            elif k in {EXTERNAL_URLS, PREVIEW_URL}:
                return "URL REMOVED FOR BREVITY"
            elif k in {"_children"}:
                return "SET REMOVED FOR BREVITY"
            elif k in {"metadata_block_picture", "APIC:0", "covr"}:
                return "BYTES REMOVED FOR BREVITY"
            return None
        
        if isinstance(obj, list) and len(obj) > 0:
            obj = [Printer._obj_shrink(item) for item in obj]
        
        elif isinstance(obj, tuple):
            if len(obj) == 2 and isinstance(obj[0], str):
                if shrink(obj[0]):
                    obj = (obj[0], shrink(obj[0]))
        
        elif isinstance(obj, (dict, FileType)):
            for k, v in obj.items():
                if shrink(k):
                    obj[k] = shrink(k)
                else:
                    obj[k] = Printer._obj_shrink(v)
        
        return obj
    
    # Primatives
    @staticmethod
    def pretty(obj, shrink: bool = False) -> str:
        if not isinstance(obj, str):
            return pformat(Printer._obj_shrink(obj) if shrink else obj)
        
        if "http" in obj:
            delims = "/?&="
        else:
            delims = "\n"
        
        pretty_str = ""
        for substr in split(fr'([{escape(delims)}])', obj):
            overshoot = len(pretty_str.split("\n")[-1]) + len(substr)
            if overshoot > int(0.75 * Printer._term_cols()) and substr not in delims:
                pretty_str += "\n"
            pretty_str += substr
        
        return pretty_str
    
    @staticmethod
    def logger(msg: str | dict, channel: PrintChannel | None = None) -> None:
        if channel in {PrintChannel.LOADER}:
            return
        from zotify.config import Zotify
        if Zotify.LOGGER is None:
            return
        if isinstance(msg, BaseException):
            msg = "".join(TracebackException.from_exception(msg).format())
        elif isinstance(msg, dict):
            msg = Printer.pretty(msg)
        msg = "\n\n" + msg.strip() + "\n"
        if channel is PrintChannel.WARNING:
            Zotify.LOGGER.warning(msg)
        elif channel in {PrintChannel.ERROR, PrintChannel.API_ERROR}:
            Zotify.LOGGER.error(msg)
        elif Zotify.CONFIG.debug():
            if channel is PrintChannel.DEBUG:
                Zotify.LOGGER.debug(msg)
            else:
                Zotify.LOGGER.info(msg)
    
    @classmethod
    @contextmanager
    def pause_loader(cls, ignore_pause: bool = False):
        if ignore_pause or not Printer.ACTIVE_LOADER or Printer.ACTIVE_LOADER.paused:
            yield; return
        Printer.ACTIVE_LOADER.pause()
        try:     yield
        finally: Printer.ACTIVE_LOADER.resume()
    
    @staticmethod
    def new_print(channel: PrintChannel, msg: str, category: PrintCategory = PrintCategory.NONE, 
                  end: str = "\n") -> None:
        Printer.logger(msg, channel)
        if channel != PrintChannel.MANDATORY:
            from zotify.config import Zotify
            if Zotify.CONFIG.get_standard_interface():
                return
        if channel == PrintChannel.MANDATORY or Zotify.CONFIG.get(channel.value):
            msg, category = Printer._prefixes(msg, category, channel)
            with Printer.pause_loader(category in {PrintCategory.LOADER, PrintCategory.LOADER_CYCLE}):
                for line in str(msg).splitlines():
                    if end == "\n":
                        tqdm.write(line.ljust(Printer._term_cols()))
                    else:
                        tqdm.write(line, end=end)
                    Printer.LAST_PRINT = category
    
    @staticmethod
    def get_input(prompt: str) -> str:
        user_input = ""
        with Printer.pause_loader():
            while len(user_input) == 0:
                Printer.new_print(PrintChannel.MANDATORY, prompt, end="")
                user_input = str(input())
        return user_input
    
    # Print Wrappers
    @staticmethod
    def debug(*msg: tuple[str | object]) -> None:
        with Printer.pause_loader():
            for m in msg:
                if isinstance(m, str):
                    Printer.new_print(PrintChannel.DEBUG, m, PrintCategory.DEBUG)
                else:
                    Printer.new_print(PrintChannel.DEBUG, Printer.pretty(m), PrintCategory.DEBUG)
    
    @staticmethod
    def hashtaged(channel: PrintChannel, msg: str) -> None:
        Printer.new_print(channel, msg, PrintCategory.HASHTAG)
    
    @staticmethod
    def json_dump(channel: PrintChannel, obj: dict) -> None:
        Printer.new_print(channel, Printer.pretty(obj), PrintCategory.JSON)
    
    @staticmethod
    def traceback(e: Exception) -> None:
        msg = "".join(TracebackException.from_exception(e).format())
        Printer.new_print(PrintChannel.ERROR, msg, PrintCategory.MANDATORY)
    
    @staticmethod
    def depreciated_warning(option_string: str, help_msg: str = None, CONFIG = True) -> None:
        Printer.hashtaged(PrintChannel.MANDATORY, 
        f"WARNING: {'CONFIG' if CONFIG else 'ARGUMENT'} `{option_string}` IS DEPRECIATED, IGNORING\n" +
        f"THIS WILL BE REMOVED IN FUTURE VERSIONS\n" +
        f"{help_msg}" if help_msg else ""
        )
    
    @staticmethod
    def table(title: str, headers: tuple[str], tabular_data: list) -> None:
        Printer.hashtaged(PrintChannel.MANDATORY, title)
        Printer.new_print(PrintChannel.MANDATORY, tabulate(tabular_data, headers=headers, tablefmt='pretty'))
    
    # Prefabs
    @staticmethod
    def clear() -> None:
        """ Clear the console window """
        if system() == WINDOWS_SYSTEM:
            system('cls')
        else:
            system('clear')
    
    @staticmethod
    def splash() -> None:
        """ Displays splash screen """
        Printer.new_print(PrintChannel.SPLASH,
        "    ███████╗ ██████╗ ████████╗██╗███████╗██╗   ██╗"+"\n"+
        "    ╚══███╔╝██╔═══██╗╚══██╔══╝██║██╔════╝╚██╗ ██╔╝"+"\n"+
        "      ███╔╝ ██║   ██║   ██║   ██║█████╗   ╚████╔╝ "+"\n"+
        "     ███╔╝  ██║   ██║   ██║   ██║██╔══╝    ╚██╔╝  "+"\n"+
        "    ███████╗╚██████╔╝   ██║   ██║██║        ██║   "+"\n"+
        "    ╚══════╝ ╚═════╝    ╚═╝   ╚═╝╚═╝        ╚═╝   "+"\n",
        PrintCategory.MANDATORY)
    
    @staticmethod
    def user_make_select_prompt(only_one: bool = False) -> None:
        """ Displays choice selection instructions to the user """
        msg = "> SELECT A SINGLE OPTION BY ID\n" +\
              "> SELECT A RANGE BY ADDING A DASH BETWEEN BOTH IDs\n" +\
              "> MAKE MULTIPLE SELECTIONS BY ADDING A COMMA BETWEEN IDs OR RANGES\n"
        Printer.new_print(PrintChannel.MANDATORY,
                          msg.splitlines()[0] if only_one else msg,
                          PrintCategory.MANDATORY)
    
    @staticmethod
    def newline() -> None:
        Printer.new_print(PrintChannel.MANDATORY, "\n")
    
    @staticmethod
    def back_up() -> None:
        Printer.new_print(PrintChannel.MANDATORY, UP_ONE_LINE, end="")
    
    # Progress Bars
    @staticmethod
    def pbar(iterable=None, desc=None, total=None, unit='it', disable=False,
              unit_scale=False, unit_divisor=1000, default_pos=1, pbar_stack=[]) -> tqdm:
        pos = default_pos
        if pbar_stack:
            pos = -pbar_stack[-1].pos + (0 if pbar_stack[-1].disable else -2)
        if iterable and len(iterable) == 1 and len(Printer.ACTIVE_PBARS) > 0:
            disable = True # minimize clutter
        new_pbar = tqdm(iterable=iterable, desc=desc, total=total, disable=disable, position=pos, 
                        unit=unit, unit_scale=unit_scale, unit_divisor=unit_divisor, leave=False)
        if new_pbar.disable: new_pbar.pos = -pos
        if not new_pbar.disable: Printer.ACTIVE_PBARS.append(new_pbar)
        return new_pbar
    
    @staticmethod
    def refresh_all_pbars(pbar_stack: list[tqdm] | None, skip_pop: bool = False) -> None:
        for pbar in pbar_stack:
            pbar.refresh()
        
        if not skip_pop and pbar_stack:
            if pbar_stack[-1].n == pbar_stack[-1].total: 
                pbar_stack.pop()
                if not pbar_stack[-1].disable: Printer.ACTIVE_PBARS.pop()
    
    @staticmethod
    def pbar_stream(stream: HTTPResponse, total: int | None = None, desc: str = ""):
        stream.read = partial(stream.read, decode_content=True)  # Decompress if needed
        return tqdmauto.wrapattr(stream, "read", total=total, desc=desc)


class Loader:
    """Busy symbol.
    
    Can be called inside a context:
    
    with Loader("This may take some Time..."):
        # do something
        pass
    """
    
    # load symbol from:
    # https://stackoverflow.com/questions/22029562/python-how-to-make-simple-animated-loading-while-process-is-running
    
    def __enter__(self):
        self.start()
    
    def __exit__(self, exc_type, exc_value, tb):
        # handle exceptions with those variables ^
        self.stop()
    
    def __init__(self, desc="Loading...", channel: PrintChannel = PrintChannel.LOADER, end='', timeout=0.1, mode='prog', disabled: bool = False):
        """
        A loader-like context manager
        
        Args:
            desc (str, optional): The loader's description. Defaults to "Loading...".
            end (str, optional): Final print. Defaults to "".
            timeout (float, optional): Sleep time between prints. Defaults to 0.1.
        """
        self.desc = desc
        self.end = end
        self.timeout = timeout
        self.channel = channel
        self.category = PrintCategory.LOADER
        
        if mode == 'std1':
            self.steps = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]
        elif mode == 'std2':
            self.steps = ["◜","◝","◞","◟"]
        elif mode == 'std3':
            self.steps = ["😐 ","😐 ","😮 ","😮 ","😦 ","😦 ","😧 ","😧 ","🤯 ","💥 ","✨ ","\u3000 ","\u3000 ","\u3000 "]
        elif mode == 'prog':
            self.steps = ["[∙∙∙]","[●∙∙]","[∙●∙]","[∙∙●]","[∙∙∙]"]
        
        self.disabled = disabled
        if self.channel is not PrintChannel.MANDATORY:
            from zotify.config import Zotify
            self.disabled = disabled or Zotify.CONFIG.get_standard_interface()
        self.done = False
        self.paused = False
        self.dead = False
    
    def _store_active_loader(self):
        self._inherited_active_loader = Printer.ACTIVE_LOADER
        Printer.ACTIVE_LOADER = self
    
    def _release_active_loader(self):
        Printer.ACTIVE_LOADER = self._inherited_active_loader
    
    def loader_print(self, msg: str):
        Printer.new_print(self.channel, msg, self.category)
        if self.category is PrintCategory.LOADER:
            self.category = PrintCategory.LOADER_CYCLE
    
    def animate(self):
        for c in cycle(self.steps):
            if self.done:
                break
            elif not self.paused:
                self.loader_print(f"{c} {self.desc}")
            sleep(self.timeout)
        self.dead = True
    
    def start(self):
        if not self.disabled:
            self._thread = Thread(target=self.animate, daemon=True)
            self._store_active_loader()
            self._thread.start()
            sleep(self.timeout) #guarantee _animate can print at least once
        return self
    
    def stop(self):
        if not self.disabled:
            self.done = True
            while not self.dead: #guarantee _animate has finished
                try:
                    sleep(self.timeout)
                except KeyboardInterrupt:
                    self.stop() # guarantee stop is called so outer funcs can clean up all loaders
                    raise
            self.category = PrintCategory.LOADER
            if self.end != "":
                self.loader_print(self.end)
            self._release_active_loader()
    
    def pause(self):
        self.paused = True
    
    def resume(self):
        self.category = PrintCategory.LOADER
        self.paused = False
        sleep(self.timeout*2) #guarantee _animate can print at least once


class Interface:
    ALL_DLCONTENT       : set  | None   = None
    CURRENT_BRANCH      : list | None   = None
    LAST_DL_TIME        : int  | None   = None
    LAST_CONV_TIME      : int  | None   = None
    LAST_DL_ITEM_NAME   : str  | None   = None
    LAST_ERROR          : str  | None   = None
    
    @staticmethod
    def _term_lines() -> int:
        try:
            _, lines = get_terminal_size()
        except OSError:
            lines = 20
        return lines
    
    @staticmethod
    def print_interface(msg) -> None:
        for line in str(msg).splitlines():
            tqdm.write(line.ljust(Printer._term_cols()))
    
    @staticmethod
    def bind(parent_stack) -> None:
        Interface.CURRENT_BRANCH = parent_stack
    
    @staticmethod
    def parse_obj_db(obj, suppress_id: bool = False) -> str:
        from zotify.api import Content, Album
        obj: Content = obj
        
        db = ""
        attrs = ([NAME,] if suppress_id else [ID, NAME]) + obj._to_db_attrs
        for attr in attrs:
            adds = Interface.parse_attr_db(obj, attr)
            if isinstance(obj, Album):
               adds = "\n".join([f"{obj.clsn} {line}" if obj.clsn not in line else line for line in adds.split("\n")])
            db += adds
            if attr != attrs[-1]: db += "\n"
        return db
    
    @staticmethod
    def parse_attr_db(obj, attr: str) -> str:
        from zotify.api import Content
        obj: Content = obj
        prefix = f"{obj.clsn} " if obj.type_attr not in attr else ""
        val: str | Content | list[str | Content] = getattr(obj, attr)
        
        if isinstance(val, Content):
            return Interface.parse_obj_db(val, suppress_id=True)
        
        base = f"{prefix}{attr.replace('_', ' ').title()} == "
        if isinstance(val, list):
            if not val:
                return base + "None"
            elif isinstance(val[0], str):
                return base + ", ".join(val)
            elif isinstance(val[0], Content):
                dbs = [Interface.parse_obj_db(c, suppress_id=True).split("\n") for c in val]
                headers = [attr.split(" == ")[0] for attr in dbs[0]]
                vals = [ [db[i].split(" == ")[-1] for db in dbs if db[i].split(" == ")[-1] != "None"]  for i in range(len(headers))]
                vals = [v if v else ["None"] for v in vals]
                combs = [h + " == " + ", ".join(v) for h, v in zip(headers, vals)]
                return "\n".join(combs)
            else:
                return base + "!UNEXPECTED ATTR!"
        else:
            return base + str(val)
    
    @staticmethod
    def refresh() -> None:
        if Interface.CURRENT_BRANCH is None:
            # attempt to preserve terminal history 
            Printer.new_print(PrintChannel.MANDATORY, "\n"*(Interface._term_lines()))
            Printer.clear()
            return
        elif Interface.ALL_DLCONTENT is None:
            Printer.debug("Interface called before Query ALL_DLCONTENT initialization")
            return
        
        from zotify.api import DLContent
        obj: DLContent = Interface.CURRENT_BRANCH[-1]
        dl_prog = len({c for c in Interface.ALL_DLCONTENT if c.downloaded})
        dashboard = f"Query Tree: {Interface.CURRENT_BRANCH}\n" +\
                    f"\n" +\
                    f"Current DLContent: {obj.clsn}\n" +\
                    f"{Interface.parse_obj_db(obj)}\n" +\
                    f"\n" +\
                    f"Status: {obj.dl_status}\n" +\
                    f"Total Query Progress: {dl_prog}/{len(Interface.ALL_DLCONTENT)}\n" +\
                    f"\n" +\
                    f"Last Download Time: {Interface.LAST_DL_TIME}\n" +\
                    f"Last Conversion Time: {Interface.LAST_CONV_TIME}\n" +\
                    f"Last Downloaded Item: {Interface.LAST_DL_ITEM_NAME}\n" +\
                    f"Last Encountered Error: {Interface.LAST_ERROR}\n"
        Printer.clear()
        Interface.print_interface(dashboard)
    
    @staticmethod
    def dl_complete(dlcontent, path, time_elapsed_dl: str, time_elapsed_ffmpeg: str | None) -> None:
        from zotify.api import DLContent
        dlcontent: DLContent = dlcontent
        
        Interface.LAST_DL_TIME = time_elapsed_dl
        Interface.LAST_CONV_TIME = time_elapsed_ffmpeg
        Interface.LAST_DL_ITEM_NAME = dlcontent.name
        
        dlcontent.set_dl_status("Waiting Between Downloads")
        Printer.hashtaged(PrintChannel.DOWNLOADS, f'DOWNLOADED: "{dlcontent.rel_path(path)}"\n' +
                                                  f'DOWNLOAD TOOK {time_elapsed_dl}' +
                                                  f' (PLUS {time_elapsed_ffmpeg} CONVERTING)' if time_elapsed_ffmpeg else '')
