from __future__ import annotations
import platform
from os import get_terminal_size, system
from itertools import cycle
from time import sleep
from pprint import pformat
from tabulate import tabulate
from threading import Thread
from traceback import TracebackException
from enum import Enum
from tqdm import tqdm
from mutagen import FileType

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
    
    PROGRESS_INFO = PRINT_PROGRESS_INFO
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
    
    @staticmethod
    def _term_cols() -> int:
        try:
            columns, _ = get_terminal_size()
        except OSError:
            columns = 80
        return columns
    
    @staticmethod
    def _logger(msg: str, channel: PrintChannel) -> None:
        if channel not in {PrintChannel.WARNING, PrintChannel.ERROR, PrintChannel.API_ERROR, PrintChannel.DEBUG,}:
            return
        from zotify.config import Zotify
        if Zotify.CONFIG.logger:
            if isinstance(msg, BaseException):
                msg = "".join(TracebackException.from_exception(msg).format())
            msg = "\n\n" + msg.strip() + "\n"
            if channel is PrintChannel.WARNING:
                Zotify.CONFIG.logger.warning(msg)
            elif channel in {PrintChannel.ERROR, PrintChannel.API_ERROR}:
                Zotify.CONFIG.logger.error(msg)
            elif channel is PrintChannel.DEBUG:
                Zotify.CONFIG.logger.debug(msg)
    
    @staticmethod
    def _api_shrink(obj: list | tuple | dict) -> dict:
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
            obj = [Printer._api_shrink(item) for item in obj]
        
        elif isinstance(obj, tuple):
            if len(obj) == 2 and isinstance(obj[0], str):
                if shrink(obj[0]):
                    obj = (obj[0], shrink(obj[0]))
        
        elif isinstance(obj, (dict, FileType)):
            for k, v in obj.items():
                if shrink(k):
                    obj[k] = shrink(k)
                else:
                    obj[k] = Printer._api_shrink(v) 
        
        return obj
    
    @staticmethod
    def _print_prefixes(msg: str, category: PrintCategory, channel: PrintChannel) -> tuple[str, PrintCategory]:
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
    def _toggle_active_loader(skip_toggle: bool = False):
        if not skip_toggle and Printer.ACTIVE_LOADER:
            if Printer.ACTIVE_LOADER.paused:
                Printer.ACTIVE_LOADER.resume()
            else:
                Printer.ACTIVE_LOADER.pause()
    
    @staticmethod
    def new_print(channel: PrintChannel, msg: str, category: PrintCategory = PrintCategory.NONE, 
                  skip_toggle: bool = False, end: str = "\n") -> None:
        Printer._logger(msg, channel)
        if channel != PrintChannel.MANDATORY:
            from zotify.config import Zotify
        if channel == PrintChannel.MANDATORY or Zotify.CONFIG.get(channel.value):
            msg, category = Printer._print_prefixes(msg, category, channel)
            Printer._toggle_active_loader(skip_toggle)
            for line in str(msg).splitlines():
                if end == "\n":
                    tqdm.write(line.ljust(Printer._term_cols()))
                else:
                    tqdm.write(line, end=end)
                Printer.LAST_PRINT = category
            Printer._toggle_active_loader(skip_toggle)
    
    @staticmethod
    def get_input(prompt: str) -> str:
        user_input = ""
        Printer._toggle_active_loader()
        while len(user_input) == 0:
            Printer.new_print(PrintChannel.MANDATORY, prompt, end="", skip_toggle=True)
            user_input = str(input())
        Printer._toggle_active_loader()
        return user_input
    
    # Print Wrappers
    @staticmethod
    def json_dump(obj: dict, channel: PrintChannel = PrintChannel.ERROR, category: PrintCategory = PrintCategory.JSON) -> None:
        obj = Printer._api_shrink(obj)
        Printer.new_print(channel, pformat(obj, indent=2), category)
    
    @staticmethod
    def debug(*msg: tuple[str | object]) -> None:
        for m in msg:
            if isinstance(m, str):
                Printer.new_print(PrintChannel.DEBUG, m, PrintCategory.DEBUG)
            else:
                Printer.json_dump(m, PrintChannel.DEBUG, PrintCategory.DEBUG)
    
    @staticmethod
    def hashtaged(channel: PrintChannel, msg: str) -> None:
        Printer.new_print(channel, msg, PrintCategory.HASHTAG)
    
    @staticmethod
    def traceback(e: Exception) -> None:
        msg = "".join(TracebackException.from_exception(e).format())
        Printer.new_print(PrintChannel.ERROR, msg, PrintCategory.MANDATORY)
    
    @staticmethod
    def depreciated_warning(option_string: str, help_msg: str = None, CONFIG = True) -> None:
        Printer.new_print(PrintChannel.MANDATORY, "\n" +\
        "###   WARNING: " + ("CONFIG" if CONFIG else "ARGUMENT") + f" `{option_string}` IS DEPRECIATED, IGNORING   ###\n" +\
        "###   THIS WILL BE REMOVED IN FUTURE VERSIONS   ###\n" +\
        f"###   {help_msg}   ###\n" if  help_msg else "\n")
    
    @staticmethod
    def table(title: str, headers: tuple[str], tabular_data: list) -> None:
        Printer.hashtaged(PrintChannel.MANDATORY, title)
        Printer.new_print(PrintChannel.MANDATORY, tabulate(tabular_data, headers=headers, tablefmt='pretty'))
    
    # Prefabs
    @staticmethod
    def clear() -> None:
        """ Clear the console window """
        if platform.system() == WINDOWS_SYSTEM:
            system('cls')
        else:
            system('clear')
    
    @staticmethod
    def splash() -> None:
        """ Displays splash screen """
        Printer.new_print(PrintChannel.SPLASH,
        "    ███████╗ ██████╗ ████████╗██╗███████╗██╗   ██╗"+"\n"+\
        "    ╚══███╔╝██╔═══██╗╚══██╔══╝██║██╔════╝╚██╗ ██╔╝"+"\n"+\
        "      ███╔╝ ██║   ██║   ██║   ██║█████╗   ╚████╔╝ "+"\n"+\
        "     ███╔╝  ██║   ██║   ██║   ██║██╔══╝    ╚██╔╝  "+"\n"+\
        "    ███████╗╚██████╔╝   ██║   ██║██║        ██║   "+"\n"+\
        "    ╚══════╝ ╚═════╝    ╚═╝   ╚═╝╚═╝        ╚═╝   "+"\n",
        PrintCategory.MANDATORY)
    
    @staticmethod
    def search_select() -> None:
        """ Displays splash screen """
        Printer.new_print(PrintChannel.MANDATORY,
        "> SELECT A DOWNLOAD OPTION BY ID\n" +
        "> SELECT A RANGE BY ADDING A DASH BETWEEN BOTH ID's\n" +
        "> OR PARTICULAR OPTIONS BY ADDING A COMMA BETWEEN ID's\n",
        PrintCategory.MANDATORY)
    
    @staticmethod
    def newline() -> None:
        Printer.new_print(PrintChannel.MANDATORY, "\n")
    
    @staticmethod
    def back_up() -> None:
        Printer.new_print(PrintChannel.MANDATORY, UP_ONE_LINE, end="")
    
    # Progress Bars
    @staticmethod
    def pbar(iterable=None, desc=None, total=None, unit='it', 
             disable=False, unit_scale=False, unit_divisor=1000, pos=1) -> tqdm:
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
    def pbar_position_handler(default_pos: int, pbar_stack: list[tqdm] | None) -> tuple[int, list[tqdm]]:
        pos = default_pos
        if pbar_stack is not None:
            pos = -pbar_stack[-1].pos + (0 if pbar_stack[-1].disable else -2)
        else:
            # next bar must be appended to this empty list
            pbar_stack = []
        
        return pos, pbar_stack


class Loader:
    """Busy symbol.
    
    Can be called inside a context:
    
    with Loader("This may take some Time..."):
        # do something
        pass
    """
    
    # load symbol from:
    # https://stackoverflow.com/questions/22029562/python-how-to-make-simple-animated-loading-while-process-is-running
    
    def __init__(self, chan, desc="Loading...", end='', timeout=0.1, mode='prog', disabled: bool = False):
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
        self.channel = chan
        self.category = PrintCategory.LOADER
        
        self._thread = Thread(target=self._animate, daemon=True)
        if mode == 'std1':
            self.steps = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]
        elif mode == 'std2':
            self.steps = ["◜","◝","◞","◟"]
        elif mode == 'std3':
            self.steps = ["😐 ","😐 ","😮 ","😮 ","😦 ","😦 ","😧 ","😧 ","🤯 ","💥 ","✨ ","\u3000 ","\u3000 ","\u3000 "]
        elif mode == 'prog':
            self.steps = ["[∙∙∙]","[●∙∙]","[∙●∙]","[∙∙●]","[∙∙∙]"]
        
        self.disabled = disabled
        self.done = False
        self.paused = False
        self.dead = False
    
    def _loader_print(self, msg: str):
        Printer.new_print(self.channel, msg, self.category, skip_toggle=True)
        
        if self.category is PrintCategory.LOADER:
            self.category = PrintCategory.LOADER_CYCLE
    
    def store_active_loader(self):
        self._inherited_active_loader = Printer.ACTIVE_LOADER
        Printer.ACTIVE_LOADER = self
    
    def release_active_loader(self):
        Printer.ACTIVE_LOADER = self._inherited_active_loader
    
    def start(self):
        if not self.disabled:
            self.store_active_loader()
            self._thread.start()
            sleep(self.timeout) #guarantee _animate can print at least once
        return self
    
    def _animate(self):
        for c in cycle(self.steps):
            if self.done:
                break
            elif not self.paused:
                self._loader_print(f"{c} {self.desc}")
            sleep(self.timeout)
        self.dead = True
    
    def __enter__(self):
        self.start()
    
    def stop(self):
        if not self.disabled:
            self.done = True
            while not self.dead: #guarantee _animate has finished
                try:
                    sleep(self.timeout)
                except KeyboardInterrupt as e:
                    self.stop() # guarantee stop is called so outer funcs can clean up all loaders
                    raise e
            self.category = PrintCategory.LOADER
            if self.end != "":
                self._loader_print(self.end)
            self.release_active_loader()
    
    def pause(self):
        self.paused = True
    
    def resume(self):
        self.category = PrintCategory.LOADER
        self.paused = False
        sleep(self.timeout*2) #guarantee _animate can print at least once
    
    def __exit__(self, exc_type, exc_value, tb):
        # handle exceptions with those variables ^
        self.stop()
