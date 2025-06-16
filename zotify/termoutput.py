from __future__ import annotations
import json
import platform
from os import get_terminal_size, system
from itertools import cycle
from time import sleep
from threading import Thread
from traceback import TracebackException
from enum import Enum
from tqdm import tqdm

from zotify.const import *


UP_ONE_LINE = "\033[A"
DOWN_ONE_LINE = "\033[B"
START_OF_PREV_LINE = "\033[F"
CLEAR_LINE = "\033[K"

ACTIVE_LOADER: list[Loader] = []
ACTIVE_PBARS: list[tqdm] = []


class PrintChannel(Enum):
    MANDATORY = "MANDATORY"
    DEBUG = DEBUG
    
    SPLASH = PRINT_SPLASH
    
    WARNINGS = PRINT_WARNINGS
    ERRORS = PRINT_ERRORS
    API_ERRORS = PRINT_API_ERRORS
    
    PROGRESS_INFO = PRINT_PROGRESS_INFO
    SKIPS = PRINT_SKIPS
    DOWNLOADS = PRINT_DOWNLOADS


class Printer:
    @staticmethod
    def print(channel: PrintChannel, msg: str, loader: bool = False) -> None:
        if channel != PrintChannel.MANDATORY:
            from zotify.zotify import Zotify
        if channel == PrintChannel.MANDATORY or Zotify.CONFIG.get(channel.value):
            try:
                columns, _ = get_terminal_size()
            except OSError:
                columns = 80
            if not loader and ACTIVE_LOADER:
                ACTIVE_LOADER[0].pause()
                msg += "\n"*4
            for line in str(msg).splitlines():
                tqdm.write(line.ljust(columns))
            if not loader and ACTIVE_LOADER:
                ACTIVE_LOADER[0].resume()
                sleep(ACTIVE_LOADER[0].timeout*2) #guarantee it appears
    
    @staticmethod
    def loader(channel: PrintChannel, msg: str) -> None:
        Printer.print(channel, START_OF_PREV_LINE*2 + msg, loader=True)
    
    @staticmethod
    def debug(msg: str) -> None:
        Printer.print(PrintChannel.DEBUG, msg)
    
    @staticmethod
    def json_dump(obj: dict, channel: PrintChannel = PrintChannel.ERRORS) -> None:
        try:
            columns, _ = get_terminal_size()
        except OSError:
            columns = 80
        if AVAIL_MARKETS in obj:
            obj[AVAIL_MARKETS] = "REMOVED FOR BREVITY"
        if ITEMS in obj and AVAIL_MARKETS in obj[ITEMS][0]:
            for item in obj[ITEMS]:
                item[AVAIL_MARKETS] = "REMOVED FOR BREVITY"
        Printer.print(channel, "#" * columns)
        Printer.print(channel, json.dumps(obj, indent=2))
        Printer.print(channel, "#" * columns + "\n\n")
    
    @staticmethod
    def traceback(e: Exception) -> None:
        Printer.print(PrintChannel.ERRORS, "\n")
        Printer.print(PrintChannel.ERRORS, "".join(TracebackException.from_exception(e).format()))
        Printer.print(PrintChannel.ERRORS, "\n")
    
    @staticmethod
    def depreciated_warning(option_string: str, help_msg: str = None, CONFIG = True) -> None:
        Printer.print(PrintChannel.MANDATORY, "\n" +\
        "###   WARNING: " + ("CONFIG" if CONFIG else "ARGUMENT") + f" `{option_string}` IS DEPRECIATED, IGNORING   ###\n" +\
        "###   THIS WILL BE REMOVED IN FUTURE VERSIONS   ###\n" +\
        f"###   {help_msg}   ###\n" if  help_msg else "\n")
    
    
    @staticmethod
    def splash() -> None:
        """ Displays splash screen """
        Printer.print(PrintChannel.SPLASH,
        "    ███████╗ ██████╗ ████████╗██╗███████╗██╗   ██╗"+"\n"+\
        "    ╚══███╔╝██╔═══██╗╚══██╔══╝██║██╔════╝╚██╗ ██╔╝"+"\n"+\
        "      ███╔╝ ██║   ██║   ██║   ██║█████╗   ╚████╔╝ "+"\n"+\
        "     ███╔╝  ██║   ██║   ██║   ██║██╔══╝    ╚██╔╝  "+"\n"+\
        "    ███████╗╚██████╔╝   ██║   ██║██║        ██║   "+"\n"+\
        "    ╚══════╝ ╚═════╝    ╚═╝   ╚═╝╚═╝        ╚═╝   "+"\n\n"
        )
    
    @staticmethod
    def search_select() -> None:
        """ Displays splash screen """
        Printer.print(PrintChannel.MANDATORY,
        "> SELECT A DOWNLOAD OPTION BY ID\n" +
        "> SELECT A RANGE BY ADDING A DASH BETWEEN BOTH ID's\n" +
        "> OR PARTICULAR OPTIONS BY ADDING A COMMA BETWEEN ID's\n"
        )
    
    @staticmethod
    def clear() -> None:
        """ Clear the console window """
        if platform.system() == WINDOWS_SYSTEM:
            system('cls')
        else:
            system('clear')
    
    
    @staticmethod
    def pbar(iterable=None, desc=None, total=None, unit='it', 
            disable=False, unit_scale=False, unit_divisor=1000, pos=1) -> tqdm:
        if iterable and len(iterable) == 1 and len(ACTIVE_PBARS) > 0:
            disable = True # minimize clutter
        new_pbar = tqdm(iterable=iterable, desc=desc, total=total, disable=disable, position=pos, 
                        unit=unit, unit_scale=unit_scale, unit_divisor=unit_divisor, leave=False)
        if new_pbar.disable: new_pbar.pos = -pos
        if not new_pbar.disable: ACTIVE_PBARS.append(new_pbar)
        return new_pbar
    
    @staticmethod
    def refresh_all_pbars(pbar_stack: list[tqdm] | None, skip_pop: bool = False) -> None:
        for pbar in pbar_stack:
            pbar.refresh()
        
        if not skip_pop and pbar_stack:
            if pbar_stack[-1].n == pbar_stack[-1].total: 
                pbar_stack.pop()
                if not pbar_stack[-1].disable: ACTIVE_PBARS.pop()
    
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
    
    def __init__(self, chan, desc="Loading...", end='', timeout=0.1, mode='prog'):
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
        
        self._thread = Thread(target=self._animate, daemon=True)
        if mode == 'std1':
            self.steps = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]
        elif mode == 'std2':
            self.steps = ["◜","◝","◞","◟"]
        elif mode == 'std3':
            self.steps = ["😐 ","😐 ","😮 ","😮 ","😦 ","😦 ","😧 ","😧 ","🤯 ","💥 ","✨ ","\u3000 ","\u3000 ","\u3000 "]
        elif mode == 'prog':
            self.steps = ["[∙∙∙]","[●∙∙]","[∙●∙]","[∙∙●]","[∙∙∙]"]
        
        self.done = False
        self.paused = False
    
    def start(self):
        ACTIVE_LOADER.append(self)
        Printer.print(self.channel, "\n", loader=True)
        self._thread.start()
        return self
    
    def _animate(self):
        for c in cycle(self.steps):
            if self.done:
                break
            elif not self.paused:
                Printer.loader(self.channel, f"\t{c} {self.desc}")
            sleep(self.timeout)
    
    def __enter__(self):
        self.start()
    
    def stop(self):
        self.done = True
        if self.end != "":
            Printer.print(self.channel, self.end, loader=True)
        if self in ACTIVE_LOADER: ACTIVE_LOADER.pop()
    
    def pause(self):
        self.paused = True
    
    def resume(self):
        self.paused = False
    
    def __exit__(self, exc_type, exc_value, tb):
        # handle exceptions with those variables ^
        self.stop()
