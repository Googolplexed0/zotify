import os
import re
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path, PurePath
from shutil import move, copyfile, copyfileobj
from typing import Literal

from zotify.termoutput import *


# Path Utils
def ensure_is_file(path: Path, def_filename: str, touch: bool = True) -> Path:
    if path.is_file():  pass
    elif path.is_dir(): path = path / def_filename
    elif path.suffix:   pass
    else:               path = path / def_filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if touch: path.touch()
    return path


def file_has_content(path: str | PurePath) -> None | bool:
    "returns None if file does not exist, False if file is empty, and True if file has content"
    path = Path(path)
    if not path.is_file():          return None
    elif not path.stat().st_size:   return False
    else:                           return True


def pathlike_move_safe(src: PurePath | bytes, dst: PurePath, copy: bool = False) -> PurePath:
    Path(dst.parent).mkdir(parents=True, exist_ok=True)
    if isinstance(src, bytes):
        with Path(dst).open("wb") as file:
                copyfileobj(src, file)
    elif copy:  copyfile(src, dst)
    else:       move(src, dst)
    return dst


def check_path_dupes(path: PurePath) -> PurePath:
    if not (Path(path).is_file() and Path(path).stat().st_size):
        return path
    c = len([file for file in Path(path.parent).iterdir() if file.match(path.stem + "*")])
    new_path = path.with_stem(f"{path.stem}_{c}") # guaranteed to be unique
    return new_path


def get_common_dir(allpaths: set[PurePath]) -> PurePath:
    if len({p.name for p in allpaths}) == 1:
        # only one path or only multiples of one path
        return allpaths.pop().parent
    return PurePath(os.path.commonpath(allpaths))


def walk_directory_for_tracks(root_path: PurePath):
    Path(root_path).mkdir(parents=True, exist_ok=True)
    for dirpath, dirnames, filenames in os.walk(Path(root_path)):
        for filename in filenames:
            if filename.endswith(tuple(EXT_MAP.values())):
                yield PurePath(dirpath) / filename


# Input Processing Utils
def safe_typecast(d: dict, k: str, to_cast: type, except_channel: PrintChannel = PrintChannel.WARNING):
    raw_val = d.get(k)
    if raw_val is None:
        return None
    elif isinstance(raw_val, to_cast):
        return raw_val
    elif to_cast is bool:
        if str(raw_val).lower() in {"0", "no", "false"}:
            return False
        return True
    elif to_cast is float and isinstance(raw_val, str) and "/" in raw_val:
        return Fraction(''.join(raw_val.split()))
    try:
        return to_cast(raw_val)
    except Exception:
        Printer.hashtaged(except_channel, f'COULD NOT CAST VALUE OF KEY "{k}" TO TYPE {str(to_cast).upper()}')
        raise


def strlist_compressor(strs: list[str]) -> str:
    res = []
    for s in strs:
        res.extend(s.split())
    return " ".join(res)


def edge_zip(sorted_list: list) -> list:
    """ Performs sort in place: [1,2,3,4,5] -> [1,5,2,4,3] (Assumes list is ascending) """
    n = len(sorted_list)
    sorted_list[::2], sorted_list[1::2] = sorted_list[:(n+1)//2], sorted_list[:(n+1)//2-1:-1]
    return sorted_list


def arg_comb(*args: str):
    return "&" + "&".join(args) if args else ""


def clamp(low: int, i: int, high: int) -> int:
    return max(low, min(i, high))


def pct_error(act: float | int, expct: float | int) -> float:
    act = float(act); expct = float(expct)
    return abs(act - expct) / expct


def select(items: list, inline_prompt: str = 'ID(s): ', first_ID: int = 1, only_one: bool = False) -> list:
    Printer.user_make_select_prompt(only_one)
    while True:
        selection = ""
        while not selection or selection == " ":
            selection = Printer.get_input(inline_prompt)
        
        # only allow digits and commas and hyphens
        sanitized = re.sub(r"[^\d\-,]*", "", selection.strip())
        if [s for s in sanitized if s.isdigit()]:
            break # at least one digit
        Printer.hashtaged(PrintChannel.MANDATORY, 'INVALID SELECTION')
    
    if "," in sanitized:
        IDranges = sanitized.split(',')
    else:
        IDranges = [sanitized,]
    
    indices = []
    for ids in IDranges:
        if "-" in ids:
            start, end = ids.split('-') # will probably error if this is a negative number or malformed range
            indices.extend(list(range(int(start), int(end) + 1)))
        else:
            indices.append(int(ids))
    indices.sort()
    return [items[i-first_ID] for i in (indices[:1] if only_one else indices) if i-first_ID >= 0]


# Time Utils
def fmt_duration(duration: float | int, unit_conv: tuple[int, int] = (60, 60), connectors: tuple[str, str] = (":", ":"), smallest_unit: str = "s", ALWAYS_ALL_UNITS: bool = False) -> str:
    """ Formats a duration to a time string, defaulting to seconds -> hh:mm:ss format """
    duration_secs = int(duration // 1)
    duration_mins = duration_secs // unit_conv[1]
    s = duration_secs % unit_conv[1]
    m = duration_mins % unit_conv[0]
    h = duration_mins // unit_conv[0]
    
    if ALWAYS_ALL_UNITS:
        return f'{h}'.zfill(2) + connectors[0] + f'{m}'.zfill(2) + connectors[1] + f'{s}'.zfill(2)
    
    if not any((h, m, s)):
        return "0" + smallest_unit
    
    if h == 0 and m == 0:
        return f'{s}' + smallest_unit
    elif h == 0:
        return f'{m}'.zfill(2) + connectors[1] + f'{s}'.zfill(2)
    else:
        return f'{h}'.zfill(2) + connectors[0] + f'{m}'.zfill(2) + connectors[1] + f'{s}'.zfill(2)


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(r'%Y-%m-%d_%H:%M:%S')


def timestamp_utc(timestamp_ms: str | None) -> str | None:
    if not timestamp_ms: return None
    dt = datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
    return dt_to_str(dt)


def strptime_utc(dtstr: str) -> datetime:
    return datetime.strptime(dtstr[:-1], r'%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
