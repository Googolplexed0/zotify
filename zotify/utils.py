import ffmpy
import os
import subprocess
import re
import time
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path, PurePath
from shutil import move, copyfile, copyfileobj

from zotify.config import Zotify
from zotify.const import EXT_MAP
from zotify.termoutput import PrintChannel, Printer


# Path Utils
def create_download_directory(dir_path: str | PurePath) -> None:
    """ Create directory and add a hidden file with song ids """
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    # add hidden file with song ids
    hidden_file_path = PurePath(dir_path).joinpath('.song_ids')
    if Zotify.CONFIG.get_no_dir_archives():
        return
    if not Path(hidden_file_path).is_file():
        with open(hidden_file_path, 'w', encoding='utf-8') as f:
            pass


def fix_filename(name: str | PurePath | Path ) -> str:
    """
    Replace invalid characters on Linux/Windows/MacOS with underscores.
    list from https://stackoverflow.com/a/31976060/819417
    Trailing spaces & periods are ignored on Windows.
    >>> fix_filename("  COM1  ")
    '_ COM1 _'
    >>> fix_filename("COM10")
    'COM10'
    >>> fix_filename("COM1,")
    'COM1,'
    >>> fix_filename("COM1.txt")
    '_.txt'
    >>> all('_' == fix_filename(chr(i)) for i in list(range(32)))
    True
    """
    name = re.sub(r'[/\\:|<>"?*\0-\x1f]|^(AUX|COM[1-9]|CON|LPT[1-9]|NUL|PRN)(?![^.])|^\s|[\s.]$', "_", str(name), flags=re.IGNORECASE)
    
    maxlen = Zotify.CONFIG.get_max_filename_length()
    if maxlen and len(name) > maxlen:
        name = name[:maxlen]
    
    return name


def fix_filepath(path: PurePath, rel_to: PurePath) -> PurePath:
    """ Fix all parts of a filepath """
    fixed_parts = [fix_filename(part) for part in path.relative_to(rel_to).parts]
    
    # maxlen = Zotify.CONFIG.get_max_filepath_length()
    # fixed_parts.reverse()
    # while len("/".join(fixed_parts)) > maxlen:
    #     diff = len("/".join(fixed_parts)) - maxlen
    #     trimmable = [p for p in fixed_parts if len(p) > 5]
    #     name = trimmable[0][:max(5, len(trimmable[0]) - diff)]
    #     fixed_parts[fixed_parts.index(trimmable[0])] = name
    # fixed_parts.reverse()
    
    return rel_to.joinpath(*fixed_parts)


def walk_directory_for_tracks(root_path: PurePath):
    Path(root_path).mkdir(parents=True, exist_ok=True)
    for dirpath, dirnames, filenames in os.walk(Path(root_path)):
        for filename in filenames:
            if filename.endswith(tuple(EXT_MAP.values())):
                yield PurePath(dirpath) / filename


def pathlike_move_safe(src: PurePath | bytes, dst: PurePath, copy: bool = False) -> PurePath:
    Path(dst.parent).mkdir(parents=True, exist_ok=True)
    
    if not isinstance(src, PurePath):
        with Path(dst).open("wb") as file:
            copyfileobj(src, file)
        return dst
    
    if not copy:
        # Path(oldpath).rename(newpath)
        move(src, dst)
    else:
        copyfile(src, dst)
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


def bulk_regex_urls(urls: str | list[str]) -> list[list[str]]:
    if isinstance(urls, list):
        urls = strlist_compressor(urls)
    
    base_uri = r'%s[:/]([0-9a-zA-Z]{22})'
    
    matched_uris = []
    from zotify.api import ITEM_BULK_FETCH
    for req_type in ITEM_BULK_FETCH:
        ids_by_type = re.findall(base_uri % req_type.type_attr, urls)
        matched_uris.append([f"{req_type.type_attr}:{s}" for s in ids_by_type])
    return matched_uris


def edge_zip(sorted_list: list) -> list:
    """ Performs sort in place: [1,2,3,4,5] -> [1,5,2,4,3] (Assumes list is ascending) """
    n = len(sorted_list)
    sorted_list[::2], sorted_list[1::2] = sorted_list[:(n+1)//2], sorted_list[:(n+1)//2-1:-1]
    return sorted_list


def arg_comb(*args: str):
    return "&" + "&".join(args) if args else ""


def clamp(low: int, i: int, high: int) -> int:
    return max(low, min(i, high))


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


# Metadata & Codec Utils
def unconv_artist_format(artists: list[str] | str) -> list[str]:
    if Zotify.CONFIG.get_artist_delimiter() == "":
        return artists
    return artists.split(Zotify.CONFIG.get_artist_delimiter())


def conv_artist_format(artists: list, FORCE_NO_LIST: bool = False) -> list[str] | str:
    """ Returns converted artist format """
    
    from zotify.api import Artist
    artists: list[Artist] | list[str] = artists
    if not artists:
        return ""
    
    artist_names = [a.name for a in artists] if isinstance(artists[0], Artist) else artists
    if Zotify.CONFIG.get_artist_delimiter() == "":
        # if len(artist_names) == 1:
        #     return artist_names[0]
        return ", ".join(artist_names) if FORCE_NO_LIST else artist_names
    else:
        return Zotify.CONFIG.get_artist_delimiter().join(artist_names)


def conv_genre_format(genres: list[str]) -> list[str] | str:
    """ Returns converted genre format """
    
    if not genres:
        return ""
    
    if not Zotify.CONFIG.get_all_genres():
        return genres[0]
    
    if Zotify.CONFIG.get_genre_delimiter() == "":
        # if len(genres) == 1:
        #     return genres[0]
        return genres
    else:
        return Zotify.CONFIG.get_genre_delimiter().join(genres)


def pct_error(act: float | int, expct: float | int) -> float:
    act = float(act); expct = float(expct)
    return abs(act - expct) / expct


def run_ffm(in_path: PurePath, in_cmd: list[str] | None, out_path: PurePath | None = None, out_cmd: list[str] | None = None) -> str:
    FFclass = ffmpy.FFprobe
    ff_config = {
        "global_options": ['-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
        "inputs": {in_path: in_cmd}
    }
    if out_path: 
        FFclass = ffmpy.FFmpeg
        ff_config["global_options"].append('-y')
        ff_config["outputs"] = {out_path: out_cmd}
    
    stdout, stderr = FFclass(**ff_config).run(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    loggable_output = ("STDOUT:\n" + (stdout.decode().replace('\r\n', '\n') if stdout else ""),
                        "STDERR:\n" + (stderr.decode().replace('\r\n', '\n') if stderr else ""))
    Printer.logger("\n\n".join(loggable_output), PrintChannel.DEBUG)
    if out_path and Path(in_path).exists(): Path(in_path).unlink()
    return stdout.decode().strip()


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


def wait_between_downloads(skip_wait: bool = False) -> None:
    waittime = Zotify.CONFIG.get_bulk_wait_time()
    if not waittime or waittime <= 0:
        return
    
    if skip_wait:
        time.sleep(min(0.5, waittime))
        return
    
    if waittime > 5:
        Printer.hashtaged(PrintChannel.DOWNLOADS, f'PAUSED: WAITING FOR {waittime} SECONDS BETWEEN DOWNLOADS')
    time.sleep(waittime)
