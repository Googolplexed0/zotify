import datetime
import os
import re
import time
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
    if Zotify.CONFIG.get_disable_directory_archives():
        return
    if not Path(hidden_file_path).is_file():
        with open(hidden_file_path, 'w', encoding='utf-8') as f:
            pass


def fix_filename(name: str | PurePath | Path ):
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


def walk_directory_for_tracks(path: str | PurePath) -> set[Path]:
    # path must already exist
    track_paths = set()
    
    for dirpath, dirnames, filenames in os.walk(Path(path)):
        for filename in filenames:
            if filename.endswith(tuple(set(EXT_MAP.values()))):
                track_paths.update({Path(dirpath) / filename,})
    
    return track_paths


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
def strlist_compressor(strs: list[str]) -> str:
    res = []
    for s in strs:
        res.extend(s.split())
    return " ".join(res)


def bulk_regex_urls(urls: str | list[str]) -> list[list[str]]:
    """ Since many kinds of search may be passed at the command line, process them all here. """
    
    if isinstance(urls, list):
        urls = strlist_compressor(urls)
    
    base_uri = r'(?:sp'+r'otify:)?%s:([0-9a-zA-Z]{22})'
    base_url = r'(?:https?://)?open\.' + base_uri.split(':')[1] + r'\.com(?:/intl-\w+)?/%s/([0-9a-zA-Z]{22})(?:\?si=.+?)?'
    
    from zotify.api import ITEM_FETCH, ITEM_NAMES
    matched_ids = [[]]*len(ITEM_FETCH)
    for i, req_type in enumerate(ITEM_NAMES):
        matched_ids[i] = re.findall(base_uri % req_type, urls) + re.findall(base_url % req_type, urls)
    return matched_ids


def edge_zip(sorted_list: list) -> None:
    # presorted small to big, in place, [1,2,3,4,5] -> [1,5,2,4,3],
    n = len(sorted_list)
    sorted_list[::2], sorted_list[1::2] = sorted_list[:(n+1)//2], sorted_list[:(n+1)//2-1:-1]


def select(items: list, get_input_prompt: str = 'ID(s): ') -> list:
    Printer.search_select()
    selection = ""
    while not selection or selection == " ":
        selection = Printer.get_input(get_input_prompt)
    
    # removes all non-numeric characters except for commas and hyphens
    sanitized = re.sub(r"[^\d\-,]*", "", selection.strip())
    
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
    
    if not indices:
        Printer.hashtaged(PrintChannel.MANDATORY, 'INVALID SELECTION - EXITING...')
        return []
    
    return [items[i-1] for i in indices]


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


# Time Utils
def fmt_duration(duration: float | int, unit_conv: tuple[int] = (60, 60), connectors: tuple[str] = (":", ":"), smallest_unit: str = "s", ALWAYS_ALL_UNITS: bool = False) -> str:
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


def strptime_utc(dtstr) -> datetime.datetime:
    return datetime.datetime.strptime(dtstr[:-1], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=datetime.timezone.utc)


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


# Song Archive Utils
def upgrade_legacy_archive(entries: list[str], archive_path: PurePath) -> None:
    """ Attempt to match a legacy archive's filename to a full filepath """
    
    rewrite_legacy = False
    from zotify.api import Track
    for i, entry in enumerate(entries):
        entry_items = entry.strip().split('\t')
        filename_or_path = PurePath(entry_items[-1])
        if filename_or_path.is_absolute():
            entries[i] = entry_items
            continue
        
        rewrite_legacy = True
        path_entry = filename_or_path
        test_track = Track(entry_items[0])
        for glob_path in Path(Zotify.CONFIG.get_root_path()).glob('**/' + str(filename_or_path)):
            test_track.filepath = PurePath(glob_path)
            reliable_tags, unreliable_tags = test_track.get_audio_tags()
            if ("trackid" in unreliable_tags and unreliable_tags["trackid"] == entry_items[0]
            or  unconv_artist_format(reliable_tags[0])[0] == entry_items[2]
            or  reliable_tags[2] == entry_items[3]):
                path_entry = PurePath(glob_path)
                break
        
        entries[i] = entry_items[:-1] + [path_entry]
    
    if rewrite_legacy:
        Path(archive_path).unlink()
        mode = 'w'
        for entry in entries:
            add_to_archive(*entry, archive_path, mode)
            mode = 'a'


def get_archived_entries(dir_path: PurePath | None = None) -> list[str]:
    """ Returns list of downloaded song entries """
    if dir_path:
        disabled = Zotify.CONFIG.get_disable_directory_archives()
        archive_path = dir_path / '.song_ids'
    else:
        disabled = Zotify.CONFIG.get_disable_song_archive()
        archive_path = Zotify.CONFIG.get_song_archive_location()
    
    if disabled or not Path(archive_path).exists():
        return []
    
    with open(archive_path, 'r', encoding='utf-8') as f:
        # id, date, author, track, filepath (only filename if from legacy archive)
        entries = f.readlines()
    
    if dir_path or not Zotify.CONFIG.get_upgrade_legacy_archive():
        return entries
    
    upgrade_legacy_archive(entries, archive_path)
    Zotify.CONFIG.set_stop_upgrade_legacy_archive()
    
    return get_archived_entries(dir_path)


def get_archived_item_ids(dir_path: PurePath | None = None) -> list[str]:
    """ Returns list of downloaded item_ids """
    entries = get_archived_entries(dir_path)
    item_ids = [entry.strip().split('\t')[0] for entry in entries]
    return item_ids


def get_archived_item_paths(dir_path: PurePath | None = None) -> list[PurePath]:
    """ Returns list of downloaded item_paths """
    entries = get_archived_entries(dir_path)
    item_paths = [PurePath(entry.strip().split('\t')[-1]) for entry in entries]
    return item_paths


def add_to_archive(item_id: str, timestamp: str, author_name: str, item_name: str, item_path: PurePath, 
                   archive_path: PurePath, mode: str) -> None:
    """ Adds item record to the song archive at archive_path """
    
    if not timestamp:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(archive_path, mode, encoding='utf-8') as file:
        file.write(f'{item_id}\t{timestamp}\t{author_name}\t{item_name}\t{item_path}\n')


def add_obj_to_song_archive(obj, dir_path: PurePath | None = None) -> None:
    if dir_path:
        disabled = Zotify.CONFIG.get_disable_directory_archives()
        archive_path = dir_path / '.song_ids'
        mode = 'a' # should already exist from create_download_directory(), so only append mode
    else:
        disabled = Zotify.CONFIG.get_disable_song_archive()
        archive_path = Zotify.CONFIG.get_song_archive_location()
        mode = 'a' if Path(archive_path).exists() else 'w'
    
    if disabled:
        return
    
    from zotify.api import Track, Episode
    obj: Track | Episode = obj
    author_name = obj.artists[0].name if isinstance(obj, Track) else obj.show.publisher
    item_name = obj.name if isinstance(obj, Track) else obj.printing_label
    add_to_archive(obj.id, "", author_name, item_name, obj.filepath,
                   archive_path, mode)


# Playlist File Utils
def add_to_m3u8(m3u8_path: PurePath, contents: list, append_strs: list[str] | None = None):
    from zotify.api import DLContent
    contents: list[DLContent] = contents
    
    if not Path(m3u8_path).exists():
        Path(m3u8_path.parent).mkdir(parents=True, exist_ok=True)
        with open(m3u8_path, 'x', encoding='utf-8') as file:
            file.write("#EXTM3U\n\n")
    
    with open(m3u8_path, 'a', encoding='utf-8') as file:
        for content in contents:
            track_path_m3u = content.filepath
            if track_path_m3u is None:
                continue
            
            track_label_m3u = f"#EXTINF:{content.duration_ms // 1000}, {content.printing_label}\n"
            if Zotify.CONFIG.get_m3u8_relative_paths():
                track_path_m3u = os.path.relpath(track_path_m3u, m3u8_path.parent)
            
            file.write(track_label_m3u)
            file.write(f"{track_path_m3u}\n\n")
        
        if append_strs:
            file.writelines(append_strs)


def fetch_m3u8_songs(m3u8_path: PurePath) -> list[str]:
    """ Fetches the songs and associated file paths in an .m3u8 playlist"""
    
    if not Path(m3u8_path).exists():
        return []
    
    with open(m3u8_path, 'r', encoding='utf-8') as file:
        linesraw = file.readlines()[2:-1]
        # group by song and filepath
        # songsgrouped = []
        # for i in range(len(linesraw)//3):
        #     songsgrouped.append(linesraw[3*i:3*i+3])
    return linesraw
