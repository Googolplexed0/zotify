import os
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
    from zotify.api import ITEM_FETCH
    for req_type in ITEM_FETCH:
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


# Song Archive Utils
class SongArchive:
    """ Entry: id, date, author, name, filepath (only filename if from legacy archive) """
    UPDATE_ARCHIVE: bool = False
    
    def __init__(self, dir_path: PurePath | None = None):
        self._global = dir_path is None
        self.filepath = Zotify.CONFIG.get_song_archive_location() if dir_path is None else dir_path / '.song_ids'
        self.mode = 'a' if Path(self.filepath).exists() else 'w'
        self.disabled = not Path(self.filepath).exists() or \
                        (Zotify.CONFIG.get_no_song_archive() if self._global else Zotify.CONFIG.get_no_dir_archives())
    
    def upgrade_legacy_archive(self, entries: list[str]) -> None:
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
            for glob_path in Path(Zotify.CONFIG.get_root_path()).glob('**/' + str(filename_or_path)):
                reliable_tags, unreliable_tags = Track.read_audio_tags(PurePath(glob_path))
                if ("trackid" in unreliable_tags and unreliable_tags["trackid"] == entry_items[0]
                or  unconv_artist_format(reliable_tags[0])[0] == entry_items[2]
                or  reliable_tags[2] == entry_items[3]):
                    path_entry = PurePath(glob_path)
                    break
            
            entries[i] = entry_items[:-1] + [path_entry]
        
        if rewrite_legacy:
            Path(self.filepath).unlink()
            mode = 'w'
            for entry in entries:
                self.add_entry(*entry, mode)
                mode = 'a'
    
    def read_entries(self) -> list[str]:
        if self.disabled:   return []
        
        with open(self.filepath, 'r', encoding='utf-8') as f:
            entries = f.readlines()
        if self._global and SongArchive.UPDATE_ARCHIVE:
            SongArchive.UPDATE_ARCHIVE = False
            self.upgrade_legacy_archive(entries)
            return self.read_entries()
        return entries
    
    def ids(self) -> list[str]:
        return [e.strip().split('\t')[0] for e in self.read_entries()]
    
    def paths(self) -> list[PurePath]:
        return [PurePath(e.strip().split('\t')[-1]) for e in self.read_entries()]
    
    def id_path(self, item_id: str) -> PurePath:
        return self.paths()[self.ids().index(item_id)]
    
    def add_entry(self, item_id: str, timestamp: str, author_name: str, item_name: str, item_path: PurePath, mode: str) -> None:
        if not timestamp:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f'{item_id}\t{timestamp}\t{author_name}\t{item_name}\t{item_path}\n'
        with open(self.filepath, mode, encoding='utf-8') as file:
            file.write(entry)
    
    def add_obj(self, obj, item_path: PurePath) -> None:
        if self.disabled: return
        from zotify.api import Track, Episode
        obj: Track | Episode = obj
        author_name = obj.artists[0].name if isinstance(obj, Track) else obj.show.publisher
        item_name = obj.name if isinstance(obj, Track) else str(obj)
        self.add_entry(obj.id, "", author_name, item_name, item_path, self.mode)


# M3U8 Playlist File Utils
class M3U8():
    def __init__(self, cont_paths: list[PurePath | None], cont_type: type, creator):
        from zotify.api import Content, Container, Query
        self.cont_type: type[Content] = cont_type
        self.creator: Query | Container = creator
        self.name = self.cont_type.uppers if isinstance(self.creator, Query) else f'"{self.creator.name}"'
        
        dir = Zotify.CONFIG.get_m3u8_location()
        if not dir: dir = self.dynamic_dir(cont_paths)
        filename = fix_filename(f"{self.creator.id}_{self.cont_type.lowers}" if isinstance(self.creator, Query) else self.creator.name) + ".m3u8"
        self.path = dir / filename if dir else None
    
    def dynamic_dir(self, cont_paths: list[PurePath | None]) -> PurePath | None:
        paths = {path for path in cont_paths if isinstance(path, PurePath) and path.is_relative_to(self.cont_type._path_root)}
        dir = get_common_dir(paths)
        return dir
    
    @staticmethod
    def fetch_songs(m3u8_path: PurePath) -> list[str]:
        if not Path(m3u8_path).exists(): return []
        
        with open(m3u8_path, 'r', encoding='utf-8') as file:
            linesraw = file.readlines()[2:]
            # songsgrouped = [] # group by song and filepath
            # for i in range(len(linesraw)//3):
            #     songsgrouped.append(linesraw[3*i:3*i+3])
        return linesraw
    
    @staticmethod
    def find_sync_point(filepaths: list[PurePath | None], m3u8_entry_path: str) -> int | None:
        for i, filepath in enumerate(filepaths):
            Printer.logger(f"{filepath} == {m3u8_entry_path}")
            if str(filepath) == m3u8_entry_path:
                return i
            elif str(filepath) in m3u8_entry_path:
                Printer.hashtaged(PrintChannel.WARNING, 'TRACK FILEPATH WITHIN LIKED SONG M3U8 ENTRY\n' +
                                                        'M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n' +
                                                        'POSSIBLY FROM NON-UPDATED SONG ARCHIVE FILE\n' +
                                                        "(CONSIDER RUNNING --update-archive)")
                return i
            elif m3u8_entry_path in str(filepath):
                Printer.hashtaged(PrintChannel.WARNING, 'LIKED SONG M3U8 ENTRY WITHIN TRACK FILEPATH\n' +
                                                        'M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n' +
                                                        'POSSIBLY FROM M3U8 USING RELATIVE PATHS\n' +
                                                        '(CONSIDER USING FULL PATHS FOR LIKED SONGS M3U8)')
                return i
    
    def write(self, dlcs: list, cont_paths: list[PurePath | None]):
        from zotify.api import DLContent, Container
        dlcs: list[DLContent | None] = dlcs
        
        if self.path is None:
            Printer.hashtaged(PrintChannel.WARNING, f'SKIPPING M3U8 CREATION FOR {self.name}\n' +
                                                     'NO CONTENT WITH VALID FILEPATHS FOUND')
            return
        elif Zotify.CONFIG.get_m3u8_relative_paths():
            cont_paths = [os.path.relpath(p, self.path.parent) if p else None for p in cont_paths]
        
        missing_name = f"{self.cont_type.clsn}"
        if isinstance(self.cont_type, Container): missing_name += f" {self.cont_type._contains}"
        
        Path(self.path.parent).mkdir(parents=True, exist_ok=True)
        with open(self.path, 'w', encoding='utf-8') as file:
            file.write("#EXTM3U\n\n")
            for i, dlc, path in zip(range(len(dlcs)), dlcs, cont_paths):
                file.write(f"#EXTINF:{dlc.duration_ms // 1000}, {dlc}\n" if dlc else f"# Missing {missing_name} {i+1}\n")
                file.write(f"{path}\n\n" if path else "# None\n\n")
        
        Printer.hashtaged(PrintChannel.MANDATORY, f'M3U8 CREATED FOR {self.name}\n' +
                                                  f'SAVED TO: {self.cont_type.rel_path(self.path)}')
    
    def append(self, append_strs: list[str]):
        if self.path is None or not Path(self.path).exists() or not append_strs:
            return
        with open(self.path, 'a', encoding='utf-8') as file:
            file.writelines(append_strs)
