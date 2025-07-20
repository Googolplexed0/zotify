import datetime
import os
import re
import subprocess
import requests
import music_tag
from music_tag.file import TAG_MAP_ENTRY
from music_tag.mp4 import freeform_set
from mutagen.id3 import TXXX
from time import sleep
from pathlib import Path, PurePath

from zotify.const import ALBUMARTIST, ARTIST, TRACKTITLE, ALBUM, YEAR, DISCNUMBER, \
    TRACKNUMBER, ARTWORK, TOTALTRACKS, TOTALDISCS, EXT_MAP, LYRICS, COMPILATION, GENRE, EXT_MAP
from zotify.zotify import Zotify
from zotify.termoutput import PrintChannel, Printer


# Path Utils
def create_download_directory(download_path: str | PurePath) -> None:
    """ Create directory and add a hidden file with song ids """
    Path(download_path).mkdir(parents=True, exist_ok=True)
    
    # add hidden file with song ids
    hidden_file_path = PurePath(download_path).joinpath('.song_ids')
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
                track_paths.update(Path(dirpath) / filename)
    
    return track_paths


# Input Processing Utils
def regex_input_for_urls(search_input: str, non_global: bool = False) -> tuple[
    str | None, str | None, str | None, str | None, str | None, str | None]:
    """ Since many kinds of search may be passed at the command line, process them all here. """
    
    link_types = ("track", "album", "playlist", "episode", "show", "artist")
    base_uri = r'^sp'+r'otify:%s:([0-9a-zA-Z]{22})$'
    base_url = r'^(?:https?://)?open\.sp'+r'otify\.com(?:/intl-\w+)?/%s/([0-9a-zA-Z]{22})(?:\?si=.+?)?$'
    if non_global:
        base_uri = base_uri[1:-1]
        base_url = base_url[1:-1]
    
    result = [None, None, None, None, None, None]
    for i, req_type in enumerate(link_types):
        uri_res = re.search(base_uri % req_type, search_input)
        url_res = re.search(base_url % req_type, search_input)
        
        if uri_res is not None or url_res is not None:
            result[i] = uri_res.group(1) if uri_res else url_res.group(1)
    
    return tuple(result)


def split_sanitize_input(raw_input: str) -> list[int]:
    """ Returns a list of IDs from a string input, including ranges and single IDs """
    
    # removes all non-numeric characters except for commas and hyphens
    sanitized = re.sub(r"[^\d\-,]*", "", raw_input.strip())
    
    if "," in sanitized:
        IDranges = sanitized.split(',')
    else:
        IDranges = [sanitized,]
    
    inputs = []
    for ids in IDranges:
        if "-" in ids:
            start, end = ids.split('-') # will probably error if this is a negative number or malformed range
            inputs.extend(list(range(int(start), int(end) + 1)))
        else:
            inputs.append(int(ids))
    inputs.sort()
    
    return inputs


# Metadata Utils
def conv_artist_format(artists: list[str], FORCE_NO_LIST: bool = False) -> list[str] | str:
    """ Returns converted artist format """
    if Zotify.CONFIG.get_artist_delimiter() == "":
        return ", ".join(artists) if FORCE_NO_LIST else artists
    else:
        return Zotify.CONFIG.get_artist_delimiter().join(artists)


def conv_genre_format(genres: list[str]) -> list[str] | str:
    """ Returns converted genre format """
    if not Zotify.CONFIG.get_all_genres():
        return genres[0]
    
    if Zotify.CONFIG.get_genre_delimiter() == "":
        return genres
    else:
        return Zotify.CONFIG.get_genre_delimiter().join(genres)


def set_audio_tags(filename, track_id: str, artists: list[str], genres: list[str], name, album_name, album_artist, release_year, disc_number, track_number, total_tracks, total_discs, compilation: int, lyrics: list[str] | None) -> None:
    """ sets music_tag metadata """
    tags = music_tag.load_file(filename)
    
    # Reliable Tags
    tags[ARTIST] = conv_artist_format(artists)
    tags[GENRE] = conv_genre_format(genres)
    tags[TRACKTITLE] = name
    tags[ALBUM] = album_name
    tags[ALBUMARTIST] = album_artist
    tags[YEAR] = release_year
    tags[DISCNUMBER] = disc_number
    tags[TRACKNUMBER] = track_number
    
    # Unreliable Tags
    ext = EXT_MAP[Zotify.CONFIG.get_download_format().lower()]
    
    if ext == "mp3":
        tags.mfile.tags.add(TXXX(encoding=3, desc='TRACKID', text=[track_id]))
    elif ext == "m4a":
        class Metadata:
            def __init__(cls, val): cls.values = [track_id]
        freeform_set(tags, '----:com.apple.iTunes:trackid',  Metadata())
    else:
        tags.tag_map["trackid"] = TAG_MAP_ENTRY(getter="trackid", setter="trackid", type=str)
        tags["trackid"] = track_id
    
    if Zotify.CONFIG.get_disc_track_totals():
        tags[TOTALTRACKS] = total_tracks
        if total_discs is not None:
            tags[TOTALDISCS] = total_discs
    
    if compilation:
        tags[COMPILATION] = compilation
    
    if lyrics and Zotify.CONFIG.get_save_lyrics_tags():
        tags[LYRICS] = "".join(lyrics)
    
    if ext == "mp3" and not Zotify.CONFIG.get_disc_track_totals():
        # music_tag python library writes DISCNUMBER and TRACKNUMBER as X/Y instead of X for mp3
        # this method bypasses all internal formatting, probably not resilient against arbitrary inputs
        tags.set_raw("mp3", "TPOS", str(disc_number))
        tags.set_raw("mp3", "TRCK", str(track_number))
    
    tags.save()


def get_audio_tags(filename: str | Path) -> tuple[tuple, tuple]:
    tags = music_tag.load_file(filename)
    
    artists = tags[ARTIST]
    genres = tags[GENRE]
    name = tags[TRACKTITLE]
    album_name = tags[ALBUM]
    album_artist = tags[ALBUMARTIST]
    release_year = tags[YEAR]
    disc_number = tags[DISCNUMBER]
    track_number = tags[TRACKNUMBER]
    
    unreliable_tags = ("trackid", TOTALTRACKS, TOTALDISCS, COMPILATION, LYRICS)
    
    return (artists, genres, name, album_name, album_artist, release_year, disc_number, track_number), \
           (tags[maybe_tag] if maybe_tag in tags else None for maybe_tag in unreliable_tags)


def compare_audio_tags(filename: str | Path, reliable_tags: tuple, unreliable_tags: tuple) -> bool:
    """ Compares music_tag metadata to provided metadata, returns True if all match """
    
    reliable_tags_onfile, unreliable_tags_onfile = get_audio_tags(filename)
    
    # Definite tags must match
    if len(reliable_tags) != len(reliable_tags_onfile):
        return False
    
    for i in range(len(reliable_tags)):
        if isinstance(reliable_tags[i], list) and isinstance(reliable_tags_onfile[i], list):
            if sorted(reliable_tags[i]) != sorted(reliable_tags_onfile[i]):
                return False
        else:
            if str(reliable_tags[i]) != str(reliable_tags_onfile[i]):
                return False
    
    if sum([bool(tag) for tag in unreliable_tags]) > sum([bool(tag) for tag in unreliable_tags_onfile]):
        return False
    
    return True


def set_music_thumbnail(filename: PurePath, image_url: str, mode: str) -> None:
    """ Fetch an album cover image, set album cover tag, and save to file if desired """
    
    # jpeg format expected from request
    img = requests.get(image_url).content
    tags = music_tag.load_file(filename)
    tags[ARTWORK] = img
    tags.save()
    
    if not Zotify.CONFIG.get_album_art_jpg_file():
        return
    
    jpg_filename = 'cover.jpg' if '{album}' in Zotify.CONFIG.get_output(mode) else filename.stem + '.jpg'
    jpg_path = Path(filename).parent.joinpath(jpg_filename)
    
    if not jpg_path.exists():
        with open(jpg_path, 'wb') as jpg_file:
            jpg_file.write(img)


# Time Utils
def get_downloaded_track_duration(filename: str) -> float:
    """ Returns the downloaded file's duration in seconds """
    
    command = ['ffprobe', '-show_entries', 'format=duration', '-i', f'{filename}']
    output = subprocess.run(command, capture_output=True)
    
    duration = re.search(r'[\D]=([\d\.]*)', str(output.stdout)).groups()[0]
    duration = float(duration)
    
    return duration


def fmt_duration(duration: float | int, unit_conv: tuple[int] = (60, 60), connectors: tuple[str] = (":", ":"), smallest_unit: str = "s", ALWAYS_ALL_UNITS: bool = False) -> str:
    """ Formats a duration to a time string, defaulting to seconds -> hh:mm:ss format """
    duration_secs = int(duration // 1)
    duration_mins = duration_secs // unit_conv[1]
    s = duration_secs % unit_conv[1]
    m = duration_mins % unit_conv[0]
    h = duration_mins // unit_conv[0]
    
    # Printer.debug(" ".join([f"{duration_secs}".zfill(5), f'{h}'.zfill(2), f'{m}'.zfill(2), f'{s}'.zfill(2)]))
    
    if not any((h, m, s)):
        return "0" + smallest_unit
    
    if ALWAYS_ALL_UNITS:
        return f'{h}'.zfill(2) + connectors[0] + f'{m}'.zfill(2) + connectors[1] + f'{s}'.zfill(2)
    
    if h == 0 and m == 0:
        return f'{s}' + smallest_unit
    elif h == 0:
        return f'{m}'.zfill(2) + connectors[1] + f'{s}'.zfill(2)
    else:
        return f'{h}'.zfill(2) + connectors[0] + f'{m}'.zfill(2) + connectors[1] + f'{s}'.zfill(2)


def strptime_utc(dtstr) -> datetime.datetime:
    return datetime.datetime.strptime(dtstr[:-1], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=datetime.timezone.utc)


def wait_between_downloads() -> None:
    waittime = Zotify.CONFIG.get_bulk_wait_time()
    if not waittime or waittime <= 0:
        return
    
    if waittime > 5:
        Printer.hashtaged(PrintChannel.DOWNLOADS, f'PAUSED: WAITING FOR {waittime} SECONDS BETWEEN DOWNLOADS')
    sleep(waittime)


# Song Archive Utils
def get_archived_entries() -> list[str]:
    """ Returns list of all time downloaded song entries """
    
    archive_path = Zotify.CONFIG.get_song_archive_location()
    
    entries = []
    if Path(archive_path).exists() and not Zotify.CONFIG.get_disable_song_archive():
        with open(archive_path, 'r', encoding='utf-8') as f:
            entries = f.readlines()
    
    return entries


def get_archived_song_ids() -> list[str]:
    """ Returns list of all-time downloaded track_ids """
    
    entries = get_archived_entries()
    
    track_ids = [entry.strip().split('\t')[0] for entry in entries]
    
    return track_ids


def add_to_song_archive(track_id: str, filename: str, author_name: str, track_name: str) -> None:
    """ Adds song id to all time installed songs archive """
    
    if Zotify.CONFIG.get_disable_song_archive():
        return
    
    archive_path = Zotify.CONFIG.get_song_archive_location()
    if Path(archive_path).exists():
        with open(archive_path, 'a', encoding='utf-8') as file:
            file.write(f'{track_id}\t{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\t{author_name}\t{track_name}\t{filename}\n')
    else:
        with open(archive_path, 'w', encoding='utf-8') as file:
            file.write(f'{track_id}\t{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\t{author_name}\t{track_name}\t{filename}\n')


def get_directory_song_ids(download_path: str) -> list[str]:
    """ Gets song ids of songs in directory """
    
    track_ids = []
    
    hidden_file_path = PurePath(download_path).joinpath('.song_ids')
    
    if Path(hidden_file_path).is_file() and not Zotify.CONFIG.get_disable_directory_archives():
        with open(hidden_file_path, 'r', encoding='utf-8') as file:
            track_ids.extend([line.strip().split('\t')[0] for line in file.readlines()])
    
    return track_ids


def add_to_directory_song_archive(download_path: str, track_id: str, filename: str, author_name: str, track_name: str) -> None:
    """ Appends song_id to .song_ids file in directory """
    
    if Zotify.CONFIG.get_disable_directory_archives():
        return
    
    hidden_file_path = PurePath(download_path).joinpath('.song_ids')
    # not checking if file exists because we need an exception
    # to be raised if something is wrong
    with open(hidden_file_path, 'a', encoding='utf-8') as file:
        file.write(f'{track_id}\t{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\t{author_name}\t{track_name}\t{filename}\n')


# Playlist File Utils
def add_to_m3u8(liked_m3u8: bool, track_duration: float, track_name: str, track_path: PurePath) -> str | None:
    """ Adds song to a .m3u8 playlist, returning the song label in m3u8 format"""
    
    m3u_dir = Zotify.CONFIG.get_m3u8_location()
    if m3u_dir is None:
        m3u_dir = track_path.parent
    
    if liked_m3u8:
        m3u_path = track_path.parent / (Zotify.datetime_launch + "_zotify.m3u8")
        if not Path(track_path.parent / "Liked Songs.m3u8").exists() or "justCreatedLikedSongsM3U8" in globals():
            m3u_path = track_path.parent / "Liked Songs.m3u8"
            global justCreatedLikedSongsM3U8; justCreatedLikedSongsM3U8 = True # hacky, terrible, truly awful: too bad!
    else:
        m3u_path = m3u_dir / (Zotify.datetime_launch + "_zotify.m3u8")
    
    if not Path(m3u_path).exists():
        Path(m3u_path.parent).mkdir(parents=True, exist_ok=True)
        with open(m3u_path, 'x', encoding='utf-8') as file:
            file.write("#EXTM3U\n\n")
    
    track_label_m3u = None
    with open(m3u_path, 'a', encoding='utf-8') as file:
        track_label_m3u = f"#EXTINF:{int(track_duration)}, {track_name}\n"
        if Zotify.CONFIG.get_m3u8_relative_paths():
            track_path = os.path.relpath(track_path, m3u_path.parent)
        
        file.write(track_label_m3u)
        file.write(f"{track_path}\n\n")
    return track_label_m3u


def fetch_m3u8_songs(m3u_path: PurePath) -> list[str] | None:
    """ Fetches the songs and associated file paths in an .m3u8 playlist"""
    
    if not Path(m3u_path).exists():
        return
    
    with open(m3u_path, 'r', encoding='utf-8') as file:
        linesraw = file.readlines()[2:-1]
        # group by song and filepath
        # songsgrouped = []
        # for i in range(len(linesraw)//3):
        #     songsgrouped.append(linesraw[3*i:3*i+3])
    return linesraw
