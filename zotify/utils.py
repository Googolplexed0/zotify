import datetime
import math
import os
import re
import subprocess
import music_tag
import requests
from time import sleep
from pathlib import Path, PurePath

from zotify.const import ALBUMARTIST, ARTIST, TRACKTITLE, ALBUM, YEAR, DISCNUMBER, \
    TRACKNUMBER, ARTWORK, TOTALTRACKS, TOTALDISCS, EXT_MAP, LYRICS, COMPILATION, GENRE
from zotify.zotify import Zotify
from zotify.termoutput import PrintChannel, Printer


def get_archived_song_ids() -> list[str]:
    """ Returns list of all time downloaded songs """
    
    ids = []
    archive_path = Zotify.CONFIG.get_song_archive_location()
    
    if Path(archive_path).exists():
        with open(archive_path, 'r', encoding='utf-8') as f:
            ids = [line.strip().split('\t')[0] for line in f.readlines()]
    
    return ids


def add_to_song_archive(song_id: str, filename: str, author_name: str, song_name: str) -> None:
    """ Adds song id to all time installed songs archive """
    
    if Zotify.CONFIG.get_disable_song_archive():
        return
    
    archive_path = Zotify.CONFIG.get_song_archive_location()
    if Path(archive_path).exists():
        with open(archive_path, 'a', encoding='utf-8') as file:
            file.write(f'{song_id}\t{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\t{author_name}\t{song_name}\t{filename}\n')
    else:
        with open(archive_path, 'w', encoding='utf-8') as file:
            file.write(f'{song_id}\t{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\t{author_name}\t{song_name}\t{filename}\n')


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


def get_directory_song_ids(download_path: str) -> list[str]:
    """ Gets song ids of songs in directory """
    
    song_ids = []
    
    hidden_file_path = PurePath(download_path).joinpath('.song_ids')
    
    if Path(hidden_file_path).is_file() and not Zotify.CONFIG.get_disable_directory_archives():
        with open(hidden_file_path, 'r', encoding='utf-8') as file:
            song_ids.extend([line.strip().split('\t')[0] for line in file.readlines()])
    
    return song_ids


def add_to_directory_song_archive(download_path: str, song_id: str, filename: str, author_name: str, song_name: str) -> None:
    """ Appends song_id to .song_ids file in directory """
    
    if Zotify.CONFIG.get_disable_directory_archives():
        return
    
    hidden_file_path = PurePath(download_path).joinpath('.song_ids')
    # not checking if file exists because we need an exception
    # to be raised if something is wrong
    with open(hidden_file_path, 'a', encoding='utf-8') as file:
        file.write(f'{song_id}\t{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\t{author_name}\t{song_name}\t{filename}\n')


def get_downloaded_song_duration(filename: str) -> float:
    """ Returns the downloaded file's duration in seconds """
    
    command = ['ffprobe', '-show_entries', 'format=duration', '-i', f'{filename}']
    output = subprocess.run(command, capture_output=True)
    
    duration = re.search(r'[\D]=([\d\.]*)', str(output.stdout)).groups()[0]
    duration = float(duration)
    
    return duration


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
            start, end = ids.split('-')
            inputs.extend(list(range(int(start), int(end) + 1)))
        else:
            inputs.append(int(ids))
    inputs.sort()
    
    return inputs


def add_to_m3u8(liked_m3u8: bool, song_duration: float, song_name: str, song_path: PurePath) -> str | None:
    """ Adds song to a .m3u8 playlist, returning the song label in m3u8 format"""
    
    m3u_dir = Zotify.CONFIG.get_m3u8_location()
    if m3u_dir is None:
        m3u_dir = song_path.parent
    
    if liked_m3u8:
        m3u_path = song_path.parent / (Zotify.datetime_launch + "_zotify.m3u8")
        if not Path(song_path.parent / "Liked Songs.m3u8").exists() or "justCreatedLikedSongsM3U8" in globals():
            m3u_path = song_path.parent / "Liked Songs.m3u8"
            global justCreatedLikedSongsM3U8; justCreatedLikedSongsM3U8 = True # hacky, terrible, truly awful: too bad!
    else:
        m3u_path = m3u_dir / (Zotify.datetime_launch + "_zotify.m3u8")
    
    if not Path(m3u_path).exists():
        Path(m3u_path.parent).mkdir(parents=True, exist_ok=True)
        with open(m3u_path, 'x', encoding='utf-8') as file:
            file.write("#EXTM3U\n\n")
    
    song_label_m3u = None
    with open(m3u_path, 'a', encoding='utf-8') as file:
        song_label_m3u = f"#EXTINF:{int(song_duration)}, {song_name}\n"
        if Zotify.CONFIG.get_m3u8_relative_paths():
            song_path = os.path.relpath(song_path, m3u_path.parent)
        
        file.write(song_label_m3u)
        file.write(f"{song_path}\n\n")
    return song_label_m3u


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


def conv_artist_format(artists: list[str]) -> list[str] | str:
    """ Returns converted artist format """
    if Zotify.CONFIG.get_artist_delimiter() == "":
        return artists
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


def set_audio_tags(filename, artists: list[str], genres: list[str], name, album_name, album_artist, release_year, disc_number, track_number, total_tracks, total_discs, compilation: int, lyrics: list[str] | None) -> None:
    """ sets music_tag metadata """
    tags = music_tag.load_file(filename)
    tags[ALBUMARTIST] = album_artist
    tags[ARTIST] = conv_artist_format(artists)
    tags[GENRE] = conv_genre_format(genres)
    tags[TRACKTITLE] = name
    tags[ALBUM] = album_name
    tags[YEAR] = release_year
    tags[DISCNUMBER] = disc_number
    tags[TRACKNUMBER] = track_number
    
    if compilation:
        tags[COMPILATION] = compilation
    
    if Zotify.CONFIG.get_disc_track_totals():
        tags[TOTALTRACKS] = total_tracks
        if total_discs is not None:
            tags[TOTALDISCS] = total_discs
    
    ext = EXT_MAP[Zotify.CONFIG.get_download_format().lower()]
    if ext == "mp3" and not Zotify.CONFIG.get_disc_track_totals():
        # music_tag python library writes DISCNUMBER and TRACKNUMBER as X/Y instead of X for mp3
        # this method bypasses all internal formatting, probably not resilient against arbitrary inputs
        tags.set_raw("mp3", "TPOS", str(disc_number))
        tags.set_raw("mp3", "TRCK", str(track_number))
    
    if lyrics and Zotify.CONFIG.get_save_lyrics_tags():
        tags[LYRICS] = "".join(lyrics)
    
    tags.save()


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


def fmt_seconds(secs: float) -> str:
    val = math.floor(secs)
    
    s = math.floor(val % 60)
    val -= s
    val /= 60
    
    m = math.floor(val % 60)
    val -= m
    val /= 60
    
    h = math.floor(val)
    
    if h == 0 and m == 0 and s == 0:
        return "0s"
    elif h == 0 and m == 0:
        return f'{s}s'.zfill(2)
    elif h == 0:
        return f'{m}'.zfill(2) + ':' + f'{s}'.zfill(2)
    else:
        return f'{h}'.zfill(2) + ':' + f'{m}'.zfill(2) + ':' + f'{s}'.zfill(2)


def strptime_utc(dtstr) -> datetime.datetime:
    return datetime.datetime.strptime(dtstr[:-1], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=datetime.timezone.utc)


def wait_between_downloads() -> None:
    waittime = Zotify.CONFIG.get_bulk_wait_time()
    if not waittime or waittime <= 0:
        return
    
    if waittime > 5:
        Printer.print(PrintChannel.DOWNLOADS, f'###   PAUSED: WAITING FOR {waittime} SECONDS BETWEEN DOWNLOADS   ###')
    sleep(waittime)
