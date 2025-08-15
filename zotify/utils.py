import datetime
import os
import re
import subprocess
import requests
import music_tag
from music_tag.file import TAG_MAP_ENTRY
from music_tag.mp4 import freeform_set
from mutagen.id3 import TXXX
from mutagen.oggvorbis import OggVorbis
from mutagen import File
from time import sleep
from pathlib import Path, PurePath

from zotify.config import Zotify
from zotify.const import ALBUMARTIST, ARTIST, TRACKTITLE, ALBUM, YEAR, DISCNUMBER, TRACKNUMBER, ARTWORK, \
    TOTALTRACKS, TOTALDISCS, EXT_MAP, LYRICS, COMPILATION, GENRE, MP3_CUSTOM_TAG_PREFIX, M4A_CUSTOM_TAG_PREFIX
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


def fill_output_template(output_template: str, track_metadata: dict, extra_keys: dict) -> tuple[str, str]:
    
    for k in extra_keys:
        output_template = output_template.replace("{"+k+"}", fix_filename(extra_keys[k]))
    
    (scraped_track_id, name, artists, artist_ids, release_date, release_year, track_number, total_tracks,
     album, album_artists, disc_number, compilation, duration_ms, image_url, is_playable) = track_metadata.values()
    
    output_template = output_template.replace("{artist}", fix_filename(artists[0]))
    output_template = output_template.replace("{album_artist}", fix_filename(album_artists[0]))
    output_template = output_template.replace("{album}", fix_filename(album))
    output_template = output_template.replace("{song_name}", fix_filename(name))
    output_template = output_template.replace("{release_year}", fix_filename(release_year))
    output_template = output_template.replace("{disc_number}", fix_filename(disc_number))
    output_template = output_template.replace("{track_number}", fix_filename(track_number))
    output_template = output_template.replace("{total_tracks}", fix_filename(total_tracks))
    output_template = output_template.replace("{id}", fix_filename(scraped_track_id))
    output_template = output_template.replace("{track_id}", fix_filename(scraped_track_id))
    
    ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower())
    output_template += f".{ext}"
    
    return output_template, fix_filename(artists[0]) + ' - ' + fix_filename(name)


def walk_directory_for_tracks(path: str | PurePath) -> set[Path]:
    # path must already exist
    track_paths = set()
    
    for dirpath, dirnames, filenames in os.walk(Path(path)):
        for filename in filenames:
            if filename.endswith(tuple(set(EXT_MAP.values()))):
                track_paths.update({Path(dirpath) / filename,})
    
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


def split_sanitize_intrange(raw_input: str) -> list[int]:
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
        # if len(artists) == 1:
        #     return artists[0]
        return ", ".join(artists) if FORCE_NO_LIST else artists
    else:
        return Zotify.CONFIG.get_artist_delimiter().join(artists)


def conv_genre_format(genres: list[str]) -> list[str] | str:
    """ Returns converted genre format """
    if not Zotify.CONFIG.get_all_genres():
        return genres[0]
    
    if Zotify.CONFIG.get_genre_delimiter() == "":
        # if len(genres) == 1:
        #     return genres[0]
        return genres
    else:
        return Zotify.CONFIG.get_genre_delimiter().join(genres)


def set_audio_tags(track_path: PurePath, track_metadata: dict, total_discs: str | None, genres: list[str], lyrics: list[str] | None) -> None:
    """ sets music_tag metadata """
    
    try:
        (scraped_track_id, track_name, artists, artist_ids, release_date, release_year, track_number, total_tracks,
         album, album_artists, disc_number, compilation, duration_ms, image_url, is_playable) = track_metadata.values()
        ext = EXT_MAP[Zotify.CONFIG.get_download_format().lower()]
        
        Printer.debug(f"Setting metadata for {track_path}")
        Printer.debug(f"Track: {track_name}, Artist: {artists}, Album: {album}")
        Printer.debug(f"File extension: {ext}")
        
        # Check if music_tag supports this format
        try:
            tags = music_tag.load_file(track_path)
            
            # Reliable Tags
            tags[ARTIST] = conv_artist_format(artists, FORCE_NO_LIST=True)
            tags[GENRE] = conv_genre_format(genres)
            tags[TRACKTITLE] = track_name
            tags[ALBUM] = album
            tags[ALBUMARTIST] = conv_artist_format(album_artists, FORCE_NO_LIST=True)
            tags[YEAR] = release_year
            tags[DISCNUMBER] = disc_number
            tags[TRACKNUMBER] = track_number
            
            # Unreliable Tags
            if ext == "mp3":
                tags.mfile.tags.add(TXXX(encoding=3, desc='TRACKID', text=[scraped_track_id]))
            elif ext == "m4a":
                freeform_set(tags, M4A_CUSTOM_TAG_PREFIX + "trackid",  type('tag', (object,), {'values': [scraped_track_id]})())
            else:
                tags.tag_map["trackid"] = TAG_MAP_ENTRY(getter="trackid", setter="trackid", type=str)
                tags["trackid"] = scraped_track_id
            
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
            Printer.debug(f"Metadata saved successfully for {track_path}")
            
        except Exception as e:
            Printer.hashtaged(
                PrintChannel.WARNING,
                f"music_tag failed for {track_path.name} ({ext}): {str(e)}"
            )
            Printer.hashtaged(
                PrintChannel.WARNING,
                f"Trying alternative metadata method for OGG files..."
            )
            
            # For OGG files, try using mutagen directly
            if ext == "ogg":
                try:
                    # Try to load the file with mutagen
                    audio = File(str(track_path))
                    if audio is None:
                        audio = OggVorbis(str(track_path))
                    
                    # Set VorbisComment tags
                    if hasattr(audio, 'tags') and audio.tags is not None:
                        audio.tags['title'] = [track_name]
                        audio.tags['artist'] = [conv_artist_format(artists, FORCE_NO_LIST=True)]
                        audio.tags['album'] = [album]
                        audio.tags['albumartist'] = [conv_artist_format(album_artists, FORCE_NO_LIST=True)]
                        audio.tags['date'] = [release_year]
                        audio.tags['tracknumber'] = [track_number]
                        audio.tags['discnumber'] = [disc_number]
                        audio.tags['genre'] = [conv_genre_format(genres)]
                        audio.tags['trackid'] = [scraped_track_id]
                        
                        if Zotify.CONFIG.get_disc_track_totals():
                            audio.tags['totaltracks'] = [total_tracks]
                            if total_discs is not None:
                                audio.tags['totaldiscs'] = [total_discs]
                        
                        if compilation:
                            audio.tags['compilation'] = [str(compilation)]
                        
                        if lyrics and Zotify.CONFIG.get_save_lyrics_tags():
                            audio.tags['lyrics'] = ["".join(lyrics)]
                        
                        audio.save()
                        Printer.debug(f"OGG metadata saved successfully using mutagen for {track_path}")
                    else:
                        Printer.hashtaged(
                            PrintChannel.WARNING,
                            f"Could not access tags for OGG file: {track_path}"
                        )
                        
                except Exception as ogg_error:
                    Printer.hashtaged(
                        PrintChannel.WARNING,
                        f"Failed to set OGG metadata with mutagen: {str(ogg_error)}"
                    )
            else:
                Printer.hashtaged(
                    PrintChannel.WARNING,
                    f"Unsupported format for metadata: {ext}"
                )
        
    except Exception as e:
        Printer.hashtaged(
            PrintChannel.ERROR,
            f"Failed to set audio tags for {track_path.name}: {str(e)}"
        )
        Printer.traceback(e)


def get_audio_tags(track_path: Path) -> tuple[tuple, tuple]:
    tags = music_tag.load_file(track_path)
    
    artists = conv_artist_format(tags[ARTIST].values)
    genres = conv_genre_format(tags[GENRE].values)
    track_name = tags[TRACKTITLE].val
    album_name = tags[ALBUM].val
    album_artist = conv_artist_format(tags[ALBUMARTIST].values)
    release_year = str(tags[YEAR].val)
    disc_number = str(tags[DISCNUMBER].val)
    track_number = str(tags[TRACKNUMBER].val).zfill(2)
    
    unreliable_tags = [TOTALTRACKS, TOTALDISCS, COMPILATION, LYRICS]
    custom_tags = ["trackid"]
    if track_path.suffix.lower() == ".mp3":
        custom_tags = [MP3_CUSTOM_TAG_PREFIX + tag.upper() for tag in custom_tags]
    elif track_path.suffix.lower() == ".m4a":
        custom_tags = [M4A_CUSTOM_TAG_PREFIX + tag for tag in custom_tags]
    unreliable_tags.extend(custom_tags)
    
    # Printer.debug(tags.mfile.tags.__dict__)
    tag_dict = dict(tags.mfile.tags)
    utag_vals = []
    for utag in unreliable_tags:
        val = None
        fetch_method = "legit"
        try:
            val = tags[utag].val
        except:
            fetch_method = "hacky"
            if utag in tag_dict:
                val = tag_dict[utag]
        
        if utag == LYRICS:
            val = [line + "\n" for line in val.splitlines()]
        elif utag == COMPILATION:
            val = int(val)
        elif MP3_CUSTOM_TAG_PREFIX in utag:
            val = val.text
            if len(val) == 1:
                val = val[0]
        elif M4A_CUSTOM_TAG_PREFIX in utag:
            if len(val) == 1:
                val = val[0].decode()
            else:
                val = [v.decode() for v in val]
        else:
            val = val[0] if isinstance(val, (list, tuple)) and len(val) == 1 else val
            val = val if val else None
        # Printer.debug(f"{fetch_method} {utag}", val)
        utag_vals.append(val)
    
    return (artists, genres, track_name, album_name, album_artist, release_year, disc_number, track_number), \
           tuple(utag_vals)


def compare_audio_tags(track_path: str | Path, reliable_tags: tuple, unreliable_tags: tuple) -> list | bool:
    """ Compares music_tag metadata to provided metadata, returns Truthy value if discrepancy is found """
    
    reliable_tags_onfile, unreliable_tags_onfile = get_audio_tags(track_path)
    
    mismatches = []
    
    # Definite tags must match
    if len(reliable_tags) != len(reliable_tags_onfile):
        if not Zotify.CONFIG.debug():
            return True
    
    for i in range(len(reliable_tags)):
        if isinstance(reliable_tags[i], list) and isinstance(reliable_tags_onfile[i], list):
            if sorted(reliable_tags[i]) != sorted(reliable_tags_onfile[i]):
                mismatches.append( (reliable_tags[i], reliable_tags_onfile[i]) )
        else:
            if str(reliable_tags[i]) != str(reliable_tags_onfile[i]):
                mismatches.append( (reliable_tags[i], reliable_tags_onfile[i]) )
    
    if mismatches:
        return mismatches
    
    # If more unreliable tags are received from API than found on file, assume the file is outdated
    if sum([bool(tag) for tag in unreliable_tags]) > sum([bool(tag) for tag in unreliable_tags_onfile]):
        if not Zotify.CONFIG.get_strict_library_verify() and not Zotify.CONFIG.debug():
            return True
    
    # stickler check for unreliable tags
    for i in range(len(unreliable_tags)):
        if isinstance(unreliable_tags[i], list) and isinstance(unreliable_tags_onfile[i], list):
            # do not sort lyrics, since order matters
            if unreliable_tags[i] != unreliable_tags_onfile[i]:
                mismatches.append( (unreliable_tags[i], unreliable_tags_onfile[i]) )
        else:
            if str(unreliable_tags[i]) != str(unreliable_tags_onfile[i]):
                mismatches.append( (unreliable_tags[i], unreliable_tags_onfile[i]) )
    
    return mismatches


def set_music_thumbnail(track_path: PurePath, image_url: str, mode: str) -> None:
    """ Fetch an album cover image, set album cover tag, and save to file if desired """
    
    try:
        # Download the image that Spotify returns
        img = requests.get(image_url).content
        
        # Try music_tag first
        try:
            tags = music_tag.load_file(track_path)
            tags[ARTWORK] = img
            tags.save()
            Printer.debug(f"Album art embedded successfully using music_tag for {track_path}")
            
        except Exception as e:
            Printer.hashtaged(
                PrintChannel.WARNING,
                f"music_tag failed for album art in {track_path.name}: {str(e)}"
            )
            
            # For OGG files, try using mutagen directly
            if track_path.suffix.lower() == '.ogg':
                try:
                    from mutagen.oggvorbis import OggVorbis
                    from mutagen import File
                    
                    # Try to load the file with mutagen
                    audio = File(str(track_path))
                    if audio is None:
                        audio = OggVorbis(str(track_path))
                    
                    # Set the artwork using mutagen
                    if hasattr(audio, 'tags') and audio.tags is not None:
                        # For OGG files, add artwork as a custom VorbisComment
                        # This is more reliable than METADATA_BLOCK_PICTURE
                        audio.tags['artwork'] = [img]
                        audio.save()
                        Printer.debug(f"OGG album art embedded successfully using mutagen for {track_path}")
                    else:
                        Printer.hashtaged(
                            PrintChannel.WARNING,
                            f"Could not access tags for OGG file: {track_path}"
                        )
                        
                except Exception as ogg_error:
                    Printer.hashtaged(
                        PrintChannel.WARNING,
                        f"Failed to embed OGG album art with mutagen: {str(ogg_error)}"
                    )
            else:
                Printer.hashtaged(
                    PrintChannel.WARNING,
                    f"Unsupported format for album art: {track_path.suffix}"
                )
        
    except Exception as e:
        Printer.hashtaged(
            PrintChannel.WARNING,
            f"Failed to embed album art in {track_path.name}: {str(e)}"
        )
    
    # Save separate JPG file if configured
    if not Zotify.CONFIG.get_album_art_jpg_file():
        return
    
    try:
        jpg_filename = 'cover.jpg' if '{album}' in Zotify.CONFIG.get_output(mode) else track_path.stem + '.jpg'
        jpg_path = Path(track_path).parent.joinpath(jpg_filename)
        
        if not jpg_path.exists():
            with open(jpg_path, 'wb') as jpg_file:
                jpg_file.write(img)
    except Exception as e:
        Printer.hashtaged(
            PrintChannel.WARNING,
            f"Failed to save separate album art file: {str(e)}"
        )


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


def add_to_directory_song_archive(track_path: PurePath, track_id: str, author_name: str, track_name: str) -> None:
    """ Appends song_id to .song_ids file in directory """
    
    if Zotify.CONFIG.get_disable_directory_archives():
        return
    
    hidden_file_path = track_path.parent / '.song_ids'
    # not checking if file exists because we need an exception
    # to be raised if something is wrong
    with open(hidden_file_path, 'a', encoding='utf-8') as file:
        file.write(f'{track_id}\t{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\t{author_name}\t{track_name}\t{track_path.name}\n')


# Playlist File Utils
def add_to_m3u8(duration_ms: int, track_name: str, track_path: PurePath, m3u8_path: PurePath | None) -> str | None:
    """ Adds song to a .m3u8 playlist, returning the song label in m3u8 format"""
    
    if m3u8_path is None:
        m3u_dir = Zotify.CONFIG.get_m3u8_location()
        if m3u_dir is None:
            m3u_dir = track_path.parent
        m3u8_path = m3u_dir / (Zotify.DATETIME_LAUNCH + "_zotify.m3u8")
    elif m3u8_path.name == "Liked Songs.m3u8": # may get confused if playlist is named "Liked Songs"
        m3u8_path = track_path.parent / (Zotify.DATETIME_LAUNCH + "_zotify.m3u8")
        if not Path(track_path.parent / "Liked Songs.m3u8").exists() or "justCreatedLikedSongsM3U8" in globals():
            m3u8_path = track_path.parent / "Liked Songs.m3u8"
            global justCreatedLikedSongsM3U8; justCreatedLikedSongsM3U8 = True # hacky, terrible, truly awful: too bad!
    
    if not Path(m3u8_path).exists():
        Path(m3u8_path.parent).mkdir(parents=True, exist_ok=True)
        with open(m3u8_path, 'x', encoding='utf-8') as file:
            file.write("#EXTM3U\n\n")
    
    track_label_m3u = None
    with open(m3u8_path, 'a', encoding='utf-8') as file:
        track_label_m3u = f"#EXTINF:{duration_ms // 1000}, {track_name}\n"
        if Zotify.CONFIG.get_m3u8_relative_paths():
            track_path = os.path.relpath(track_path, m3u8_path.parent)
        
        file.write(track_label_m3u)
        file.write(f"{track_path}\n\n")
    return track_label_m3u


def fetch_m3u8_songs(m3u8_path: PurePath) -> list[str] | None:
    """ Fetches the songs and associated file paths in an .m3u8 playlist"""
    
    if not Path(m3u8_path).exists():
        return
    
    with open(m3u8_path, 'r', encoding='utf-8') as file:
        linesraw = file.readlines()[2:-1]
        # group by song and filepath
        # songsgrouped = []
        # for i in range(len(linesraw)//3):
        #     songsgrouped.append(linesraw[3*i:3*i+3])
    return linesraw
