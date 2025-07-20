import time
import uuid
import ffmpy
from typing import Any
from pathlib import Path, PurePath
from librespot.metadata import TrackId

from zotify import __version__
from zotify.const import TRACKS, ALBUM, GENRES, NAME, DISC_NUMBER, TRACK_NUMBER, TOTAL_TRACKS, \
    IS_PLAYABLE, ARTISTS, IMAGES, URL, RELEASE_DATE, ID, TRACKS_URL, TRACK_STATS_URL, \
    CODEC_MAP, EXT_MAP, DURATION_MS, ARTISTS, WIDTH, COMPILATION, ALBUM_TYPE, ARTIST_BULK_URL
from zotify.config import EXPORT_M3U8
from zotify.termoutput import Printer, PrintChannel, Loader
from zotify.utils import fix_filename, set_audio_tags, set_music_thumbnail, create_download_directory, \
    add_to_m3u8, fetch_m3u8_songs, get_directory_song_ids, add_to_directory_song_archive, \
    get_archived_song_ids, add_to_song_archive, fmt_duration, wait_between_downloads, conv_artist_format, \
    conv_genre_format, compare_audio_tags
from zotify.zotify import Zotify


def parse_track_metadata(track_metadata: dict[str, Any]) -> tuple[list[str], list[Any], str, str, str, Any, Any, Any, Any, Any, Any, Any, Any, int]:
    artists = []
    artist_ids = []
    for data in track_metadata[ARTISTS]:
        artists.append(data[NAME])
        artist_ids.append(data[ID])
    
    album_name = track_metadata[ALBUM][NAME]
    album_artist = track_metadata[ALBUM][ARTISTS][0][NAME]
    album_compilation = 1 if COMPILATION in track_metadata[ALBUM][ALBUM_TYPE] else 0
    name = track_metadata[NAME]
    release_year = track_metadata[ALBUM][RELEASE_DATE].split('-')[0]
    disc_number = track_metadata[DISC_NUMBER]
    track_number = track_metadata[TRACK_NUMBER]
    total_tracks = track_metadata[ALBUM][TOTAL_TRACKS]
    scraped_track_id = track_metadata[ID]
    is_playable = track_metadata[IS_PLAYABLE]
    duration_ms = track_metadata[DURATION_MS]
    
    image = track_metadata[ALBUM][IMAGES][0]
    for i in track_metadata[ALBUM][IMAGES]:
        if i[WIDTH] > image[WIDTH]:
            image = i
    image_url = image[URL]
    
    return (artists, artist_ids, album_name, album_artist, name, 
            image_url, release_year, disc_number, track_number, total_tracks, 
            album_compilation, scraped_track_id, is_playable, duration_ms)


def get_track_info(track_id: str) -> tuple[list[str], list[Any], str, str, str, Any, Any, Any, Any, Any, Any, Any, Any, int]:
    """ Retrieves metadata for downloaded songs """
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching track information..."):
        (raw, info) = Zotify.invoke_url(f'{TRACKS_URL}?ids={track_id}&market=from_token')
    
    if not TRACKS in info:
        raise ValueError(f'Invalid response from TRACKS_URL:\n{raw}')
    
    try:
        return parse_track_metadata(info[TRACKS][0])
    except Exception as e:
        raise ValueError(f'Failed to parse TRACKS_URL response: {str(e)}\n{raw}')


def get_track_genres(artist_ids: list[str], track_name: str) -> list[str]:
    if Zotify.CONFIG.get_save_genres():
        with Loader(PrintChannel.PROGRESS_INFO, "Fetching genre information..."):
            
            artists = Zotify.invoke_url_bulk(ARTIST_BULK_URL, artist_ids, ARTISTS)
            
            genres = set()
            for artist in artists:
                if GENRES in artist and len(artist[GENRES]) > 0:
                    genres.update(artist[GENRES])
        
        if len(genres) == 0:
            Printer.hashtaged(PrintChannel.WARNING, 'NO GENRES FOUND\n' +\
                                                   f'Track_Name: {track_name}')
            genres = ['']
        else:
            genres = list(genres)
            genres.sort()
        
        return genres
        
    else:
        return ['']


def get_track_lyrics(track_id: str) -> list[str]:
    # expect failure here, lyrics are not guaranteed to be available
    (raw, lyrics_dict) = Zotify.invoke_url('https://spclient.wg.spot' + f'ify.com/color-lyrics/v2/track/{track_id}', expectFail=True)
    if lyrics_dict:
        try:
            formatted_lyrics = lyrics_dict['lyrics']['lines']
        except KeyError:
            raise ValueError(f'Failed to fetch lyrics: {track_id}')
        
        if(lyrics_dict['lyrics']['syncType'] == "UNSYNCED"):
            lyrics = [line['words'] + '\n' for line in formatted_lyrics]
        elif(lyrics_dict['lyrics']['syncType'] == "LINE_SYNCED"):
            lyrics = []
            Printer.debug("Synced Lyric Timestamps:")
            for line in formatted_lyrics:
                timestamp = int(line['startTimeMs']) // 10
                ts = fmt_duration(timestamp // 1, (60, 100), (':', '.'), "cs", True)
                Printer.debug(f"{timestamp}".zfill(5) + f" {ts.split(':')[0]} {ts.split(':')[1].replace('.', ' ')}")
                lyrics.append(f'[{ts}]' + line['words'] + '\n')
        return lyrics
    raise ValueError(f'Failed to fetch lyrics: {track_id}')


def get_track_duration(track_id: str) -> float:
    """ Retrieves duration of song in seconds according to track API stats """
    
    (raw, resp) = Zotify.invoke_url(f'{TRACK_STATS_URL}{track_id}')
    
    # get duration in miliseconds
    ms_duration = resp['duration_ms']
    # convert to seconds
    duration = float(ms_duration)/1000
    
    return duration


def handle_lyrics(track_id: str, track_name: str, filedir: PurePath,
                  name: str, artists: list[str], album: str, duration_ms: int) -> list[str] | None:
    lyrics = None
    if not Zotify.CONFIG.get_download_lyrics() and not Zotify.CONFIG.get_always_check_lyrics():
        return lyrics
    
    try:
        lyricdir = Zotify.CONFIG.get_lyrics_location()
        if lyricdir is None:
            lyricdir = filedir
        
        Path(lyricdir).mkdir(parents=True, exist_ok=True)
        
        lyrics = get_track_lyrics(track_id)
        
        lrc_header = [f"[ti: {name}]\n",
                      f"[ar: {conv_artist_format(artists, FORCE_NO_LIST=True)}]\n",
                      f"[al: {album}]\n",
                      f"[length: {duration_ms // 60000}:{(duration_ms % 60000) // 1000}]\n",
                      f"[by: Zotify v{__version__}]\n",
                      "\n"]
        
        with open(lyricdir / f"{track_name}.lrc", 'w', encoding='utf-8') as file:
            if Zotify.CONFIG.get_lyrics_header():
                file.writelines(lrc_header)
            file.writelines(lyrics)
        
    except ValueError:
        Printer.hashtaged(PrintChannel.SKIPPING, f'LYRICS FOR "{track_name}" (LYRICS NOT AVAILABLE)')
    return lyrics


def update_track_metadata(track_id: str, track_file: Path, track_metadata: str) -> None:
    (artists, artist_ids, album_name, album_artist, name, image_url, release_year, disc_number,
     track_number, total_tracks, compilation, _, _, duration_ms) = parse_track_metadata(track_metadata)
    
    #TODO implement total discs
    total_discs = None 
    
    genres = get_track_genres(artist_ids, name)
    
    track_name = fix_filename(artists[0]) + ' - ' + fix_filename(name)
    lyrics = handle_lyrics(track_id, track_name, track_file.parent, name, artists, album_name, duration_ms)
    
    reliable_tags = (conv_artist_format(artists), conv_genre_format(genres), name, album_name, album_artist, release_year, disc_number, track_number)
    unreliable_tags = (track_id, total_tracks, total_discs, compilation, lyrics)
    
    if compare_audio_tags(track_file, reliable_tags, unreliable_tags):
        Printer.print(PrintChannel.SKIPS, f'###   SKIPPING:  METADATA FOR "{track_file.name}" (TAGS ARE UP-TO-DATE)   ###\n')
        return
    
    try:
        set_audio_tags(track_file.name, track_id, artists, genres, name, album_name, album_artist, release_year, 
                        disc_number, track_number, total_tracks, total_discs, compilation, lyrics)
        set_music_thumbnail(track_file.name, image_url, mode="single")
    except Exception as e:
        Printer.print(PrintChannel.ERRORS, "###   ERROR:  FAILED TO WRITE METADATA   ###\n" +\
                                            "###   Ensure FFMPEG is installed and added to your PATH   ###")
        Printer.traceback(e)


def download_track(mode: str, track_id: str, extra_keys: dict | None = None, pbar_stack: list | None = None) -> None:
    """ Downloads raw song audio content stream"""
    
    # recursive header for parent album download
    child_request_mode = mode
    child_request_id = track_id
    if Zotify.CONFIG.get_download_parent_album():
        if mode == "album" and "M3U8_bypass" in extra_keys and extra_keys["M3U8_bypass"] is not None:
            child_request_mode, child_request_id = extra_keys.pop("M3U8_bypass")
        else:
            album_id = total_tracks = None
            try:
                (raw, info) = Zotify.invoke_url(f'{TRACKS_URL}?ids={track_id}&market=from_token')
                album_id = info[TRACKS][0][ALBUM][ID]
                total_tracks = info[TRACKS][0][ALBUM][TOTAL_TRACKS]
            except:
                Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FIND PARENT ALBUM\n' +\
                                                     f'Track_ID: {track_id}')
            
            if album_id and total_tracks and int(total_tracks) > 1:
                from zotify.album import download_album
                # uses album OUTPUT template for filename formatting, but handle m3u8 as if only this track was downloaded
                download_album(album_id, pbar_stack, M3U8_bypass=(mode, track_id))
                return
    
    if extra_keys is None:
        extra_keys = {}
    
    try:
        output_template = Zotify.CONFIG.get_output(mode)
        
        (artists, artist_ids, album_name, album_artist, name, image_url, release_year, disc_number,
         track_number, total_tracks, compilation, scraped_track_id, is_playable, duration_ms) = get_track_info(track_id)
        total_discs = None
        if "total_discs" in extra_keys:
            total_discs = extra_keys["total_discs"]
        
        if Zotify.CONFIG.get_regex_track():
            regex_match = Zotify.CONFIG.get_regex_track().search(name)
            Printer.debug("Regex Check\n" +\
                         f"Pattern: {Zotify.CONFIG.get_regex_track().pattern}\n" +\
                         f"Song Name: {name}\n" +\
                         f"Match Object: {regex_match}")
            if regex_match:
                Printer.hashtaged(PrintChannel.SKIPPING, 'TRACK MATCHES REGEX FILTER\n' +\
                                                        f'Track_Name: {name} - Track_ID: {track_id}\n'+\
                                                       (f'Regex Groups: {regex_match.groupdict()}\n' if regex_match.groups() else ""))
                return
        
        prepare_download_loader = Loader(PrintChannel.PROGRESS_INFO, "Preparing download...")
        prepare_download_loader.start()
        
        track_name = fix_filename(artists[0]) + ' - ' + fix_filename(name)
        
        for k in extra_keys:
            output_template = output_template.replace("{"+k+"}", fix_filename(extra_keys[k]))
        
        ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower())
        
        output_template = output_template.replace("{artist}", fix_filename(artists[0]))
        output_template = output_template.replace("{album_artist}", fix_filename(album_artist))
        output_template = output_template.replace("{album}", fix_filename(album_name))
        output_template = output_template.replace("{song_name}", fix_filename(name))
        output_template = output_template.replace("{release_year}", fix_filename(release_year))
        output_template = output_template.replace("{disc_number}", fix_filename(disc_number))
        output_template = output_template.replace("{track_number}", '{:02d}'.format(int(fix_filename(track_number))))
        output_template = output_template.replace("{total_tracks}", fix_filename(total_tracks))
        output_template = output_template.replace("{id}", fix_filename(scraped_track_id))
        output_template = output_template.replace("{track_id}", fix_filename(track_id))
        output_template += f".{ext}"
        
        filename = PurePath(Zotify.CONFIG.get_root_path()).joinpath(output_template)
        filedir = PurePath(filename).parent
        
        filename_temp = filename
        if Zotify.CONFIG.get_temp_download_dir() != '':
            filename_temp = PurePath(Zotify.CONFIG.get_temp_download_dir()).joinpath(f'zotify_{str(uuid.uuid4())}_{track_id}.{ext}')
        
        check_name = Path(filename).is_file() and Path(filename).stat().st_size
        check_local = scraped_track_id in get_directory_song_ids(filedir)
        check_all_time = scraped_track_id in get_archived_song_ids()
        Printer.debug("Duplicate Check\n" +\
                     f"File Already Exists: {check_name}\n" +\
                     f"song_id in Local Archive: {check_local}\n" +\
                     f"song_id in Global Archive: {check_all_time}")
        
        # same filename, not same song_id, rename the newcomer
        if check_name and not check_local and not Zotify.CONFIG.get_disable_directory_archives():
            c = len([file for file in Path(filedir).iterdir() if file.match(filename.stem + "*")])
            filename = PurePath(filedir).joinpath(f'{filename.stem}_{c}{filename.suffix}')
            check_name = False # new filename guaranteed to be unique
        
        liked_m3u8 = child_request_mode == "liked" and Zotify.CONFIG.get_liked_songs_archive_m3u8()
        if Zotify.CONFIG.get_export_m3u8() and track_id == child_request_id:
            if liked_m3u8:
                m3u_path = filedir / "Liked Songs.m3u8"
                songs_m3u = fetch_m3u8_songs(m3u_path)
            track_label = add_to_m3u8(liked_m3u8, get_track_duration(track_id), track_name, filename)
            if liked_m3u8:
                if songs_m3u is not None and track_label in songs_m3u[0]:
                    Zotify.CONFIG.Values[EXPORT_M3U8] = False
                    Path(filedir / (Zotify.datetime_launch + "_zotify.m3u8")).replace(m3u_path)
                    with open(m3u_path, 'a', encoding='utf-8') as file:
                        file.writelines(songs_m3u[3:])
        
        if Zotify.CONFIG.get_always_check_lyrics():
            lyrics = handle_lyrics(track_id, track_name, filedir, name, artists, album_name, duration_ms)
    
    except Exception as e:
        if "prepare_download_loader" in locals():
            prepare_download_loader.stop()
        Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING SONG - FAILED TO QUERY METADATA\n' +\
                                             f'Track_ID: {track_id}')
        Printer.json_dump(extra_keys)
        Printer.traceback(e)
    
    else:
        try:
            if not is_playable:
                prepare_download_loader.stop()
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{track_name}" (TRACK IS UNAVAILABLE)')
            else:
                if check_name and Zotify.CONFIG.get_skip_existing() and Zotify.CONFIG.get_disable_directory_archives():
                    prepare_download_loader.stop()
                    Printer.hashtaged(PrintChannel.SKIPPING, f'"{PurePath(filename).relative_to(Zotify.CONFIG.get_root_path())}" (FILE ALREADY EXISTS)')
                
                elif check_local and Zotify.CONFIG.get_skip_existing() and not Zotify.CONFIG.get_disable_directory_archives():
                    prepare_download_loader.stop()
                    Printer.hashtaged(PrintChannel.SKIPPING, f'"{track_name}" (TRACK ALREADY EXISTS)')
                
                elif check_all_time and Zotify.CONFIG.get_skip_previously_downloaded():
                    prepare_download_loader.stop()
                    Printer.hashtaged(PrintChannel.SKIPPING, f'"{track_name}" (TRACK ALREADY DOWNLOADED ONCE)')
                
                else:
                    if track_id != scraped_track_id:
                        track_id = scraped_track_id
                    track = TrackId.from_base62(track_id)
                    stream = Zotify.get_content_stream(track, Zotify.DOWNLOAD_QUALITY)
                    if stream is None:
                        prepare_download_loader.stop()
                        Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING SONG - FAILED TO GET CONTENT STREAM\n' +\
                                                             f'Track_ID: {track_id}')
                        return
                    create_download_directory(filedir)
                    total_size = stream.input_stream.size
                    
                    prepare_download_loader.stop()
                    
                    time_start = time.time()
                    downloaded = 0
                    pos, pbar_stack = Printer.pbar_position_handler(1, pbar_stack)
                    with open(filename_temp, 'wb') as file, Printer.pbar(
                            desc=track_name,
                            total=total_size,
                            unit='B',
                            unit_scale=True,
                            unit_divisor=1024,
                            disable=not Zotify.CONFIG.get_show_download_pbar(),
                            pos=pos
                    ) as pbar:
                        b = 0
                        while b < 5:
                        #for _ in range(int(total_size / Zotify.CONFIG.get_chunk_size()) + 2):
                            data = stream.input_stream.stream().read(Zotify.CONFIG.get_chunk_size())
                            pbar.update(file.write(data))
                            downloaded += len(data)
                            b += 1 if data == b'' else 0
                            if Zotify.CONFIG.get_download_real_time():
                                delta_real = time.time() - time_start
                                delta_want = (downloaded / total_size) * (duration_ms/1000)
                                if delta_want > delta_real:
                                    time.sleep(delta_want - delta_real)
                    
                    time_dl_end = time.time()
                    time_elapsed_dl = fmt_duration(time_dl_end - time_start)
                    
                    genres = get_track_genres(artist_ids, name)
                    
                    lyrics = handle_lyrics(track_id, track_name, filedir, name, artists, album_name, duration_ms)
                    
                    # no metadata is written to track prior to conversion
                    time_elapsed_ffmpeg = convert_audio_format(filename_temp)
                    
                    try:
                        set_audio_tags(filename_temp, track_id, artists, genres, name, album_name, album_artist, release_year, 
                                       disc_number, track_number, total_tracks, total_discs, compilation, lyrics)
                        set_music_thumbnail(filename_temp, image_url, mode)
                    except Exception as e:
                        Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO WRITE METADATA\n' +\
                                                              'Ensure FFMPEG is installed and added to your PATH')
                        Printer.traceback(e)
                    
                    if filename_temp != filename:
                        if Path(filename).exists():
                            Path(filename).unlink()
                        Path(filename_temp).rename(filename)
                    
                    Printer.hashtaged(PrintChannel.DOWNLOADS, f'DOWNLOADED: "{PurePath(filename).relative_to(Zotify.CONFIG.get_root_path())}"\n' +\
                                                              f'DOWNLOAD TOOK {time_elapsed_dl} (PLUS {time_elapsed_ffmpeg} CONVERTING)')
                    
                    # add song ID to global .song_archive file
                    if not check_all_time:
                        add_to_song_archive(scraped_track_id, PurePath(filename).name, artists[0], name)
                    # add song ID to download directory's .song_ids file
                    if not check_local:
                        add_to_directory_song_archive(filedir, scraped_track_id, PurePath(filename).name, artists[0], name)
                    
                    wait_between_downloads()
            
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING SONG - GENERAL DOWNLOAD ERROR\n' +\
                                                 f'Track_Name: {track_name} - Track_ID: {track_id}')
            Printer.json_dump(extra_keys)
            Printer.traceback(e)
            if Path(filename_temp).exists():
                Path(filename_temp).unlink()
        
        prepare_download_loader.stop()


def convert_audio_format(filename) -> None:
    """ Converts raw audio into playable file """
    temp_filename = f'{PurePath(filename).parent}.tmp'
    Path(filename).replace(temp_filename)
    
    download_format = Zotify.CONFIG.get_download_format().lower()
    file_codec = CODEC_MAP.get(download_format, 'copy')
    bitrate = None
    if file_codec != 'copy':
        bitrate = Zotify.CONFIG.get_transcode_bitrate()
        if bitrate in {"auto", ""}:
            bitrates = {
                'auto': '320k' if Zotify.check_premium() else '160k',
                'normal': '96k',
                'high': '160k',
                'very_high': '320k'
            }
            bitrate = bitrates[Zotify.CONFIG.get_download_quality()]
    
    output_params = ['-c:a', file_codec]
    if bitrate is not None:
        output_params += ['-b:a', bitrate]
    
    time_ffmpeg_start = time.time()
    try:
        ff_m = ffmpy.FFmpeg(
            global_options=['-y', '-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
            inputs={temp_filename: None},
            outputs={filename: output_params}
        )
        with Loader(PrintChannel.PROGRESS_INFO, "Converting file..."):
            ff_m.run()
        
        if Path(temp_filename).exists():
            Path(temp_filename).unlink()
        
    except ffmpy.FFExecutableNotFoundError:
        Printer.hashtaged(PrintChannel.WARNING, 'FFMPEG NOT FOUND\n' +\
                                               f'SKIPPING CONVERSION TO {file_codec.upper()}')
    
    time_ffmpeg_end = time.time()
    return fmt_duration(time_ffmpeg_end - time_ffmpeg_start)
