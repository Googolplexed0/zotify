import time
import uuid
import ffmpy
from pathlib import Path, PurePath
from librespot.metadata import TrackId

from zotify import __version__
from zotify.config import Zotify
from zotify.const import TRACKS, ALBUM, GENRES, NAME, DISC_NUMBER, TRACK_NUMBER, TOTAL_TRACKS, \
    IS_PLAYABLE, ARTISTS, ARTIST_IDS, IMAGES, URL, RELEASE_DATE, ID, TRACK_URL, \
    CODEC_MAP, DURATION_MS, WIDTH, COMPILATION, ALBUM_TYPE, ARTIST_BULK_URL, YEAR, \
    ALBUM_ARTISTS, IMAGE_URL, EXPORT_M3U8
from zotify.termoutput import Printer, PrintChannel, Loader, PrintCategory
from zotify.utils import fill_output_template, set_audio_tags, set_music_thumbnail, create_download_directory, \
    add_to_m3u8, fetch_m3u8_songs, get_directory_song_ids, add_to_directory_song_archive, \
    get_archived_song_ids, add_to_song_archive, fmt_duration, wait_between_downloads, conv_artist_format, \
    conv_genre_format, compare_audio_tags, fix_filename


def parse_track_metadata(track_resp: dict) -> dict[str, list[str] | str | int | bool]:
    track_metadata: dict[str, list[str] | str | int | bool] = {}
    
    # track_metadata unpack to individual variables
    # (scraped_track_id, track_name, artists, artist_ids, release_date, release_year, track_number, total_tracks,
    # album, album_artists, disc_number, compilation, duration_ms, image_url, is_playable) = track_metadata.values()
    
    track_metadata[ID] = track_resp[ID] # str
    track_metadata[NAME] = track_resp[NAME] # str
    track_metadata[ARTISTS] = [artist[NAME] for artist in track_resp[ARTISTS]] # list[str]
    track_metadata[ARTIST_IDS] = [artist[ID] for artist in track_resp[ARTISTS]] # list[str]
    track_metadata[RELEASE_DATE] = track_resp[ALBUM][RELEASE_DATE] # date as str
    track_metadata[YEAR] = track_metadata[RELEASE_DATE].split('-')[0] # int as str
    track_metadata[TRACK_NUMBER] = str(track_resp[TRACK_NUMBER]).zfill(2) # int as str
    track_metadata[TOTAL_TRACKS] = str(track_resp[ALBUM][TOTAL_TRACKS]).zfill(2) # int as str
    track_metadata[ALBUM] = track_resp[ALBUM][NAME] # str
    track_metadata[ALBUM_ARTISTS] = [artist[NAME] for artist in track_resp[ALBUM][ARTISTS]] # list[str]
    track_metadata[DISC_NUMBER] = str(track_resp[DISC_NUMBER]) # int as str
    track_metadata[COMPILATION] = 1 if COMPILATION in track_resp[ALBUM][ALBUM_TYPE] else 0 # int
    track_metadata[DURATION_MS] = track_resp[DURATION_MS] # int
    
    largest_image = max(track_resp[ALBUM][IMAGES], key=lambda img: img[WIDTH], default=None)
    track_metadata[IMAGE_URL] = largest_image[URL] # str
    
    # not provided by playlist API, but available in track API
    track_metadata[IS_PLAYABLE] = track_resp[IS_PLAYABLE] if IS_PLAYABLE in track_resp else True # bool, assume true
    
    return track_metadata


def get_track_metadata(track_id) -> dict[str, list[str] | str | int | bool]:
    """ Retrieves metadata for downloaded songs """
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching track information..."):
        (raw, info) = Zotify.invoke_url(f'{TRACK_URL}?ids={track_id}&market=from_token')
        
        if not TRACKS in info:
            raise ValueError(f'Invalid response from TRACK_URL:\n{raw}')
        
        try:
            return parse_track_metadata(info[TRACKS][0])
        except Exception as e:
            raise ValueError(f'Failed to parse TRACK_URL response: {str(e)}\n{raw}')


def get_track_genres(artist_ids: list[str], track_name: str) -> list[str]:
    if Zotify.CONFIG.get_save_genres():
        with Loader(PrintChannel.PROGRESS_INFO, "Fetching genre information..."):
            
            artists = Zotify.invoke_url_bulk(ARTIST_BULK_URL, artist_ids, ARTISTS)
            
            genres = set()
            for artist in artists:
                if GENRES in artist and len(artist[GENRES]) > 0:
                    Printer.new_print(PrintChannel.PROGRESS_INFO, f'Found genres for artist: {artist[NAME]} - {", ".join(artist[GENRES])}', PrintCategory.GENERAL)
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
            tss = []
            for line in formatted_lyrics:
                timestamp = int(line['startTimeMs']) // 10
                ts = fmt_duration(timestamp // 1, (60, 100), (':', '.'), "cs", True)
                tss.append(f"{timestamp}".zfill(5) + f" {ts.split(':')[0]} {ts.split(':')[1].replace('.', ' ')}\n")
                lyrics.append(f'[{ts}]' + line['words'] + '\n')
            # Printer.debug("Synced Lyric Timestamps:\n" + "".join(tss))
        return lyrics
    raise ValueError(f'Failed to fetch lyrics: {track_id}')


def handle_lyrics(track_id: str, filedir: PurePath, track_metadata: dict) -> list[str] | None:
    lyrics = None
    if not Zotify.CONFIG.get_download_lyrics() and not Zotify.CONFIG.get_always_check_lyrics():
        return lyrics
    
    try:
        with Loader(PrintChannel.PROGRESS_INFO, "Fetching lyrics..."):
            track_label = fix_filename(track_metadata[ARTISTS][0]) + ' - ' + fix_filename(track_metadata[NAME])
            lyricdir = Zotify.CONFIG.get_lyrics_location()
            if lyricdir is None:
                lyricdir = filedir
            
            Path(lyricdir).mkdir(parents=True, exist_ok=True)
            
            lyrics = get_track_lyrics(track_id)
            
            lrc_header = [f"[ti: {track_metadata[NAME]}]\n",
                          f"[ar: {conv_artist_format(track_metadata[ARTISTS], FORCE_NO_LIST=True)}]\n",
                          f"[al: {track_metadata[ALBUM]}]\n",
                          f"[length: {track_metadata[DURATION_MS] // 60000}:{(track_metadata[DURATION_MS] % 60000) // 1000}]\n",
                          f"[by: Zotify v{__version__}]\n",
                          "\n"]
            
            with open(lyricdir / f"{track_label}.lrc", 'w', encoding='utf-8') as file:
                if Zotify.CONFIG.get_lyrics_header():
                    file.writelines(lrc_header)
                file.writelines(lyrics)
        
    except ValueError:
        Printer.hashtaged(PrintChannel.SKIPPING, f'LYRICS FOR "{track_label}" (LYRICS NOT AVAILABLE)')
    return lyrics


def update_track_metadata(track_id: str, track_path: Path, track_resp: dict) -> None:
    track_metadata = parse_track_metadata(track_resp)
    (scraped_track_id, track_name, artists, artist_ids, release_date, release_year, track_number, total_tracks,
     album, album_artists, disc_number, compilation, duration_ms, image_url, is_playable) = track_metadata.values()
    total_discs = None #TODO implement total discs or just ignore to halve API calls
    
    genres = get_track_genres(track_metadata[ARTIST_IDS], track_name)
    lyrics = handle_lyrics(track_id, track_path.parent, track_metadata)
    
    reliable_tags = (conv_artist_format(artists), conv_genre_format(genres), track_name, album, 
                     conv_artist_format(album_artists), release_year, disc_number, track_number)
    unreliable_tags = (track_id, total_tracks if Zotify.CONFIG.get_disc_track_totals() else None,
                       total_discs if Zotify.CONFIG.get_disc_track_totals() else None, compilation, lyrics)
    
    mismatches = compare_audio_tags(track_path, reliable_tags, unreliable_tags)
    if not mismatches:
        Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{track_path.relative_to(Zotify.CONFIG.get_root_path())}"\n' +\
                                                   '(NO UPDATES REQUIRED)')
        return
    
    try:
        Printer.debug(f'Metadata Mismatches:', mismatches)
        set_audio_tags(track_path, track_metadata, total_discs, genres, lyrics)
        set_music_thumbnail(track_path, track_metadata[IMAGE_URL], mode="single")
        Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{track_path.relative_to(Zotify.CONFIG.get_root_path())}"\n' +\
                                                  f'(UPDATED TAGS TO MATCH CURRENT API METADATA)')
    except Exception as e:
        Printer.hashtaged(PrintChannel.ERROR, "FAILED TO WRITE METADATA\n" +\
                                              "Ensure FFMPEG is installed and added to your PATH")
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
                (raw, info) = Zotify.invoke_url(f'{TRACK_URL}?ids={track_id}&market=from_token')
                album_id = info[TRACKS][0][ALBUM][ID]
                total_tracks = info[TRACKS][0][ALBUM][TOTAL_TRACKS]
            except:
                Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FIND PARENT ALBUM\n' +\
                                                     f'Track_ID: {track_id}')
            
            if album_id and total_tracks and int(total_tracks) > 1:
                from zotify.album import download_album
                # uses album OUTPUT template for track_path formatting, but handle m3u8 as if only this track was downloaded
                download_album(album_id, pbar_stack, M3U8_bypass=(mode, track_id))
                return
    
    if extra_keys is None:
        extra_keys = {}
    
    try:
        track_metadata = get_track_metadata(track_id)
        
        with Loader(PrintChannel.PROGRESS_INFO, "Preparing download..."):
            track_name = track_metadata[NAME]
            total_discs = None
            if "total_discs" in extra_keys:
                total_discs = extra_keys["total_discs"]
            
            if Zotify.CONFIG.get_regex_track():
                regex_match = Zotify.CONFIG.get_regex_track().search(track_name)
                Printer.debug("Regex Check\n" +\
                             f"Pattern: {Zotify.CONFIG.get_regex_track().pattern}\n" +\
                             f"Song Name: {track_name}\n" +\
                             f"Match Object: {regex_match}")
                if regex_match:
                    Printer.hashtaged(PrintChannel.SKIPPING, 'TRACK MATCHES REGEX FILTER\n' +\
                                                            f'Track_Name: {track_name} - Track_ID: {track_id}\n'+\
                                                        (f'Regex Groups: {regex_match.groupdict()}\n' if regex_match.groups() else ""))
                    return
            
            output_template = Zotify.CONFIG.get_output(mode)
            root_to_track, track_label = fill_output_template(output_template, track_metadata, extra_keys)
            
            track_path = PurePath(Zotify.CONFIG.get_root_path()).joinpath(root_to_track)
            filedir = PurePath(track_path).parent
            
            track_path_temp = track_path
            if Zotify.CONFIG.get_temp_download_dir() != '':
                track_path_temp = PurePath(Zotify.CONFIG.get_temp_download_dir()).joinpath(f'zotify_{str(uuid.uuid4())}_{track_id}.{track_path.suffix}')
            
            track_path_exists = Path(track_path).is_file() and Path(track_path).stat().st_size
            in_dir_songids = track_metadata[ID] in get_directory_song_ids(filedir)
            in_global_songids = track_metadata[ID] in get_archived_song_ids()
            Printer.debug("Duplicate Check\n" +\
                         f"File Already Exists: {track_path_exists}\n" +\
                         f"song_id in Local Archive: {in_dir_songids}\n" +\
                         f"song_id in Global Archive: {in_global_songids}")
            
            # same track_path, not same song_id, rename the newcomer
            if track_path_exists and not in_dir_songids and not Zotify.CONFIG.get_disable_directory_archives():
                c = len([file for file in Path(filedir).iterdir() if file.match(track_path.stem + "*")])
                track_path = PurePath(filedir).joinpath(f'{track_path.stem}_{c}{track_path.suffix}')
                track_path_exists = False # new track_path guaranteed to be unique
            
            liked_m3u8 = child_request_mode == "liked" and Zotify.CONFIG.get_liked_songs_archive_m3u8()
            if Zotify.CONFIG.get_export_m3u8() and track_id == child_request_id:
                m3u8_path: PurePath | None = extra_keys['m3u8_path'] if 'm3u8_path' in extra_keys else None
                if liked_m3u8:
                    m3u8_path = filedir / "Liked Songs.m3u8"
                    songs_m3u = fetch_m3u8_songs(m3u8_path)
                track_m3u8_label = add_to_m3u8(track_metadata[DURATION_MS], track_label, track_path, m3u8_path)
                if liked_m3u8:
                    if songs_m3u is not None and track_m3u8_label in songs_m3u[0]:
                        Zotify.CONFIG.Values[EXPORT_M3U8] = False
                        Path(filedir / (Zotify.DATETIME_LAUNCH + "_zotify.m3u8")).replace(m3u8_path)
                        with open(m3u8_path, 'a', encoding='utf-8') as file:
                            file.writelines(songs_m3u[3:])
        
        if Zotify.CONFIG.get_always_check_lyrics():
            lyrics = handle_lyrics(track_id, filedir, track_metadata)
    
    except Exception as e:
        Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING SONG - FAILED TO QUERY METADATA\n' +\
                                             f'Track_ID: {track_id}')
        Printer.json_dump(extra_keys)
        Printer.traceback(e)
    
    else:
        try:
            if not track_metadata[IS_PLAYABLE]:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{track_label}" (TRACK IS UNAVAILABLE)')
            else:
                if track_path_exists and Zotify.CONFIG.get_skip_existing() and Zotify.CONFIG.get_disable_directory_archives():
                    Printer.hashtaged(PrintChannel.SKIPPING, f'"{PurePath(track_path).relative_to(Zotify.CONFIG.get_root_path())}" (FILE ALREADY EXISTS)')
                
                elif in_dir_songids and Zotify.CONFIG.get_skip_existing() and not Zotify.CONFIG.get_disable_directory_archives():
                    Printer.hashtaged(PrintChannel.SKIPPING, f'"{track_label}" (TRACK ALREADY EXISTS)')
                
                elif in_global_songids and Zotify.CONFIG.get_skip_previously_downloaded():
                    Printer.hashtaged(PrintChannel.SKIPPING, f'"{track_label}" (TRACK ALREADY DOWNLOADED ONCE)')
                
                else:
                    if track_id != track_metadata[ID]:
                        track_id = track_metadata[ID]
                    track = TrackId.from_base62(track_id)
                    stream = Zotify.get_content_stream(track, Zotify.DOWNLOAD_QUALITY)
                    if stream is None:
                        Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING SONG - FAILED TO GET CONTENT STREAM\n' +\
                                                             f'Track_ID: {track_id}')
                        return
                    create_download_directory(filedir)
                    total_size = stream.input_stream.size
                    
                    time_start = time.time()
                    downloaded = 0
                    pos, pbar_stack = Printer.pbar_position_handler(1, pbar_stack)
                    with open(track_path_temp, 'wb') as file, Printer.pbar(
                            desc=track_label,
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
                                delta_want = (downloaded / total_size) * (track_metadata[DURATION_MS]/1000)
                                if delta_want > delta_real:
                                    time.sleep(delta_want - delta_real)
                    
                    time_dl_end = time.time()
                    time_elapsed_dl = fmt_duration(time_dl_end - time_start)
                    
                    genres = get_track_genres(track_metadata[ARTIST_IDS], track_name)
                    
                    lyrics = handle_lyrics(track_id, filedir, track_metadata)
                    
                    # no metadata is written to track prior to conversion
                    time_elapsed_ffmpeg = convert_audio_format(track_path_temp)
                    
                    try:
                        set_audio_tags(track_path_temp, track_metadata, total_discs, genres, lyrics)
                        set_music_thumbnail(track_path_temp, track_metadata[IMAGE_URL], mode)
                    except Exception as e:
                        Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO WRITE METADATA\n' +\
                                                              'Ensure FFMPEG is installed and added to your PATH')
                        Printer.traceback(e)
                    
                    if track_path_temp != track_path:
                        if Path(track_path).exists():
                            Path(track_path).unlink()
                        Path(track_path_temp).rename(track_path)
                    
                    Printer.hashtaged(PrintChannel.DOWNLOADS, f'DOWNLOADED: "{PurePath(track_path).relative_to(Zotify.CONFIG.get_root_path())}"\n' +\
                                                              f'DOWNLOAD TOOK {time_elapsed_dl} (PLUS {time_elapsed_ffmpeg} CONVERTING)')
                    
                    if not in_global_songids:
                        add_to_song_archive(track_metadata[ID], PurePath(track_path).name, track_metadata[ARTISTS][0], track_name)
                    if not in_dir_songids:
                        add_to_directory_song_archive(track_path, track_metadata[ID], track_metadata[ARTISTS][0], track_name)
                    
                    wait_between_downloads()
            
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING SONG - GENERAL DOWNLOAD ERROR\n' +\
                                                 f'Track_Label: {track_label} - Track_ID: {track_id}')
            Printer.json_dump(extra_keys)
            Printer.traceback(e)
            if Path(track_path_temp).exists():
                Path(track_path_temp).unlink()


def convert_audio_format(track_path) -> None:
    """ Converts raw audio into playable file """
    temp_track_path = f'{PurePath(track_path).parent}.tmp'
    Path(track_path).replace(temp_track_path)
    
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
            inputs={temp_track_path: None},
            outputs={track_path: output_params}
        )
        with Loader(PrintChannel.PROGRESS_INFO, "Converting file..."):
            ff_m.run()
        
        if Path(temp_track_path).exists():
            Path(temp_track_path).unlink()
        
    except Exception as e:
        if isinstance(e, ffmpy.FFExecutableNotFoundError):
            reason = 'FFMPEG NOT FOUND\n'
        else:
            reason = str(e) + "\n"
        Printer.hashtaged(PrintChannel.WARNING, reason + f'SKIPPING CONVERSION TO {file_codec.upper()}')
    
    time_ffmpeg_end = time.time()
    return fmt_duration(time_ffmpeg_end - time_ffmpeg_start)
