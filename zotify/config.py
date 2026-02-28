import json
import logging
import sys
import re
import requests
from base64 import b64encode, b64decode
from contextlib import contextmanager
from datetime import datetime
from google.protobuf.json_format import MessageToDict, ParseDict
from librespot import metadata
from librespot.audio import FeederException, GeneralAudioStream, CdnFeedHelper
from librespot.audio.decoders import AudioQuality, SuperAudioFormat, FormatOnlyAudioQuality
from librespot.core import Session, OAuth, MercuryRequests
from librespot.proto.Authentication_pb2 import AuthenticationType
from librespot.proto.Metadata_pb2 import AudioFile
from pathlib import Path, PurePath
from time import sleep
from typing import Any, Callable

from zotify.const import *
from zotify.termoutput import Printer, PrintChannel, Loader

FORCE_STREAM_API_CALL = False


CONFIG_VALUES = {
    # Main Options
    ROOT_PATH:                  { 'default': '~/Music/Zotify Music',    'type': str,    'arg': ('-rp', '--root-path'                     ,) },
    SAVE_CREDENTIALS:           { 'default': 'True',                    'type': bool,   'arg': ('--save-credentials'                     ,) },
    CREDENTIALS_LOCATION:       { 'default': '',                        'type': str,    'arg': ('--creds', '--credentials-location'      ,) },
    
    # File Options
    OUTPUT:                     { 'default': '',                        'type': str,    'arg': ('--output'                               ,) },
    OUTPUT_SINGLE:              { 'default': '{artist}/{album}/{artist}_{song_name}',
                                  'type': str,
                                  'arg': ('-os', '--output-single' ,) },
    OUTPUT_ALBUM:               { 'default': '{artist}/{album}/{album_num}_{artist}_{song_name}',
                                  'type': str,
                                  'arg': ('-oa', '--output-album' ,) },
    OUTPUT_PLAYLIST_EXT:        { 'default': '{playlist}/{playlist_num}_{artist}_{song_name}',
                                  'type': str,  
                                  'arg': ('-oe', '--output-ext-playlist' ,) },
    OUTPUT_LIKED_SONGS:         { 'default': 'Liked Songs/{artist}_{song_name}',
                                  'type': str,
                                  'arg': ('-ol', '--output-liked-songs' ,) },
    ROOT_PODCAST_PATH:          { 'default': '~/Music/Zotify Podcasts', 'type': str,    'arg': ('-rpp', '--root-podcast-path'            ,) },
    SPLIT_ALBUM_DISCS:          { 'default': 'False',                   'type': bool,   'arg': ('--split-album-discs'                    ,) },
    MAX_FILENAME_LENGTH:        { 'default': '0',                       'type': int,    'arg': ('--max-filename-length'                  ,) },
    
    # Download Options
    OPTIMIZED_DOWNLOADING:      { 'default': 'True',                    'type': bool,   'arg': ('--optimized-downloading'                ,) },
    BULK_WAIT_TIME:             { 'default': '1',                       'type': int,    'arg': ('--bulk-wait-time'                       ,) },
    DOWNLOAD_REAL_TIME:         { 'default': 'False',                   'type': bool,   'arg': ('-rt', '--download-real-time'            ,) },
    TEMP_DOWNLOAD_DIR:          { 'default': '',                        'type': str,    'arg': ('-td', '--temp-download-dir'             ,) },
    
    # Album/Artist Options
    DOWNLOAD_PARENT_ALBUM:      { 'default': 'False',                   'type': bool,   'arg': ('--download-parent-album'                ,) },
    NO_COMPILATION_ALBUMS:      { 'default': 'False',                   'type': bool,   'arg': ('--no-compilation-albums'                ,) },
    NO_VARIOUS_ARTISTS:         { 'default': 'False',                   'type': bool,   'arg': ('--no-various-artists'                   ,) },
    NO_ARTIST_APPEARS_ON:       { 'default': 'False',                   'type': bool,   'arg': ('--no-artist-appears-on'                 ,) },
    DISCOG_BY_ALBUM_ARTIST:     { 'default': 'False',                   'type': bool,   'arg': ('--discog-by-album-artist'               ,) },
    
    # Regex Options
    REGEX_ENABLED:              { 'default': 'False',                   'type': bool,   'arg': ('--regex-enabled'                        ,) },
    REGEX_TRACK_SKIP:           { 'default': '',                        'type': str,    'arg': ('--regex-track-skip'                     ,) },
    REGEX_EPISODE_SKIP:         { 'default': '',                        'type': str,    'arg': ('--regex-episode-skip'                   ,) },
    REGEX_ALBUM_SKIP:           { 'default': '',                        'type': str,    'arg': ('--regex-album-skip'                     ,) },
    
    # Encoding Options
    DOWNLOAD_FORMAT:            { 'default': 'copy',                    'type': str,    'arg': ('--codec', '--download-format'           ,) },
    DOWNLOAD_QUALITY:           { 'default': 'auto',                    'type': str,    'arg': ('-q', '--download-quality'               ,) },
    TRANSCODE_BITRATE:          { 'default': 'auto',                    'type': str,    'arg': ('-b', '--bitrate', '--transcode-bitrate' ,) },
    CUSTOM_FFMEPG_ARGS:         { 'default': '',                        'type': str,    'arg': ('--custom-ffmpeg-args'                   ,) },
    
    # Archive Options
    SONG_ARCHIVE_LOCATION:      { 'default': '',                        'type': str,    'arg': ('--song-archive-location'                ,) },
    DISABLE_SONG_ARCHIVE:       { 'default': 'False',                   'type': bool,   'arg': ('--disable-song-archive'                 ,) },
    DISABLE_DIRECTORY_ARCHIVES: { 'default': 'False',                   'type': bool,   'arg': ('--disable-directory-archives'           ,) },
    SKIP_EXISTING:              { 'default': 'True',                    'type': bool,   'arg': ('-ie', '--skip-existing'                 ,) },
    SKIP_PREVIOUSLY_DOWNLOADED: { 'default': 'False',                   'type': bool,   'arg': ('-ip', '--skip-prev-downloaded', 
                                                                                                '--skip-previously-downloaded'           ,) },
    
    # Playlist File Options
    EXPORT_M3U8:                { 'default': 'False',                   'type': bool,   'arg': ('-e, --export-m3u8'                      ,) },
    M3U8_LOCATION:              { 'default': '',                        'type': str,    'arg': ('--m3u8-location'                        ,) },
    M3U8_REL_PATHS:             { 'default': 'True',                    'type': bool,   'arg': ('--m3u8-relative-paths'                  ,) },
    LIKED_SONGS_ARCHIVE_M3U8:   { 'default': 'True',                    'type': bool,   'arg': ('--liked-songs-archive-m3u8'             ,) },
    
    # Lyrics Options
    LYRICS_TO_METADATA:         { 'default': 'True',                    'type': bool,   'arg': ('--lyrics-to-metadata'                   ,) },
    LYRICS_TO_FILE:             { 'default': 'True',                    'type': bool,   'arg': ('--lyrics-to-file'                       ,) },
    LYRICS_LOCATION:            { 'default': '',                        'type': str,    'arg': ('--lyrics-location'                      ,) },
    OUTPUT_LYRICS:              { 'default': '{artist}_{song_name}',    'type': str,    'arg': ('-oy', '--output-lyrics'                 ,) },
    ALWAYS_CHECK_LYRICS:        { 'default': 'False',                   'type': bool,   'arg': ('--always-check-lyrics'                  ,) },
    LYRICS_MD_HEADER:           { 'default': 'False',                   'type': bool,   'arg': ('--lyrics-md-header'                     ,) },
    
    # Metadata Options
    API_CLIENT_ID:              { 'default': '',                        'type': str,    'arg': ('--client-id'                            ,) },
    LANGUAGE:                   { 'default': 'en',                      'type': str,    'arg': ('--language'                             ,) },
    MD_DISC_TRACK_TOTALS:       { 'default': 'True',                    'type': bool,   'arg': ('--md-disc-track-totals'                 ,) },
    MD_SAVE_GENRES:             { 'default': 'True',                    'type': bool,   'arg': ('--md-save-genres'                       ,) },
    MD_ALLGENRES:               { 'default': 'False',                   'type': bool,   'arg': ('--md-allgenres'                         ,) },
    MD_GENREDELIMITER:          { 'default': ', ',                      'type': str,    'arg': ('--md-genredelimiter'                    ,) },
    MD_ARTISTDELIMITER:         { 'default': ', ',                      'type': str,    'arg': ('--md-artistdelimiter'                   ,) },
    SEARCH_QUERY_SIZE:          { 'default': '10',                      'type': str,    'arg': ('--search-query-size'                    ,) },
    STRICT_LIBRARY_VERIFY:      { 'default': 'True',                    'type': bool,   'arg': ('--strict-library-verify'                ,) },
    ALBUM_ART_JPG_FILE:         { 'default': 'False',                   'type': bool,   'arg': ('--album-art-jpg-file'                   ,) },
    
    # API Options
    RETRY_ATTEMPTS:             { 'default': '1',                       'type': int,    'arg': ('--retry-attempts'                       ,) },
    CHUNK_SIZE:                 { 'default': '20000',                   'type': int,    'arg': ('--chunk-size'                           ,) },
    REDIRECT_ADDRESS:           { 'default': '127.0.0.1',               'type': str,    'arg': ('--redirect-address'                     ,) },
    
    # Terminal & Logging Options
    PRINT_SPLASH:               { 'default': 'False',                   'type': bool,   'arg': ('--print-splash'                         ,) },
    PRINT_PROGRESS_INFO:        { 'default': 'True',                    'type': bool,   'arg': ('--print-progress-info'                  ,) },
    PRINT_SKIPS:                { 'default': 'True',                    'type': bool,   'arg': ('--print-skips'                          ,) },
    PRINT_DOWNLOADS:            { 'default': 'True',                    'type': bool,   'arg': ('--print-downloads'                      ,) },
    PRINT_DOWNLOAD_PROGRESS:    { 'default': 'True',                    'type': bool,   'arg': ('--print-download-progress'              ,) },
    PRINT_URL_PROGRESS:         { 'default': 'True',                    'type': bool,   'arg': ('--print-url-progress'                   ,) },
    PRINT_ALBUM_PROGRESS:       { 'default': 'True',                    'type': bool,   'arg': ('--print-album-progress'                 ,) },
    PRINT_ARTIST_PROGRESS:      { 'default': 'True',                    'type': bool,   'arg': ('--print-artist-progress'                ,) },
    PRINT_PLAYLIST_PROGRESS:    { 'default': 'True',                    'type': bool,   'arg': ('--print-playlist-progress'              ,) },
    PRINT_WARNINGS:             { 'default': 'True',                    'type': bool,   'arg': ('--print-warnings'                       ,) },
    PRINT_ERRORS:               { 'default': 'True',                    'type': bool,   'arg': ('--print-errors'                         ,) },
    PRINT_API_ERRORS:           { 'default': 'True',                    'type': bool,   'arg': ('--print-api-errors'                     ,) },
    STANDARD_INTERFACE:         { 'default': 'False',                   'type': bool,   'arg': ('--standard-interface'                   ,) },
    FFMPEG_LOG_LEVEL:           { 'default': 'error',                   'type': str,    'arg': ('--ffmpeg-log-level'                     ,) },
}  


DEPRECIATED_CONFIGS = {
    "SONG_ARCHIVE":             { 'default': '',                        'type': str,    'arg': ('--song-archive'                         ,) },
    "OVERRIDE_AUTO_WAIT":       { 'default': 'False',                   'type': bool,   'arg': ('--override-auto-wait'                   ,) },
    "REDIRECT_URI":             { 'default': '127.0.0.1:4381',          'type': str,    'arg': ('--redirect-uri'                         ,) },
    "OAUTH_ADDRESS":            { 'default': '0.0.0.0',                 'type': str,    'arg': ('--oauth-address'                        ,) },
    "OUTPUT_PLAYLIST":          { 'default': '{playlist}/{artist}_{song_name}',
                                  'type': str, 
                                  'arg': ('-op', '--output-playlist' ,) },
    "DOWNLOAD_LYRICS":          { 'default': 'True',                    'type': bool,   'arg': ('--download-lyrics'                      ,) },
    "LYRICS_FILENAME":          { 'default': '{artist}_{song_name}',    'type': str,    'arg': ('--lyrics-filename'                      ,) },
    "MD_SAVE_LYRICS":           { 'default': 'True',                    'type': bool,   'arg': ('--md-save-lyrics'                       ,) },
    "BYPASS_MD_API":            { 'default': 'False',                   'type': bool,   'arg': ('--bypass-metadata-api'                  ,) },
    
}


class Config:
    Values = {}
    
    @classmethod
    def load(cls, args) -> None:
        from zotify.utils import safe_typecast
        
        system_paths = {
            'win32': Path.home() / 'AppData/Roaming/Zotify',
            'linux': Path.home() / '.config/zotify',
            'darwin': Path.home() / 'Library/Application Support/Zotify'
        }
        if sys.platform not in system_paths:
            config_fp = Path.cwd() / '.zotify/config.json'
        else:
            config_fp = system_paths[sys.platform] / 'config.json'
        if args.config_location:
            config_fp = Path(args.config_location)
            if config_fp.is_dir():
                config_fp = config_fp / 'config.json'
        full_config_path = Path(config_fp).expanduser()
        
        # Debug Check (guarantee at top of config)
        cmd_args: dict = vars(args)
        cls.Values[DEBUG] = safe_typecast(cmd_args, DEBUG.lower(), bool)
        
        # Load default values
        for cfg, cfg_setup in CONFIG_VALUES.items():
            cls.Values[cfg] = safe_typecast(cfg_setup, 'default', cfg_setup['type'])
        
        # Load config from config.json
        Path(PurePath(full_config_path).parent).mkdir(parents=True, exist_ok=True)
        if not Path(full_config_path).exists():
            if cls.Values[DEBUG] == False: del cls.Values[DEBUG]
            with open(full_config_path, 'w', encoding='utf-8') as config_file:
                json.dump(cls.get_default_json(), config_file, indent=4)
            Printer.hashtaged(PrintChannel.MANDATORY, f"config.json saved to {full_config_path.resolve().parent}")
        else:
            with open(full_config_path, encoding='utf-8') as config_file:
                jsonvalues: dict[str, dict[str, Any]] = json.load(config_file)
            for cfg in jsonvalues:
                if cfg == DEBUG and not cls.Values[DEBUG]:
                    cls.Values[DEBUG] = safe_typecast(jsonvalues, cfg, bool)
                elif cfg in CONFIG_VALUES:
                    cls.Values[cfg] = safe_typecast(jsonvalues, cfg, CONFIG_VALUES[cfg]['type'])
                elif cfg in DEPRECIATED_CONFIGS: # keep, warn, and place at the bottom (don't delete)
                    Printer.depreciated_warning(cfg, f'Delete the `"{cfg}": "{jsonvalues[cfg]}"` line from your config.json')
                    cls.Values["vvv___DEPRECIATED_BELOW_HERE___vvv"] = "vvv___REMOVE_THESE___vvv"
                    cls.Values[cfg] = safe_typecast(jsonvalues, cfg, DEPRECIATED_CONFIGS[cfg]['type'])
        
        # Standardize config.json if debugging or refreshing 
        if cls.debug() or args.update_config:
            if cls.debug() and not full_config_path.name.endswith("_DEBUG.json"):
                full_config_path = full_config_path.parent / (full_config_path.stem + "_DEBUG.json")
            with open(full_config_path, 'w' if full_config_path.exists() else 'x', encoding='utf-8') as debug_file:
                json.dump(cls.parse_config_jsonstr(), debug_file, indent=4)
            real_debug = cls.Values[DEBUG]; cls.Values[DEBUG] = True
            Printer.hashtaged(PrintChannel.DEBUG, f"{full_config_path.name} saved to {full_config_path.resolve().parent}")
            cls.Values[DEBUG] = real_debug
        
        # Override config from commandline arguments
        for cfg in CONFIG_VALUES:
            if cmd_args.get(cfg.lower()) is not None:
                cls.Values[cfg] = safe_typecast(cmd_args, cfg.lower(), CONFIG_VALUES[cfg]['type'])
        
        # Confirm regex patterns
        if cls.get_regex_enabled():
            for mode in ["Track", "Episode", "Album"]:
                regex_method: Callable[[None], None | re.Pattern] = getattr(cls, f"get_regex_{mode.lower()}")
                if regex_method(): Printer.hashtaged(PrintChannel.DEBUG, f'{mode} Regex Filter:  r"{regex_method().pattern}"')
        
        # Check no-splash
        if args.no_splash:
            cls.Values[PRINT_SPLASH] = False
        
        # Check update_archive
        cls.Values[UPDATE_ARCHIVE] = cls.debug() or args.update_archive or args.verify_library
    
    @classmethod
    def get_default_json(cls) -> dict:
        r = {}
        # if DEBUG in cls.Values and cls.Values[DEBUG]:
        #     r[DEBUG] = True
        for key in CONFIG_VALUES:
            r[key] = CONFIG_VALUES[key]['default']
        return r
    
    @classmethod
    def parse_config_jsonstr(cls, key_subset: tuple | dict | None = None) -> dict:
        d = {}
        if key_subset is None: key_subset = cls.Values
        for key in key_subset:
            d[key] = str(cls.Values[key])
        return d
    
    @classmethod
    def get(cls, key: str) -> Any:
        return cls.Values.get(key)
    
    @classmethod
    @contextmanager
    def temporary_config(cls, cfg: str, temp_value):
        from zotify.utils import safe_typecast
        original_val = cls.get(cfg)
        cls.Values[cfg] = safe_typecast({cfg: temp_value}, cfg, CONFIG_VALUES[cfg]['type'])
        try:
            yield
        finally:
            cls.Values[cfg] = original_val
    
    @classmethod
    def debug(cls) -> bool:
        return cls.Values.get(DEBUG)
    
    @classmethod
    def get_root_path(cls) -> PurePath:
        if cls.get(ROOT_PATH) == '':
            root_path = PurePath(Path.home() / 'Music/Zotify Music/')
        else:
            root_path = PurePath(Path(cls.get(ROOT_PATH)).expanduser())
        Path(root_path).mkdir(parents=True, exist_ok=True)
        return root_path
    
    @classmethod
    def get_root_podcast_path(cls) -> PurePath:
        if cls.get(ROOT_PODCAST_PATH) == '':
            root_podcast_path = PurePath(Path.home() / 'Music/Zotify Podcasts/')
        else:
            root_podcast_path:str = cls.get(ROOT_PODCAST_PATH)
            if root_podcast_path[0] == ".":
                root_podcast_path = cls.get_root_path() / PurePath(root_podcast_path).relative_to(".")
            root_podcast_path = PurePath(Path(root_podcast_path).expanduser())
        return root_podcast_path
    
    @classmethod
    def get_save_credentials(cls) -> bool:
        return cls.get(SAVE_CREDENTIALS)
    
    @classmethod
    def get_credentials_location(cls) -> PurePath:
        cred_str: str = cls.get(CREDENTIALS_LOCATION)
        if not cred_str:
            system_paths = {
                'win32': Path.home() / 'AppData/Roaming/Zotify',
                'linux': Path.home() / '.local/share/zotify',
                'darwin': Path.home() / 'Library/Application Support/Zotify'
            }
            cred_dir_or_file = system_paths.get(sys.platform, Path.cwd() / '.zotify')
        elif cred_str[0] == ".":
            cred_dir_or_file = Path(cls.get_root_path()) / Path(cred_str).expanduser().relative_to(".")
        else:
            cred_dir_or_file = Path(cred_str).expanduser()
        credentials = cred_dir_or_file if cred_dir_or_file.suffix else cred_dir_or_file / 'credentials.json'
        credentials.parent.mkdir(parents=True, exist_ok=True)
        return PurePath(credentials)
    
    @classmethod
    def get_output(cls, dl_obj_clsn: str) -> str:
        v = cls.get(OUTPUT)
        if v:
            # User must include {disc_number} in OUTPUT if they want split album discs
            return v
        
        if dl_obj_clsn == 'Query':
            v = cls.get(OUTPUT_SINGLE)
        elif dl_obj_clsn == 'Album':
            v = cls.get(OUTPUT_ALBUM)
        elif dl_obj_clsn == 'Playlist':
            v = cls.get(OUTPUT_PLAYLIST_EXT)
        elif dl_obj_clsn == 'Liked Song':
            v = cls.get(OUTPUT_LIKED_SONGS)
        else:
            raise ValueError(f'INVALID DOWNLOAD OBJECT CLASS "{dl_obj_clsn}"')
        
        if cls.get_split_album_discs() and dl_obj_clsn == "Album":
            return str(PurePath(v).parent / 'Disc {disc_number}' / PurePath(v).name)
        return v
    
    @classmethod
    def get_skip_existing(cls) -> bool:
        return cls.get(SKIP_EXISTING)
    
    @classmethod
    def get_skip_previously_downloaded(cls) -> bool:
        return cls.get(SKIP_PREVIOUSLY_DOWNLOADED)
    
    @classmethod
    def get_split_album_discs(cls) -> bool:
        return cls.get(SPLIT_ALBUM_DISCS)
    
    @classmethod
    def get_chunk_size(cls) -> int:
        return cls.get(CHUNK_SIZE)
    
    @classmethod
    def get_download_format(cls) -> str:
        return cls.get(DOWNLOAD_FORMAT)
    
    @classmethod
    def get_bulk_wait_time(cls) -> int:
        return cls.get(BULK_WAIT_TIME)
    
    @classmethod
    def get_language(cls) -> str:
        return cls.get(LANGUAGE)
    
    @classmethod
    def get_download_real_time(cls) -> bool:
        return cls.get(DOWNLOAD_REAL_TIME)
    
    @classmethod
    def get_download_qual_pref(cls) -> str:
        return cls.get(DOWNLOAD_QUALITY)
    
    @classmethod
    def get_transcode_bitrate(cls) -> str:
        return cls.get(TRANSCODE_BITRATE)
    
    @classmethod
    def get_song_archive_location(cls) -> PurePath:
        song_archive_str: str = cls.get(SONG_ARCHIVE_LOCATION)
        if not song_archive_str:
            system_paths = {
                'win32': Path.home() / 'AppData/Roaming/Zotify',
                'linux': Path.home() / '.local/share/zotify',
                'darwin': Path.home() / 'Library/Application Support/Zotify'
            }
            song_archive_dir = system_paths.get(sys.platform, Path.cwd() / '.zotify')
        elif song_archive_str[0] == ".":
            song_archive_dir = Path(cls.get_root_path()) / Path(song_archive_str).expanduser().relative_to(".")
        else:
            song_archive_dir = Path(song_archive_str).expanduser()
        song_archive_dir.mkdir(parents=True, exist_ok=True)
        return PurePath(song_archive_dir / '.song_archive')
    
    @classmethod
    def get_temp_download_dir(cls) -> str | PurePath:
        if cls.get(TEMP_DOWNLOAD_DIR) == '':
            return ''
        temp_download_path: str = cls.get(TEMP_DOWNLOAD_DIR)
        if temp_download_path[0] == ".":
            temp_download_path = cls.get_root_path() / PurePath(temp_download_path).relative_to(".")
        return PurePath(Path(temp_download_path).expanduser())
    
    @classmethod
    def get_disc_track_totals(cls) -> bool:
        return cls.get(MD_DISC_TRACK_TOTALS)
    
    @classmethod
    def get_save_genres(cls) -> bool:
        return cls.get(MD_SAVE_GENRES)
    
    @classmethod
    def get_all_genres(cls) -> bool:
        return cls.get(MD_ALLGENRES)
    
    @classmethod
    def get_genre_delimiter(cls) -> str:
        return cls.get(MD_GENREDELIMITER)
    
    @classmethod
    def get_artist_delimiter(cls) -> str:
        return cls.get(MD_ARTISTDELIMITER)
    
    @classmethod
    def get_retry_attempts(cls) -> int:
        return cls.get(RETRY_ATTEMPTS)
    
    @classmethod
    def get_disable_dir_archives(cls) -> bool:
        return cls.get(DISABLE_DIRECTORY_ARCHIVES)
    
    @classmethod
    def get_disable_song_archive(cls) -> bool:
        return cls.get(DISABLE_SONG_ARCHIVE)
    
    @classmethod
    def get_ffmpeg_log_level(cls) -> str:
        level = str(cls.get(FFMPEG_LOG_LEVEL)).lower()
        # see https://ffmpeg.org/ffmpeg.html#Generic-options, -loglevel
        valid_levels = {"trace", "debug", "verbose", "info", "warning", "error", "fatal", "panic", "quiet"}
        
        if level == "warn": level += "ing"
        if level not in valid_levels:
            raise ValueError(f'FFMPEG LOGGING LEVEL "{level}" NOT VALID\n' +
                             f'SELECT FROM: {valid_levels}')
        return level
    
    @classmethod
    def get_standard_interface(cls) -> bool:
        return cls.get(STANDARD_INTERFACE)
    
    @classmethod
    def get_show_any_progress(cls) -> bool:
        if cls.get_standard_interface():
            return False
        return cls.get(PRINT_DOWNLOAD_PROGRESS) or cls.get(PRINT_URL_PROGRESS) \
           or cls.get(PRINT_ALBUM_PROGRESS) or cls.get(PRINT_ARTIST_PROGRESS) \
        or cls.get(PRINT_PLAYLIST_PROGRESS)
    
    @classmethod
    def get_show_download_pbar(cls) -> bool:
        return cls.get_show_any_progress() and cls.get(PRINT_DOWNLOAD_PROGRESS)
    
    @classmethod
    def get_show_url_pbar(cls) -> bool:
        return cls.get_show_any_progress() and cls.get(PRINT_URL_PROGRESS)
    
    @classmethod
    def get_show_album_pbar(cls) -> bool:
        return cls.get_show_any_progress() and cls.get(PRINT_ALBUM_PROGRESS)
    
    @classmethod
    def get_show_artist_pbar(cls) -> bool:
        return cls.get_show_any_progress() and cls.get(PRINT_ARTIST_PROGRESS)
    
    @classmethod
    def get_show_playlist_pbar(cls) -> bool:
        return cls.get_show_any_progress() and cls.get(PRINT_PLAYLIST_PROGRESS)
    
    @classmethod
    def get_export_m3u8(cls) -> bool:
        return cls.get(EXPORT_M3U8)
    
    @classmethod
    def get_liked_songs_archive_m3u8(cls) -> bool:
        return cls.get(LIKED_SONGS_ARCHIVE_M3U8)
    
    @classmethod
    def get_album_art_jpg_file(cls) -> bool:
        return cls.get(ALBUM_ART_JPG_FILE)
    
    @classmethod
    def get_max_filename_length(cls) -> int:
        return cls.get(MAX_FILENAME_LENGTH)
    
    @classmethod
    def get_m3u8_location(cls) -> PurePath | None:
        if cls.get(M3U8_LOCATION) == '':
            # Use OUTPUT path as default location
            return None
        else:
            m3u8_path = cls.get(M3U8_LOCATION)
            if m3u8_path[0] == ".":
                m3u8_path = cls.get_root_path() / PurePath(m3u8_path).relative_to(".")
            m3u8_path = PurePath(Path(m3u8_path).expanduser())
        
        return m3u8_path
    
    @classmethod
    def get_m3u8_relative_paths(cls) -> bool:
        return cls.get(M3U8_REL_PATHS)
    
    @classmethod
    def get_oauth_address(cls) -> tuple[str, str]:
        redirect_address = cls.get(REDIRECT_ADDRESS)
        if redirect_address:
            return redirect_address
        return '127.0.0.1'
    
    @classmethod
    def get_download_parent_album(cls) -> bool:
        return cls.get(DOWNLOAD_PARENT_ALBUM)
    
    @classmethod
    def get_skip_comp_albums(cls) -> bool:
        return cls.get(NO_COMPILATION_ALBUMS)
    
    @classmethod
    def get_skip_various_artists(cls) -> bool:
        return cls.get(NO_VARIOUS_ARTISTS)
    
    @classmethod
    def get_skip_appears_on_album(cls) -> bool:
        return cls.get(NO_ARTIST_APPEARS_ON)
    
    @classmethod
    def get_discog_by_album_artist(cls) -> bool:
        return cls.get(DISCOG_BY_ALBUM_ARTIST)
    
    @classmethod
    def get_regex_enabled(cls) -> bool:
        return cls.get(REGEX_ENABLED)
    
    @classmethod
    def get_regex_album(cls) -> None | re.Pattern:
        if not (cls.get_regex_enabled() and cls.get(REGEX_ALBUM_SKIP)):
            return None
        return re.compile(cls.get(REGEX_ALBUM_SKIP), re.I)
    
    @classmethod
    def get_regex_track(cls) -> None | re.Pattern:
        if not (cls.get_regex_enabled() and cls.get(REGEX_TRACK_SKIP)):
            return None
        return re.compile(cls.get(REGEX_TRACK_SKIP), re.I)
 
    @classmethod
    def get_regex_episode(cls) -> None | re.Pattern:
        if not (cls.get_regex_enabled() and cls.get(REGEX_EPISODE_SKIP)):
            return None
        return re.compile(cls.get(REGEX_EPISODE_SKIP), re.I)
    
    @classmethod
    def get_strict_library_verify(cls) -> bool:
        return cls.get(STRICT_LIBRARY_VERIFY)
    
    @classmethod
    def get_optimized_dl(cls) -> bool:
        return cls.get(OPTIMIZED_DOWNLOADING)
    
    @classmethod
    def get_upgrade_legacy_archive(cls) -> bool:
        return cls.get(UPDATE_ARCHIVE)
    
    @classmethod
    def set_stop_upgrade_legacy_archive(cls) -> None:
        cls.Values[UPDATE_ARCHIVE] = False
    
    @classmethod
    def get_search_query_size(cls) -> str:
        size = cls.get(SEARCH_QUERY_SIZE)
        return size if size else "10"
    
    @classmethod
    def get_custom_ffmpeg_args(cls) -> list[str]:
        argstr: str = cls.get(CUSTOM_FFMEPG_ARGS)
        ffmpeg_args = argstr.split()
        return ffmpeg_args
    
    @classmethod
    def get_lyrics_to_file(cls) -> bool:
        return cls.get(LYRICS_TO_FILE)
    
    @classmethod
    def get_lyrics_to_metadata(cls) -> bool:
        return cls.get(LYRICS_TO_METADATA)
    
    @classmethod
    def get_lyrics_location(cls) -> PurePath | None:
        if cls.get(LYRICS_LOCATION) == '':
            # Use OUTPUT path as default location
            return None
        else:
            lyrics_path = cls.get(LYRICS_LOCATION)
            if lyrics_path[0] == ".":
                lyrics_path = cls.get_root_path() / PurePath(lyrics_path).relative_to(".")
            lyrics_path = PurePath(Path(lyrics_path).expanduser())
        
        return lyrics_path
    
    @classmethod
    def get_lyrics_filename(cls) -> str:
        return cls.get(OUTPUT_LYRICS)
    
    @classmethod
    def get_always_check_lyrics(cls) -> bool:
        return cls.get(ALWAYS_CHECK_LYRICS)
    
    @classmethod
    def get_lyrics_header(cls) -> bool:
        return cls.get(LYRICS_MD_HEADER)
    
    @classmethod
    def get_api_client_id(cls) -> str:
        return cls.get(API_CLIENT_ID)


class Zotify:
    CONFIG              : Config                    = Config()
    OAUTH               : OAuth                     = None
    SESSION             : Session                   = None
    TOTAL_API_CALLS     : int                       = None
    DATETIME_LAUNCH     : str                       = None
    LOGGER              : logging.Logger            = None
    LOGFILE             : Path                      = None
    DOWNLOAD_QUALITY    : FormatOnlyAudioQuality    = None
    DOWNLOAD_BITRATE    : str                       = None
    
    @classmethod
    def start(cls) -> None:
        if cls.TOTAL_API_CALLS:
            Printer.debug(f"Total API Calls: {cls.TOTAL_API_CALLS}")
        cls.DATETIME_LAUNCH = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        cls.TOTAL_API_CALLS = 0
    
    @classmethod
    def login(cls, args):
        """ Authenticates and saves credentials to a file """
        
        session_builder = Session.Builder() # stored_credentials == True by default
        session_builder.conf.store_credentials = False
        
        # login via saved credentials
        cred_path = cls.CONFIG.get_credentials_location()
        if cls.CONFIG.get_save_credentials() and Path(cred_path).exists():
            with open(cred_path, 'r',) as f:
                creds = json.load(f)
            try:
                if creds["type"] == OAuth.OAUTH_PKCE_TOKEN:
                    cls.CONFIG.Values[API_CLIENT_ID] = creds["client_id"]
                    cls.OAUTH = OAuth(cls.CONFIG.get_api_client_id(), "", None).ingest_token_response(creds)
                    cls.OAUTH.refresh_token()
                    cls.OAUTH.save_creds(cred_path)
                    session_builder.login_credentials = cls.OAUTH.get_credentials()
                    cls.SESSION = session_builder.create()
                    return
                else:
                    cls.CONFIG.Values[API_CLIENT_ID] = ""
                    session_builder.stored_file(cred_path)
                    cls.SESSION = session_builder.create()
                    return
            except RuntimeError:
                Printer.hashtaged(PrintChannel.MANDATORY, f'Login via saved {creds["type"]} credentials failed! Falling back to interactive login')
                # Path(cred_path).unlink()
        
        # login via commandline args (login5 token only)
        if args.username not in {None, ""} and args.token not in {None, ""}:
            try:
                auth_obj = {"username": args.username,
                            "credentials": args.token,
                            "type": AuthenticationType.keys()[1]}
                auth_as_bytes = b64encode(json.dumps(auth_obj, ensure_ascii=True).encode("ascii"))
                cls.SESSION = session_builder.stored(auth_as_bytes).create()
                return
            except:
                Printer.hashtaged(PrintChannel.MANDATORY, f"Login via commandline args failed! Falling back to interactive login")
        
        # interactive OAuth login
        port = 4381
        redirect_url = f"http://{cls.CONFIG.get_oauth_address()}:{port}/login"
        def oauth_print(url):
            Printer.new_print(PrintChannel.MANDATORY, f"Click on the following link to login:\n{url}")
        
        client_id = cls.CONFIG.get_api_client_id()
        if not client_id: client_id = MercuryRequests.keymaster_client_id
        oauth = OAuth(client_id, redirect_url, oauth_print).set_scopes(SCOPES).set_listen_all(True)
        session_builder.login_credentials = oauth.flow()
        if cls.CONFIG.get_save_credentials():
            if client_id != MercuryRequests.keymaster_client_id:
                cls.OAUTH = oauth
                oauth.save_creds(cred_path)
            else:
                session_builder.conf.store_credentials = True
                session_builder.conf.stored_credentials_file = str(cred_path)
        cls.SESSION = session_builder.create()
        return
    
    @classmethod
    def parse_dl_quality(cls, preference: str | None = None) -> tuple[FormatOnlyAudioQuality, str | None]:
        
        def format_filter(quality: AudioQuality) -> FormatOnlyAudioQuality:
           codec = SuperAudioFormat.FLAC if quality is AudioQuality.LOSSLESS else SuperAudioFormat.VORBIS
           return FormatOnlyAudioQuality(quality, codec)
        
        prem: bool = cls.SESSION.get_user_attribute(TYPE) == PREMIUM
        if preference is not None:
            Printer.debug(f"User Subscription Type: {'PREMIUM' if prem else 'FREE'}")
        
        quality_options: dict[str, tuple[AudioQuality, str | None]] = {
        'lossless':  (AudioQuality.LOSSLESS,     None ), # upstream API does not yet support lossless, will fallback to auto 
        'very_high': (AudioQuality.VERY_HIGH,   '320k'),
        'auto':      (AudioQuality.VERY_HIGH,   '320k') if prem else (AudioQuality.HIGH, '160k'),
        'high':      (AudioQuality.HIGH,        '160k'),
        'normal':    (AudioQuality.NORMAL,      '96k' ),
        }
        
        if preference is None:
            quality, bitrate = quality_options["auto"]
            return format_filter(quality), bitrate
        
        pref = quality_options.get(preference, quality_options["auto"])
        quality, bitrate = quality_options["high"] if (pref[-1] is None or int(pref[-1][:-1]) > 160) and not prem else pref
        return format_filter(quality), bitrate
    
    @classmethod
    def boot(cls, args):
        Printer.splash()
        cls.start()
        cls.CONFIG.load(args)
        
        # Handle sub-library logging
        cls.LOGFILE = Path(cls.CONFIG.get_root_path() / 
                         ("zotify_" + ("DEBUG_" if cls.CONFIG.debug() else "") + f"{cls.DATETIME_LAUNCH}.log"))
        Printer.hashtaged(PrintChannel.DEBUG, f"{cls.LOGFILE.name} logging to {cls.LOGFILE.resolve().parent}")
        logging.basicConfig(level=logging.DEBUG if cls.CONFIG.debug() else logging.CRITICAL,
                            filemode="x", filename=cls.LOGFILE)
        
        with Loader("Logging in...", PrintChannel.MANDATORY):
            login_try = 0
            while login_try <= cls.CONFIG.get_retry_attempts():
                login_try += 1
                try: cls.login(args)
                except ConnectionError as e:
                    Printer.hashtaged(PrintChannel.WARNING, f'LOGIN FAILED ({e.args[0]})\n' + 
                                                             'TRYING AGAIN AFTER SMALL WAIT')
                    sleep(3)
        cls.LOGGER = logging.getLogger("zotify.debug")
        
        Printer.debug(f'{"CLIENT_ID" if cls.OAUTH else ""} Session Initialized Successfully')
        quality, bitrate = cls.parse_dl_quality(cls.CONFIG.get_download_qual_pref())
        cls.DOWNLOAD_QUALITY = quality
        cls.DOWNLOAD_BITRATE = bitrate
    
    @staticmethod
    def id_from_gid(gid: str) -> str:
        return metadata.Id.b62.encode(b64decode(gid.encode())).decode()
    
    @staticmethod
    def to_libre_content(ContClass: type, uri: str) -> metadata.Id | None:
        try:
            libre_content_type: metadata.Id = getattr(metadata, ContClass.clsn + "Id")
            return libre_content_type.from_base62(uri.split(":")[-1])
        except:
            return
    
    @classmethod
    def invoke_libre_md(cls, ContClass: type, uri: str) -> dict:
        try:
            content_id = cls.to_libre_content(ContClass, uri)
            if ContClass.clsn == "Playlist":
                proto = cls.SESSION.api().get_playlist(content_id)
            else:
                proto = getattr(cls.SESSION.api(), f"get_metadata_4_{ContClass.type_attr}")(content_id)
            resp = MessageToDict(proto, preserving_proto_field_name=True)
            if resp.get(GID): resp[GID] = proto.gid # use gid in bytes
            return resp
        except Exception as e:
            Printer.debug(f"Failed to fetch metadata for {uri}")
            Printer.traceback(e)
            return {}
    
    @classmethod
    def invoke_url(cls, url: str, params: dict | None = None, expectFail: bool = False, force_login5: bool = False) -> dict:
        def choose_token() -> str:
            if cls.OAUTH and not force_login5:
                return cls.OAUTH.token()
            return cls.SESSION.tokens().get_token(*SCOPES).access_token
        
        headers = {
            'Authorization': f'Bearer {choose_token()}',
            'Accept-Language': f'{cls.CONFIG.get_language()}',
            'Accept': 'application/json',
            'app-platform': 'WebPlayer',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0'
        }
        
        tryCount = 0
        while tryCount <= cls.CONFIG.get_retry_attempts():
            resp = requests.get(url, headers=headers, params=params)
            cls.TOTAL_API_CALLS += 1
            if resp.status_code == 403 and not expectFail:
                Printer.hashtaged(PrintChannel.WARNING, f'API ERROR\n' +
                                                        f'ATTEMPTING TO ACCESS FORBIDDEN ENDPOINT')
                return {}
            try:
                responsejson = resp.json()
                if not responsejson:
                    raise json.decoder.JSONDecodeError()
            except json.decoder.JSONDecodeError:
                responsejson = {ERROR: {STATUS: "Unknown", MESSAGE: "Received an empty response"}}
            if resp.ok and resp.status_code == 200 and not responsejson.get(ERROR):
                return responsejson
            elif not expectFail:
                Printer.hashtaged(PrintChannel.WARNING, f'API ERROR (TRY {tryCount}) - RETRYING\n' +
                                                        f'{STATUS} {responsejson.get(ERROR, {}).get(STATUS, "Unknown")}:  '+
                                                        f'{responsejson.get(ERROR, {}).get(MESSAGE, "No message provided")}')
            
            tryCount += 1
            if tryCount > cls.CONFIG.get_retry_attempts():
                break
            sleep(5 if not expectFail else 1)
        
        if not expectFail:
            Printer.hashtaged(PrintChannel.API_ERROR, f'RETRY LIMIT EXCEDED\n' +
                                                      f'RESPONSE TEXT: {Printer.pretty(responsejson)}\n' +
                                                      f'URL: {Printer.pretty(url)}')
        
        return {}
    
    @classmethod
    def invoke_url_nextable(cls, url: str, response_key: str = ITEMS, stop: int = 0, limit: int = 50, offset: int = 0,
                            stripper: str | tuple[str] | None = None, params: dict | None = None) -> list[dict] | dict[list[dict]]:
        p = {LIMIT: limit, OFFSET: offset}
        if params: p.update(params)
        resp = cls.invoke_url(url, p)
        
        items: dict[str | None, list[dict]] = dict()
        strippers = stripper if isinstance(stripper, tuple) else (stripper,)
        for strip in strippers:
            nextable: dict = resp.get(strip, resp)
            
            if response_key not in nextable:
                Printer.hashtaged(PrintChannel.WARNING, f'KEY "{response_key}" NOT FOUND IN API RESPONSE: {nextable}')
                return []
            
            items[strip] = nextable[response_key]
            while nextable.get(NEXT) is not None and not (stop and len(items[strip]) >= stop):
                nextable = cls.invoke_url(nextable[NEXT])
                if len(strippers) > 1: nextable: dict = nextable.get(strip, nextable)
                if response_key not in nextable:
                    Printer.hashtaged(PrintChannel.WARNING, f'KEY "{response_key}" NOT FOUND IN PAGINATED API RESPONSE: {nextable}')
                    break
                items[strip].extend(nextable[response_key])
            if stop:
                items[strip] = items[strip][:stop]
        
        return items[strip] if len(strippers) == 1 and not isinstance(stripper, tuple) else items
    
    @classmethod
    def invoke_url_bulk(cls, url: str, bulk_items: list[str], stripper: str, limit: int = 50) -> list[dict]:
        items = []
        while len(bulk_items):
            items_batch = '%2c'.join(bulk_items[:limit])
            bulk_items = bulk_items[limit:]
            
            resp = cls.invoke_url(url + items_batch)
            if not resp: # assume 403 forbidden, warning handled in invoke_url
                return items
            elif not resp.get(stripper):
                Printer.hashtaged(PrintChannel.WARNING, f'STRIPPER "{stripper}" NOT FOUND IN API RESPONSE FOR BULK URL: {url}')
                continue
            items.extend(resp[stripper])
        return items
    
    @classmethod
    def get_content_stream(cls, content, use_qual_pref: bool = True) -> GeneralAudioStream | None:
        global FORCE_STREAM_API_CALL
        from zotify.api import DLContent
        if not isinstance(content, DLContent): return
        content_id = cls.to_libre_content(content.__class__, content.uri)
        if not content_id: return
        qual = cls.DOWNLOAD_QUALITY if use_qual_pref else cls.parse_dl_quality()[0]
        Printer.logger(f'Fetching stream for {content.uri} at quality {qual.preferred.name}')
        try:
            if not content.file_ids or FORCE_STREAM_API_CALL:
                risky_method = False
                lds = cls.SESSION.content_feeder().load(content_id, qual, False, None)
                return lds.input_stream if lds else None
            risky_method = True
            if getattr(content, "external_url", None):
                url = cls.SESSION.client().head(content.external_url).url
                return cls.SESSION.cdn().stream_external_episode(content, url, None)
            file = qual.get_file([ParseDict(f, AudioFile()) for f in content.file_ids])
            key = cls.SESSION.audio_key().get_audio_key(content.gid, file.file_id)
            url = cls.SESSION.content_feeder().resolve_storage_interactive(file.file_id, False)
            return cls.SESSION.cdn().stream_file(file, key, CdnFeedHelper.get_url(url), None)
        except FeederException as e:
            if not use_qual_pref:
                Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO FILE\n' +
                                                      'FALLBACK (AUTO) AUDIO QUALITY NOT AVAILABLE')
                return
            preference = cls.DOWNLOAD_QUALITY.preferred.name
            Printer.hashtaged(PrintChannel.WARNING, 'FAILED TO FETCH AUDIO FILE\n' +
                                                   f'PREFERED AUDIO QUALITY {preference} NOT AVAILABLE - FALLING BACK TO AUTO')
            return cls.get_content_stream(content, use_qual_pref=False)
        except RuntimeError as e:
            if 'Failed fetching audio key!' not in e.args[0]: raise
            gid, fileid = e.args[0].split('! ')[1].split(', ')
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO KEY\n' +
                                                  'MAY BE CAUSED BY RATE LIMITS - CONSIDER INCREASING `BULK_WAIT_TIME`\n' +
                                                 f'GID: {gid[5:]} - File_ID: {fileid[8:]}')
            Printer.logger("\n".join(e.args), PrintChannel.ERROR)
        except ConnectionError as e:
            if "Status code " not in e.args[0]: raise
            status_code = e.args[0].split("Status code ")[1]
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO FILE\n' +
                                                 f'CONNECTION ERROR WHEN FETCHING CONTENT STREAM - STATUS CODE {status_code}')
            Printer.logger("\n".join(e.args), PrintChannel.ERROR)
        except Exception as e:
            if risky_method:
                FORCE_STREAM_API_CALL = True
                return cls.get_content_stream(content, use_qual_pref=use_qual_pref)
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO STREAM\n' +
                                                  'AN UNEXPECTED ERROR OCCURED - CHECK LOGS FOR DETAILS')
            Printer.traceback(e)
        return None
    
    @classmethod
    def end(cls) -> None:
        cls.start()
        logging.shutdown()
        
        # delete non-debug logfiles if empty (no critical errors)
        if cls.LOGFILE.exists():
            with open(cls.LOGFILE) as file:
                lines = file.readlines()
            if not lines:
                cls.LOGFILE.unlink()
        
        for dir in (Path(cls.CONFIG.get_root_path()), Path(cls.CONFIG.get_root_podcast_path())):
            for tempfile in dir.glob("*.tmp"):
                    tempfile.unlink()
        
        print("\n")
