import json
import logging
import re
import sys
import requests
from binascii import hexlify
from base64 import b64encode, b64decode
from contextlib import contextmanager
from importlib.metadata import version
from google.protobuf.json_format import MessageToDict, ParseDict
from librespot import metadata
from librespot.audio import FeederException, CdnManager, CdnFeedHelper
from librespot.audio.decoders import AudioQuality, SuperAudioFormat, FormatOnlyAudioQuality
from librespot.core import Session, OAuth, MercuryRequests, ApiClient
from librespot.proto.Authentication_pb2 import AuthenticationType
from librespot.proto.Metadata_pb2 import AudioFile
from pathlib import Path, PurePath
from platform import system
from time import sleep
from typing import Any, Callable

from zotify.utils import ensure_is_file, file_has_content, safe_typecast, now
from zotify.termoutput import *

Streamer = CdnManager.Streamer


CONFIG_VALUES = {
    # Main Options
    ROOT_PATH:                  { DEFAULT: '~/Music/Zotify Music',    TYPE: str,    ARG: ('-rp', '--root-path'                     ,) },
    ROOT_PODCAST_PATH:          { DEFAULT: '~/Music/Zotify Podcasts', TYPE: str,    ARG: ('-rpp', '--root-podcast-path'            ,) },
    SAVE_CREDENTIALS:           { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--save-credentials'                     ,) },
    CREDENTIALS_LOCATION:       { DEFAULT: '',                        TYPE: str,    ARG: ('--creds', '--credentials-location'      ,) },
    
    # File Options
    OUTPUT:                     { DEFAULT: '',                        TYPE: str,    ARG: ('--output'                               ,) },
    OUTPUT_SINGLE:              { DEFAULT: '{artist}/{album}/{artist}_{song_name}',
                                  TYPE: str,
                                  ARG: ('-os', '--output-single' ,) },
    OUTPUT_ALBUM:               { DEFAULT: '{artist}/{album}/{album_num}_{artist}_{song_name}',
                                  TYPE: str,
                                  ARG: ('-oa', '--output-album' ,) },
    OUTPUT_PLAYLIST_EXT:        { DEFAULT: '{playlist}/{playlist_num}_{artist}_{song_name}',
                                  TYPE: str,  
                                  ARG: ('-oe', '--output-ext-playlist' ,) },
    OUTPUT_LIKED_SONGS:         { DEFAULT: 'Liked Songs/{artist}_{song_name}',
                                  TYPE: str,
                                  ARG: ('-ol', '--output-liked-songs' ,) },
    SPLIT_ALBUM_DISCS:          { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--split-album-discs'                       ,) },
    MAX_FILENAME_LENGTH:        { DEFAULT: '0',                       TYPE: int,    ARG: ('--max-filename-length'                     ,) },
    
    # Download Options
    OPTIMIZED_DOWNLOADING:      { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--optimized-downloading'                   ,) },
    DOWNLOAD_RATE_LIMITER:      { DEFAULT: '0.0',                     TYPE: float,  ARG: ('-dlr', '--download-rate-limiter'           ,) },
    BULK_WAIT_TIME:             { DEFAULT: '1.0',                     TYPE: float,  ARG: ('--bulk-wait-time'                          ,) },
    TEMP_DOWNLOAD_DIR:          { DEFAULT: '',                        TYPE: str,    ARG: ('-td', '--temp-download-dir'                ,) },
    
    # Album/Artist Options
    DOWNLOAD_PARENT_ALBUM:      { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--download-parent-album'                   ,) },
    NO_COMPILATION_ALBUMS:      { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--no-compilation-albums'                   ,) },
    NO_VARIOUS_ARTISTS:         { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--no-various-artists'                      ,) },
    NO_ARTIST_APPEARS_ON:       { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--no-artist-appears-on'                    ,) },
    DISCOG_BY_ALBUM_ARTIST:     { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--discog-by-album-artist'                  ,) },
    
    # Regex Options
    REGEX_ENABLED:              { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--regex-enabled'                           ,) },
    REGEX_TRACK_SKIP:           { DEFAULT: '',                        TYPE: str,    ARG: ('--regex-track-skip'                        ,) },
    REGEX_EPISODE_SKIP:         { DEFAULT: '',                        TYPE: str,    ARG: ('--regex-episode-skip'                      ,) },
    REGEX_ALBUM_SKIP:           { DEFAULT: '',                        TYPE: str,    ARG: ('--regex-album-skip'                        ,) },
    
    # Encoding Options
    DOWNLOAD_FORMAT:            { DEFAULT: 'copy',                    TYPE: str,    ARG: ('--codec', '--download-format'              ,) },
    DOWNLOAD_QUALITY:           { DEFAULT: 'auto',                    TYPE: str,    ARG: ('-q', '--download-quality'                  ,) },
    TRANSCODE_BITRATE:          { DEFAULT: 'auto',                    TYPE: str,    ARG: ('-b', '--bitrate', '--transcode-bitrate'    ,) },
    CUSTOM_FFMEPG_ARGS:         { DEFAULT: '',                        TYPE: str,    ARG: ('--custom-ffmpeg-args'                      ,) },
    
    # Archive Options
    SONG_ARCHIVE_LOCATION:      { DEFAULT: '',                        TYPE: str,    ARG: ('--song-archive-location'                   ,) },
    DISABLE_SONG_ARCHIVE:       { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--disable-song-archive'                    ,) },
    DISABLE_DIRECTORY_ARCHIVES: { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--disable-directory-archives'              ,) },
    SKIP_EXISTING:              { DEFAULT: 'True',                    TYPE: bool,   ARG: ('-ie', '--skip-existing'                    ,) },
    SKIP_PREVIOUSLY_DOWNLOADED: { DEFAULT: 'False',                   TYPE: bool,   ARG: ('-ip', '--skip-prev-downloaded', 
                                                                                                 '--skip-previously-downloaded'       ,) },
    SKIP_BY_ISRC:               { DEFAULT: 'False',                   TYPE: bool,   ARG: ('-ii', '--skip-by-isrc'                     ,) },
    
    # Playlist File Options
    EXPORT_M3U8:                { DEFAULT: 'False',                   TYPE: bool,   ARG: ('-e, --export-m3u8'                         ,) },
    M3U8_LOCATION:              { DEFAULT: '',                        TYPE: str,    ARG: ('--m3u8-location'                           ,) },
    OUTPUT_M3U8:                { DEFAULT: '{name}',                  TYPE: str,    ARG: ('-om', '--output-m3u8'                      ,) },
    M3U8_REL_PATHS:             { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--m3u8-relative-paths'                     ,) },
    LIKED_SONGS_ARCHIVE_M3U8:   { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--liked-songs-archive-m3u8'                ,) },
    
    # Lyrics Options
    LYRICS_TO_METADATA:         { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--lyrics-to-metadata'                      ,) },
    LYRICS_TO_FILE:             { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--lyrics-to-file'                          ,) },
    LYRICS_LOCATION:            { DEFAULT: '',                        TYPE: str,    ARG: ('--lyrics-location'                         ,) },
    OUTPUT_LYRICS:              { DEFAULT: '{artist}_{song_name}',    TYPE: str,    ARG: ('-oy', '--output-lyrics'                    ,) },
    ALWAYS_CHECK_LYRICS:        { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--always-check-lyrics'                     ,) },
    LYRICS_MD_HEADER:           { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--lyrics-md-header'                        ,) },
    
    # Metadata Options
    LANGUAGE:                   { DEFAULT: 'en',                      TYPE: str,    ARG: ('--language'                                ,) },
    MD_DISC_TRACK_TOTALS:       { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--md-disc-track-totals'                    ,) },
    MD_SAVE_GENRES:             { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--md-save-genres'                          ,) },
    MD_ALLGENRES:               { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--md-allgenres'                            ,) },
    MD_GENREDELIMITER:          { DEFAULT: ', ',                      TYPE: str,    ARG: ('--md-genredelimiter'                       ,) },
    MD_ARTISTDELIMITER:         { DEFAULT: ', ',                      TYPE: str,    ARG: ('--md-artistdelimiter'                      ,) },
    SEARCH_QUERY_SIZE:          { DEFAULT: '10',                      TYPE: int,    ARG: ('--search-query-size'                       ,) },
    STRICT_LIBRARY_VERIFY:      { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--strict-library-verify'                   ,) },
    ALBUM_ART_JPG_FILE:         { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--album-art-jpg-file'                      ,) },
    
    # ZMD Options
    IMPORT_ZMD:                 { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--import-zmd'                              ,) },
    IMPORT_ZMD_LOCATION:        { DEFAULT: './.zmd',                  TYPE: str,    ARG: ('--import-zmd-location'                     ,) },
    EXPORT_ZMD:                 { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--export-zmd'                              ,) },
    EXPORT_ZMD_LOCATION:        { DEFAULT: './.zmd',                  TYPE: str,    ARG: ('--export-zmd-location'                     ,) },
    
    # API Options
    API_CLIENT_ID:              { DEFAULT: '',                        TYPE: str,    ARG: ('--client-id'                               ,) },
    API_CREDENTIALS_LOCATION:   { DEFAULT: '',                        TYPE: str,    ARG: ('--api-creds', '--api-credentials-location' ,) },
    API_CLIENT_LEGACY:          { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--client-legacy'                           ,) },
    FETCH_DELAY:                { DEFAULT: '0.0',                     TYPE: float,  ARG: ('--fetch-delay'                             ,) },
    RETRY_ATTEMPTS:             { DEFAULT: '1',                       TYPE: int,    ARG: ('--retry-attempts'                          ,) },
    RETRY_DELAY:                { DEFAULT: '5.0',                     TYPE: float,  ARG: ('--retry-delay'                             ,) },
    ESCALATING_DELAY:           { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--escalating-delay'                        ,) },
    CHUNK_SIZE:                 { DEFAULT: '20000',                   TYPE: int,    ARG: ('--chunk-size'                              ,) },
    REDIRECT_TIMEOUT:           { DEFAULT: '120.0',                   TYPE: float,  ARG: ('--redirect-timeout'                        ,) },
    REDIRECT_ADDRESS:           { DEFAULT: '127.0.0.1',               TYPE: str,    ARG: ('--redirect-address'                        ,) },
    REDIRECT_PORT:              { DEFAULT: '4381',                    TYPE: int,    ARG: ('--redirect-port'                           ,) },
    
    # Terminal & Logging Options
    PRINT_SPLASH:               { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--print-splash'                            ,) },
    PRINT_PROGRESS_INFO:        { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-progress-info'                     ,) },
    PRINT_SKIPS:                { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-skips'                             ,) },
    PRINT_DOWNLOADS:            { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-downloads'                         ,) },
    PRINT_DOWNLOAD_PROGRESS:    { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-download-progress'                 ,) },
    PRINT_URL_PROGRESS:         { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-url-progress'                      ,) },
    PRINT_ALBUM_PROGRESS:       { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-album-progress'                    ,) },
    PRINT_ARTIST_PROGRESS:      { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-artist-progress'                   ,) },
    PRINT_PLAYLIST_PROGRESS:    { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-playlist-progress'                 ,) },
    PRINT_WARNINGS:             { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-warnings'                          ,) },
    PRINT_ERRORS:               { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-errors'                            ,) },
    PRINT_API_ERRORS:           { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--print-api-errors'                        ,) },
    STANDARD_INTERFACE:         { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--standard-interface'                      ,) },
    FFMPEG_LOG_LEVEL:           { DEFAULT: 'error',                   TYPE: str,    ARG: ('--ffmpeg-log-level'                        ,) },
}     


DEPRECIATED_CONFIGS = {
    "SONG_ARCHIVE":             { DEFAULT: '',                        TYPE: str,    ARG: ('--song-archive'                         ,) },
    "OVERRIDE_AUTO_WAIT":       { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--override-auto-wait'                   ,) },
    "REDIRECT_URI":             { DEFAULT: '127.0.0.1:4381',          TYPE: str,    ARG: ('--redirect-uri'                         ,) },
    "OAUTH_ADDRESS":            { DEFAULT: '0.0.0.0',                 TYPE: str,    ARG: ('--oauth-address'                        ,) },
    "OUTPUT_PLAYLIST":          { DEFAULT: '{playlist}/{artist}_{song_name}',
                                  TYPE: str, 
                                  ARG: ('-op', '--output-playlist' ,) },
    "DOWNLOAD_LYRICS":          { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--download-lyrics'                      ,) },
    "LYRICS_FILENAME":          { DEFAULT: '{artist}_{song_name}',    TYPE: str,    ARG: ('--lyrics-filename'                      ,) },
    "MD_SAVE_LYRICS":           { DEFAULT: 'True',                    TYPE: bool,   ARG: ('--md-save-lyrics'                       ,) },
    "BYPASS_MD_API":            { DEFAULT: 'False',                   TYPE: bool,   ARG: ('--bypass-metadata-api'                  ,) },
    "DOWNLOAD_REAL_TIME":       { DEFAULT: 'False',                   TYPE: bool,   ARG: ('-rt', '--download-real-time'            ,) },
}


class Config:
    Values = {}
    
    @staticmethod
    def _default() -> dict[str, str]:
        return {k: v[DEFAULT] for k, v in CONFIG_VALUES.items()}
    
    @staticmethod
    def _default_path() -> Path:
        system_paths = {
            WINDOWS_SYSTEM  : Path.home() / 'AppData/Roaming/Zotify',
            LINUX_SYSTEM    : Path.home() / '.config/zotify',
            MACOS_SYSTEM    : Path.home() / 'Library/Application Support/Zotify'
        }
        return system_paths.get(system(), Path.cwd() / '.zotify')
    
    @classmethod
    def load(cls, args) -> None:
        config_str = args.config_location
        if not config_str:
            config_dir_or_file = cls._default_path()
        else:
            config_dir_or_file = Path(config_str).expanduser()
        config_path = ensure_is_file(config_dir_or_file, 'config.json')
        
        # Debug Check (guarantee at top of config)
        cmd_args: dict = vars(args)
        cls.Values[DEBUG] = safe_typecast(cmd_args, DEBUG.lower(), bool)
        
        # Load default values
        for cfg, cfg_setup in CONFIG_VALUES.items():
            cls.Values[cfg] = safe_typecast(cfg_setup, DEFAULT, cfg_setup[TYPE])
        
        # Load config from config.json
        if not file_has_content(config_path):
            with open(config_path, 'w', encoding='utf-8') as config_file:
                json.dump(cls._default(), config_file, indent=4)
            Printer.hashtaged(PrintChannel.MANDATORY, f"config.json saved to {config_path.resolve().parent}")
        else:
            with open(config_path, encoding='utf-8') as config_file:
                jsonvalues: dict[str, dict[str, Any]] = json.load(config_file)
            for cfg in jsonvalues:
                if cfg == DEBUG and not cls.Values[DEBUG]:
                    cls.Values[DEBUG] = safe_typecast(jsonvalues, cfg, bool)
                elif cfg in CONFIG_VALUES:
                    cls.Values[cfg] = safe_typecast(jsonvalues, cfg, CONFIG_VALUES[cfg][TYPE])
                elif cfg in DEPRECIATED_CONFIGS: # keep, warn, and place at the bottom (don't delete)
                    Printer.depreciated_warning(cfg, f'Delete the `"{cfg}": "{jsonvalues[cfg]}"` line from your config.json')
                    cls.Values["vvv___DEPRECIATED_BELOW_HERE___vvv"] = "vvv___REMOVE_THESE___vvv"
                    cls.Values[cfg] = safe_typecast(jsonvalues, cfg, DEPRECIATED_CONFIGS[cfg][TYPE])
        
        # Standardize and write config to file if debugging or refreshing 
        if cls.debug() or args.update_config:
            if cls.debug() and not config_path.name.endswith("_DEBUG.json"):
                config_path = config_path.with_stem(config_path.stem + "_DEBUG")
            config_path.touch()
            with open(config_path, 'w', encoding='utf-8') as debug_file:
                json.dump({k: str(v) for k, v in cls.Values.items()}, debug_file, indent=4)
            real_debug = cls.Values[DEBUG]; cls.Values[DEBUG] = True
            Printer.hashtaged(PrintChannel.DEBUG, f"{config_path.name} saved to {config_path.resolve().parent}")
            cls.Values[DEBUG] = real_debug
        
        # Override config from commandline arguments
        for cfg in CONFIG_VALUES:
            if cmd_args.get(cfg.lower()) is not None:
                cls.Values[cfg] = safe_typecast(cmd_args, cfg.lower(), CONFIG_VALUES[cfg][TYPE])
        
        if cls.get_regex_enabled():
            for mode in [TRACK, EPISODE, ALBUM]:
                regex_method: Callable[[None], None | re.Pattern] = getattr(cls, f"get_regex_{mode.lower()}")
                if regex_method(): 
                    Printer.hashtaged(PrintChannel.DEBUG, f'{mode.capitalize()} Regex Filter:  r"{regex_method().pattern}"')
        
        if cls.debug() or args.update_archive or args.verify_library:
            cls.Values[UPDATE_ARCHIVE] = True
        
        if args.test:
            cls.Values[TEST_MODE] = True
    
    @classmethod
    def get(cls, key: str) -> Any:
        return cls.Values.get(key)
    
    @classmethod
    @contextmanager
    def temporary_config(cls, cfg: str, temp_value):
        original_val = cls.get(cfg)
        cls.Values[cfg] = safe_typecast({cfg: temp_value}, cfg, CONFIG_VALUES[cfg][TYPE])
        try:
            yield
        finally:
            cls.Values[cfg] = original_val
    
    @classmethod
    def debug(cls) -> bool:
        return cls.get(DEBUG)
    
    @classmethod
    def test_mode(cls) -> bool:
        return cls.get(TEST_MODE)
    
    # Main Options
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
            cred_dir_or_file = cls._default_path()
        elif cred_str[0] == ".":
            cred_dir_or_file = Path(cls.get_root_path()) / Path(cred_str).expanduser().relative_to(".")
        else:
            cred_dir_or_file = Path(cred_str).expanduser()
        credentials = ensure_is_file(cred_dir_or_file, 'credentials.json', touch=False)
        return PurePath(credentials)
    
    # File Options
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
    def get_split_album_discs(cls) -> bool:
        return cls.get(SPLIT_ALBUM_DISCS)
    
    @classmethod
    def get_max_filename_length(cls) -> int:
        return cls.get(MAX_FILENAME_LENGTH)
    
    # Download Options
    @classmethod
    def get_optimized_dl(cls) -> bool:
        return cls.get(OPTIMIZED_DOWNLOADING)
    
    @classmethod
    def get_dl_rate_limter(cls) -> float:
        return cls.get(DOWNLOAD_RATE_LIMITER)
    
    @classmethod
    def get_bulk_wait_time(cls) -> float:
        return cls.get(BULK_WAIT_TIME)
    
    @classmethod
    def get_download_qual_pref(cls) -> str:
        return cls.get(DOWNLOAD_QUALITY)
    
    @classmethod
    def get_temp_download_dir(cls) -> str | PurePath:
        if cls.get(TEMP_DOWNLOAD_DIR) == '':
            return ''
        temp_download_path: str = cls.get(TEMP_DOWNLOAD_DIR)
        if temp_download_path[0] == ".":
            temp_download_path = cls.get_root_path() / PurePath(temp_download_path).relative_to(".")
        return PurePath(Path(temp_download_path).expanduser())
    
    # Album/Artist Options
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
    
    # Regex Options
    @classmethod
    def get_regex_enabled(cls) -> bool:
        return cls.get(REGEX_ENABLED)
    
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
    def get_regex_album(cls) -> None | re.Pattern:
        if not (cls.get_regex_enabled() and cls.get(REGEX_ALBUM_SKIP)):
            return None
        return re.compile(cls.get(REGEX_ALBUM_SKIP), re.I)
    
    # Encoding Options
    @classmethod
    def get_download_format(cls) -> str:
        return cls.get(DOWNLOAD_FORMAT)
    
    @classmethod
    def get_transcode_bitrate(cls) -> str:
        return cls.get(TRANSCODE_BITRATE)
    
    @classmethod
    def get_custom_ffmpeg_args(cls) -> list[str]:
        argstr: str = cls.get(CUSTOM_FFMEPG_ARGS)
        ffmpeg_args = argstr.split()
        return ffmpeg_args
    
    # Archive Options
    @classmethod
    def get_song_archive_location(cls) -> PurePath:
        archive_str: str = cls.get(SONG_ARCHIVE_LOCATION)
        if not archive_str:
            archive_dir_or_file = cls._default_path()
        elif archive_str[0] == ".":
            archive_dir_or_file = Path(cls.get_root_path()) / Path(archive_str).expanduser().relative_to(".")
        else:
            archive_dir_or_file = Path(archive_str).expanduser()
        archive_path = ensure_is_file(archive_dir_or_file, '.song_archive')
        return PurePath(archive_path)
    
    @classmethod
    def get_no_song_archive(cls) -> bool:
        return cls.get(DISABLE_SONG_ARCHIVE)
    
    @classmethod
    def get_no_dir_archives(cls) -> bool:
        return cls.get(DISABLE_DIRECTORY_ARCHIVES)
    
    @classmethod
    def get_skip_existing(cls) -> bool:
        return cls.get(SKIP_EXISTING)
    
    @classmethod
    def get_skip_previously_downloaded(cls) -> bool:
        return cls.get(SKIP_PREVIOUSLY_DOWNLOADED)
    
    @classmethod
    def get_skip_by_isrc(cls) -> bool:
        return cls.get(SKIP_BY_ISRC)
    
    @classmethod
    def get_update_archive(cls) -> bool:
        return cls.get(UPDATE_ARCHIVE)
    
    # Playlist File Options
    @classmethod
    def get_export_m3u8(cls) -> bool:
        return cls.get(EXPORT_M3U8)
    
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
    def get_m3u8_filename(cls) -> str:
        return cls.get(OUTPUT_M3U8)
    
    @classmethod
    def get_m3u8_relative_paths(cls) -> bool:
        return cls.get(M3U8_REL_PATHS)
    
    @classmethod
    def get_liked_songs_archive_m3u8(cls) -> bool:
        return cls.get(LIKED_SONGS_ARCHIVE_M3U8)
    
    # Lyrics Options
    @classmethod
    def get_lyrics_to_metadata(cls) -> bool:
        return cls.get(LYRICS_TO_METADATA)
    
    @classmethod
    def get_lyrics_to_file(cls) -> bool:
        return cls.get(LYRICS_TO_FILE)
    
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
    
    # Metadata Options
    @classmethod
    def get_language(cls) -> str:
        return cls.get(LANGUAGE)
    
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
    def get_search_query_size(cls) -> int:
        return cls.get(SEARCH_QUERY_SIZE)
    
    @classmethod
    def get_strict_library_verify(cls) -> bool:
        return cls.get(STRICT_LIBRARY_VERIFY)
    
    @classmethod
    def get_album_art_jpg_file(cls) -> bool:
        return cls.get(ALBUM_ART_JPG_FILE)
    
    # ZMD Options
    @classmethod
    def get_import_zmd(cls) -> bool:
        return cls.get(IMPORT_ZMD)
    
    @classmethod
    def get_zmd_import_location(cls) -> PurePath:
        if cls.get(IMPORT_ZMD_LOCATION) == '':
            zmd_path = cls.get_root_path() / ".zmd"
        else:
            zmd_path = cls.get(IMPORT_ZMD_LOCATION)
            if zmd_path[0] == ".":
                zmd_path = cls.get_root_path() / PurePath(zmd_path).relative_to(".")
            zmd_path = PurePath(Path(zmd_path).expanduser())
        return zmd_path
    
    @classmethod
    def get_export_zmd(cls) -> bool:
        return cls.get(EXPORT_ZMD)
    
    @classmethod
    def get_zmd_export_location(cls) -> PurePath:
        if cls.get(EXPORT_ZMD_LOCATION) == '':
            zmd_path = cls.get_root_path() / ".zmd"
            Path(zmd_path).touch()
        else:
            zmd_path = cls.get(EXPORT_ZMD_LOCATION)
            if zmd_path[0] == ".":
                zmd_path = cls.get_root_path() / PurePath(zmd_path).relative_to(".")
            zmd_path = PurePath(Path(zmd_path).expanduser())
        return zmd_path
    
    # API Options
    @classmethod
    def get_api_client_id(cls) -> str:
        return cls.get(API_CLIENT_ID)
    
    @classmethod
    def get_api_credentials_location(cls) -> PurePath:
        cred_str: str = cls.get(API_CREDENTIALS_LOCATION)
        if not cred_str:
            cred_dir_or_file = cls._default_path()
        elif cred_str[0] == ".":
            cred_dir_or_file = Path(cls.get_root_path()) / Path(cred_str).expanduser().relative_to(".")
        else:
            cred_dir_or_file = Path(cred_str).expanduser()
        credentials = ensure_is_file(cred_dir_or_file, 'api_credentials.json', touch=False)
        return PurePath(credentials)
    
    @classmethod
    def permit_client_api(cls) -> bool:
        return bool(cls.get_api_client_id() and not Zotify.FORCE_LIBRE_METADATA)
    
    @classmethod
    def permit_legacy_api(cls) -> bool:
        return bool(cls.permit_client_api() and cls.get(API_CLIENT_LEGACY) and Zotify.LEGACY_API_ENDOINTS)
    
    @classmethod
    def get_fetch_delay(cls) -> float:
        return max(cls.get(FETCH_DELAY), 0.0)
    
    @classmethod
    def get_retry_attempts(cls) -> int:
        return max(cls.get(RETRY_ATTEMPTS), 0)
    
    @classmethod
    def get_retry_delay(cls, retry_attempt_number: int = 0) -> float:
        base_delay = max(cls.get(RETRY_DELAY), 0.0)
        if cls.get_escalating_delay():
            base_delay *= 2 ** retry_attempt_number
        return base_delay
    
    @classmethod
    def get_escalating_delay(cls) -> bool:
        return cls.get(ESCALATING_DELAY)
    
    @classmethod
    def get_chunk_size(cls) -> int:
        return cls.get(CHUNK_SIZE)
    
    @classmethod
    def get_oauth_timeout(cls) -> float | None:
        timeout = cls.get(REDIRECT_TIMEOUT)
        return timeout if timeout > 0.0 else None
    
    @classmethod
    def get_oauth_address(cls) -> str:
        return cls.get(REDIRECT_ADDRESS)
    
    @classmethod
    def get_oauth_port(cls) -> int:
        return cls.get(REDIRECT_PORT)
    
    # Terminal & Logging Options
    @classmethod
    def get_show_any_progress(cls) -> bool:
        if cls.get_standard_interface():
            return False
        return cls.get(PRINT_DOWNLOAD_PROGRESS) or cls.get(PRINT_URL_PROGRESS) \
            or cls.get(PRINT_ALBUM_PROGRESS)    or cls.get(PRINT_ARTIST_PROGRESS) \
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
    def get_standard_interface(cls) -> bool:
        return cls.get(STANDARD_INTERFACE)
    
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


class LogHandler:
    LOG_PATH    : Path              = None
    LOGGER      : logging.Logger    = None
    
    @classmethod
    def start_logger(cls, launch: str) -> logging.Logger:
        logfile = "zotify_" + ("DEBUG_" if Zotify.CONFIG.debug() else "") + f"{launch}.log"
        cls.LOG_PATH = Path(Zotify.CONFIG.get_root_path() / logfile)
        Printer.hashtaged(PrintChannel.DEBUG, f"{logfile} logging to {cls.LOG_PATH.resolve().parent}")
        logging.basicConfig(level=logging.DEBUG if Zotify.CONFIG.debug() else logging.CRITICAL,
                            filemode="x", filename=cls.LOG_PATH)
        cls.LOGGER = logging.getLogger("zotify.debug")
        return cls.LOGGER
    
    @classmethod
    def kill_logger(cls) -> None:
        logging.shutdown()
        
        # delete non-debug logfiles if empty (no critical errors)
        if not file_has_content(cls.LOG_PATH):
            Path(cls.LOG_PATH).unlink()


class LoginHandler:
    CRED_TYPES          : set[str]          = {OAuth.OAUTH_PKCE_TOKEN, AuthenticationType.keys()[1]}
    SESSION_BUILDER     : Session.Builder   = Session.Builder()
    SESSION_BUILDER.conf.store_credentials  = False # stored_credentials == True by default
    SESSION             : Session           = None
    OAUTH               : OAuth             = None
    
    @classmethod
    def get_creds_from_file(cls, cred_path: PurePath) -> dict | None:
        if not Zotify.CONFIG.get_save_credentials() or not cred_path:
            return
        elif not file_has_content(cred_path):
            raise RuntimeError("Credentials file missing or empty")
        with open(cred_path, 'r',) as f:
            creds: dict = json.load(f)
        if not creds or not isinstance(creds, dict) or not creds.get("type"):
            raise RuntimeError("Invalid credentials file")
        elif creds["type"] not in cls.CRED_TYPES:
            raise RuntimeError(f"Invalid credentials file type: {creds['type']}")
        return creds
    
    @staticmethod
    def create_oauth(client_id: str) -> OAuth:
        redirect_url = f"http://{Zotify.CONFIG.get_oauth_address()}:{Zotify.CONFIG.get_oauth_port()}/login"
        def oauth_print(url):
            Printer.new_print(PrintChannel.MANDATORY, f"Click on the following link to login:\n{url}")
        
        timeout = Zotify.CONFIG.get_oauth_timeout()
        return OAuth(client_id, redirect_url, oauth_print).set_scopes(SCOPES).set_listen_all(True).set_timeout(timeout)
    
    @staticmethod
    def get_login5_from_args(args) -> dict | None:
        if args.username in {None, ""} or args.token in {None, ""}:
            return
        elif not str(args.username).isalnum():
            raise RuntimeError("Provided username invalid, not expected alphanumeric")
        elif not str(args.token) == b64encode(b64decode(args.token)).decode():
            raise RuntimeError("Provided token invalid, not expected base64")
        
        return {"username": str(args.username),
                "credentials": str(args.token),
                "type": AuthenticationType.keys()[1]}
    
    @classmethod
    def login5_cred_login(cls, login5_creds: dict | None) -> None:
        if not login5_creds: return
        b64creds = b64encode(json.dumps(login5_creds, ensure_ascii=True).encode("ascii"))
        cls.SESSION = cls.SESSION_BUILDER.stored(b64creds).create()
    
    @classmethod
    def oauth_cred_login(cls, oauth_creds: dict | None) -> None:
        if not oauth_creds: return
        cls.OAUTH = cls.create_oauth(oauth_creds["client_id"]).ingest_token_response(oauth_creds)
        cls.OAUTH.refresh_token()
    
    @classmethod
    def oauth_link_login(cls) -> None:
        cls.OAUTH = cls.create_oauth(Zotify.CONFIG.get_api_client_id())
        cls.OAUTH.flow()
    
    @classmethod
    def login5_link_login(cls) -> None:
        cls.SESSION_BUILDER.login_credentials = cls.create_oauth(MercuryRequests.keymaster_client_id).flow()
        cls.SESSION = cls.SESSION_BUILDER.create()
    
    @classmethod
    def attempt_login(cls, args) -> None:
        if not cls.SESSION:
            try: cls.login5_cred_login(cls.get_creds_from_file(Zotify.CONFIG.get_credentials_location()))
            except Exception as e:
                Printer.hashtaged(PrintChannel.MANDATORY, f'Login5 via saved credentials failed! {e.args[0]}. Falling back to interactive login')
        if not cls.SESSION:
            try: cls.login5_cred_login(cls.get_login5_from_args(args))
            except Exception as e:
                Printer.hashtaged(PrintChannel.MANDATORY, f'Login5 via commandline args failed! {e.args[0]}. Falling back to interactive login')
        if not cls.SESSION:
            try: cls.login5_link_login()
            except Exception as e:
                Printer.hashtaged(PrintChannel.MANDATORY, f'Login5 failed! {e.args[0]}')
        
        if not Zotify.CONFIG.permit_client_api(): return
        if not cls.OAUTH: 
            try: cls.oauth_cred_login(cls.get_creds_from_file(Zotify.CONFIG.get_api_credentials_location()))
            except Exception as e:
                Printer.hashtaged(PrintChannel.MANDATORY, f'Custom Client API via saved credentials failed! {e.args[0]}. Falling back to interactive login')
        if not cls.OAUTH: 
            try: cls.oauth_link_login()
            except Exception as e:
                Printer.hashtaged(PrintChannel.MANDATORY, f'Custom Client API failed! {e.args[0]}')
    
    @classmethod
    def login_success(cls) -> bool:
        return bool(cls.SESSION and (Zotify.CONFIG.permit_client_api() == bool(cls.OAUTH)))
    
    @classmethod
    def save_credentials(cls) -> None:
        if not Zotify.CONFIG.get_save_credentials():
            return
        if cls.SESSION:
            with open(Zotify.CONFIG.get_credentials_location(), "w") as f:
                json.dump(cls.SESSION.credentials(), f)
        if cls.OAUTH:
            cls.OAUTH.save_creds(Zotify.CONFIG.get_api_credentials_location())
    
    @classmethod
    def login(cls, args) -> Session | None:
        login_retry = 0
        while not cls.login_success():
            cls.attempt_login(args)
            if cls.login_success() or login_retry >= Zotify.CONFIG.get_retry_attempts():
                return cls.SESSION
            Printer.hashtaged(PrintChannel.WARNING, 'LOGIN FAILED, TRYING AGAIN AFTER DELAY')
            login_retry += 1; sleep(Zotify.CONFIG.get_retry_delay())
    
    @classmethod
    def choose_token(cls, force_login5: bool) -> str:
        if cls.OAUTH and not force_login5:
            return cls.OAUTH.token()
        return cls.SESSION.tokens().get_token(*SCOPES).access_token


class Zotify:
    # STATIC
    VERSION                                             = version("zotify")
    LEGACY_API_ENDOINTS     : bool                      = True
    FORCE_LIBRE_METADATA    : bool                      = False
    ALLOW_LIBRE_BULK        : bool                      = True
    
    # STATIC AFTER BOOT
    CONFIG                  : Config                    = Config
    SESSION                 : Session                   = None
    LOGGER                  : logging.Logger            = None
    DOWNLOAD_QUALITY        : FormatOnlyAudioQuality    = None
    DOWNLOAD_BITRATE        : str                       = None
    
    # DYNAMIC PER SESSION
    FORCE_STREAM_API_CALLS  : bool                      = False
    
    # DYNAMIC PER QUERY
    TOTAL_API_CALLS         : int                       = None
    DATETIME_LAUNCH         : str                       = None
    
    @classmethod
    def start_stats(cls) -> None:
        if cls.TOTAL_API_CALLS:
            Printer.debug(f"Total API Calls: {cls.TOTAL_API_CALLS}")
        cls.DATETIME_LAUNCH = now().replace(":", "-").replace(" ", "_")
        cls.TOTAL_API_CALLS = 0
    
    @classmethod
    def parse_dl_quality(cls, preference: str | None = None) -> tuple[bool, FormatOnlyAudioQuality, str | None]:
        prem: bool = cls.SESSION.get_user_attribute(TYPE) == PREMIUM
        quality_options: dict[str, tuple[AudioQuality, str | None]] = {
        'lossless':  (AudioQuality.LOSSLESS,     None ), # upstream API does not yet support lossless, will fallback to auto 
        'very_high': (AudioQuality.VERY_HIGH,   '320k'),
        'auto':      (AudioQuality.VERY_HIGH,   '320k') if prem else (AudioQuality.HIGH, '160k'),
        'high':      (AudioQuality.HIGH,        '160k'),
        'normal':    (AudioQuality.NORMAL,      '96k' ),
        }
        
        def format_filter(quality: AudioQuality) -> FormatOnlyAudioQuality:
           codec = SuperAudioFormat.FLAC if quality is AudioQuality.LOSSLESS else SuperAudioFormat.VORBIS
           return FormatOnlyAudioQuality(quality, codec)
        if preference is None:
            quality, bitrate = quality_options["auto"]
            return prem, format_filter(quality), bitrate
        
        pref = quality_options.get(preference, quality_options["auto"])
        quality, bitrate = quality_options["high"] if (pref[-1] is None or int(pref[-1][:-1]) > 160) and not prem else pref
        return prem, format_filter(quality), bitrate
    
    @classmethod
    def boot(cls, args) -> None:
        Printer.splash()
        cls.start_stats()
        cls.CONFIG.load(args)
        cls.LOGGER = LogHandler.start_logger(cls.DATETIME_LAUNCH)
        
        with Loader(LOGIN_STRING, PrintChannel.MANDATORY):
            cls.SESSION = LoginHandler.login(args)
            LoginHandler.save_credentials()
        if not cls.SESSION:
            Printer.hashtaged(PrintChannel.MANDATORY, 'ALL LOGIN ATTEMPTS UNSUCCESSFUL\n'+ 
                                                      'NO SESSION CREATED, EXITING PROGRAM')
            cls.end()
            sys.exit(1) # TODO implement full exit code scheme
        
        prem, quality, bitrate = cls.parse_dl_quality(cls.CONFIG.get_download_qual_pref())
        cls.DOWNLOAD_QUALITY = quality
        cls.DOWNLOAD_BITRATE = bitrate
        Printer.debug(f'Login5 Session Initialized Successfully\n' +
                      ('Custom Client API Initialized Successfully\n' if LoginHandler.OAUTH else '') +
                      f'User Subscription Type: {"PREMIUM" if prem else "FREE"}\n' +
                      f'Zotify Version v{cls.VERSION}')
    
    @staticmethod
    def id_from_gid(gid: str) -> str:
        return metadata.Id.b62.encode(b64decode(gid)).decode()
    
    @staticmethod
    def hex_id_from_file_id(file_id: str) -> str:
        return hexlify(b64decode(file_id)).decode()
    
    @staticmethod
    def to_libre_content(ContClass: type, id: str) -> metadata.Id | None:
        try:
            libre_content_type: metadata.Id = getattr(metadata, ContClass.clsn + "Id")
            return libre_content_type.from_base62(id)
        except:
            return
    
    @staticmethod
    def api_status_str(status_code: int, http: requests.Response = None) -> str:
        if   status_code == 200:        return "OK"
        elif status_code == 201:        return "Request fullfilled internally"
        elif status_code == 202:        return "Awaiting processing"
        elif status_code == 204:        return "No content"
        elif status_code == 304:        return "Use cached values"
        elif status_code == 400:        return "Malformed request" + (
                                              f": {http.json()[ERROR][MESSAGE]}" if 
                                                   http and http.json().get(ERROR, {}).get(MESSAGE) else "")
        elif status_code in {401, 403}: return "Unauthorized or forbidden request"
        elif status_code == 404:        return "Requested Content not Found"
        elif status_code == 429:        return "Too Many Requests, Rate Limit Exceeded" + (
                                              f". Timed out for {float(http.headers[RETRY_AFTER])} seconds." if
                                                                 http and http.headers.get(RETRY_AFTER) else "")
        elif status_code in {500, 502}: return "Internal/Upstream Server Error"
        elif status_code == 503:        return "Service Unavailable (Possibly a Rate Limit)"
        else:                           return ""
    
    @classmethod
    def invoke_libre_md(cls, ContClass: type, uri: str) -> dict[str, str | int | dict]:
        api_retry = 0
        while api_retry <= cls.CONFIG.get_retry_attempts():
            if api_retry:
                Printer.hashtaged(PrintChannel.WARNING, f'API ERROR {retry_text}- RETRYING\n' +
                                                        f'FAILED TO FETCH METADATA FOR {uri}'+
                                                        f'{fallback_message}')
                sleep(retry_delay)
            
            try:
                content_id = cls.to_libre_content(ContClass, uri.split(":")[-1])
                if ContClass.clsn == "Playlist":
                    proto = cls.SESSION.api().get_playlist(content_id)
                else:
                    proto = getattr(cls.SESSION.api(), f"get_metadata_4_{ContClass.type_attr}")(content_id)
                resp = MessageToDict(proto, preserving_proto_field_name=True)
                if resp.get(GID): resp[GID] = proto.gid # use gid in bytes
                break
            except ApiClient.StatusCodeException as e:
                fallback_message = f'Status {e.code}:   \n{cls.api_status_str(e.code)}'
            except ConnectionError as e:
                fallback_message = e.args[0]
            except Exception as e:
                fallback_message = f'UNKNOWN OR UNEXPECTED ERROR: {e}'
            finally: cls.TOTAL_API_CALLS += 1
            retry_text = f"(RETRY {api_retry}) " if api_retry else ""
            retry_delay = cls.CONFIG.get_retry_delay(api_retry)
            api_retry += 1
        
        sleep(cls.CONFIG.get_fetch_delay())
        if api_retry <= cls.CONFIG.get_retry_attempts():
            return resp
        Printer.hashtaged(PrintChannel.API_ERROR, f'RETRY LIMIT EXCEDED\n' +
                                                  f'FAILED TO FETCH METADATA FOR {uri}')
        return {}
    
    @classmethod
    def invoke_libre_bulk_md(cls, ContClass: type, uris: list[str]) -> list[dict[str, str | int | dict] | None] | None:
        def fetch_batch(batch: list[str]) -> list[dict | None] | None:
            api_retry = 0
            while api_retry <= cls.CONFIG.get_retry_attempts():
                if api_retry:
                    Printer.hashtaged(PrintChannel.WARNING, f'API ERROR (RETRY {api_retry}) - RETRYING\n' +
                                                            f'FAILED TO FETCH METADATA FOR {ContClass.uppers}\n' +
                                                            fallback_message)
                    sleep(cls.CONFIG.get_retry_delay(api_retry - 1))
                try:
                    content_ids = [cls.to_libre_content(ContClass, uri.split(":")[-1]) for uri in batch]
                    protos = cls.SESSION.api().get_metadata_4_multiple(content_ids)
                    resps = [MessageToDict(proto, preserving_proto_field_name=True) if proto else None
                             for proto in protos]
                    for proto, resp in zip(protos, resps):
                        if proto and resp and resp.get(GID): resp[GID] = proto.gid
                    break
                except ApiClient.StatusCodeException as e:
                    fallback_message = f'Status {e.code}:   \n{cls.api_status_str(e.code)}'
                    if e.code == 413:
                        if len(batch) == 1:
                            # The batch wrapper can be too large even for one item;
                            # try the provider's single-item endpoint once.
                            return [cls.invoke_libre_md(ContClass, batch[0]) or None]
                        midpoint = len(batch) // 2
                        return fetch_batch(batch[:midpoint]) + fetch_batch(batch[midpoint:])
                except ConnectionError as e:
                    fallback_message = str(e)
                except Exception as e:
                    fallback_message = f'UNKNOWN OR UNEXPECTED ERROR: {e}'
                finally:
                    cls.TOTAL_API_CALLS += 1
                retry_delay = cls.CONFIG.get_retry_delay(api_retry)
                api_retry += 1

            sleep(cls.CONFIG.get_fetch_delay())
            if api_retry <= cls.CONFIG.get_retry_attempts():
                # A partial batch can omit an unavailable or malformed item. Retry
                # only those positions through the single-item endpoint.
                resps = (resps + [None] * len(batch))[:len(batch)]
                for i, resp in enumerate(resps):
                    if not resp:
                        resps[i] = cls.invoke_libre_md(ContClass, batch[i]) or None
                return resps
            Printer.hashtaged(PrintChannel.API_ERROR, f'RETRY LIMIT EXCEEDED\n' +
                                                      f'FAILED TO FETCH METADATA FOR {ContClass.uppers}')
            return None

        # Keep payloads comfortably below the size that caused 413 responses.
        # Split further on a 413, preserving the original URI order.
        batch_size = 50
        results = []
        for offset in range(0, len(uris), batch_size):
            batch_result = fetch_batch(uris[offset:offset + batch_size])
            if batch_result is None:
                return None
            results.extend(batch_result)
        return results
    
    @classmethod
    def invoke_url(cls, url: str, params: dict | None = None, expectFail: bool = False, force_login5: bool = False) -> dict[str, str | int | dict]:
        headers = {
            'Authorization': f'Bearer {LoginHandler.choose_token(force_login5)}',
            'Accept-Language': f'{cls.CONFIG.get_language()}',
            'Accept': 'application/json',
            'app-platform': 'WebPlayer',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0'
        }
        
        api_retry = 0
        while api_retry <= cls.CONFIG.get_retry_attempts():
            if api_retry and not expectFail:
                Printer.hashtaged(PrintChannel.WARNING, f'{"REQUEST SUCCESSFUL" if http.ok else "API ERROR"} {retry_text}- RETRYING\n' +
                                                        f'Status {http.status_code}:  '+
                                                        f'{resp.get(ERROR, {}).get(MESSAGE, "No message provided")}')
            if api_retry: sleep(retry_delay if not expectFail else 1)
            
            try:
                http = requests.get(url, headers=headers, params=params)
                fallback_message = cls.api_status_str(http.status_code, http)
                resp: dict[str, str | int | dict] = http.json()
                http.raise_for_status()
                if http.status_code != 202: break
                resp = {ERROR: {MESSAGE: fallback_message}}
            except json.decoder.JSONDecodeError:
                resp = {ERROR: {MESSAGE: "ERROR: MALFORMED JSON"}}
            except requests.exceptions.HTTPError:
                if expectFail:              pass
                elif http.status_code in {401, 403}:
                    Printer.hashtaged(PrintChannel.API_ERROR, 'API ERROR\n' +
                                                              'ATTEMPTING TO ACCESS FORBIDDEN ENDPOINT')
                    return {} # do not count as fetch, skip FETCH_DELAY
                elif not resp:              resp = {ERROR: {MESSAGE: "Received an empty response"}}
                elif not resp.get(ERROR):   resp = {ERROR: {MESSAGE: fallback_message}}
            finally: cls.TOTAL_API_CALLS += 1
            retry_text = f"(RETRY {api_retry}) " if api_retry else ""
            retry_delay = max(cls.CONFIG.get_retry_delay(api_retry), float(http.headers.get(RETRY_AFTER, 0.0)))
            api_retry += 1
        
        sleep(cls.CONFIG.get_fetch_delay())
        if http.status_code == 200:
            return resp
        elif api_retry <= cls.CONFIG.get_retry_attempts():
            Printer.hashtaged(PrintChannel.WARNING, f'REQUEST SUCCESSFUL\n' +
                                                    f'Status {http.status_code}:   \n' +
                                                    f'{fallback_message}')
        elif not expectFail:
            Printer.hashtaged(PrintChannel.API_ERROR, f'RETRY LIMIT EXCEDED\n' +
                                                      f'RESPONSE TEXT: {Printer.pretty(resp)}\n' +
                                                      f'URL: {Printer.pretty(url)}')
        return {}
    
    @classmethod
    def invoke_url_nextable(cls, url: str, stripper: tuple[str] | str = None, max: int = 0, params: dict = {}) -> list[dict] | dict[str, list[dict]]:
        
        def handle_next(resp: dict, strip: str | None, total: int = 0) -> list[dict]:
            nextable: dict = resp.get(strip, resp)
            items: list[dict] = nextable.get(ITEMS)
            if not items:
                p = "PAGINATED " if total > 0 else ""
                Printer.hashtaged(PrintChannel.WARNING, f'NO ITEMS FOUND IN {p}API RESPONSE')
                Printer.debug(resp)
                return []
            elif nextable.get(NEXT) is None or (max and total + len(items) >= max):
                return items[:max-total] if max else items
            return items + handle_next(cls.invoke_url(nextable[NEXT]), strip, total + len(items))
        
        resp = cls.invoke_url(url, {LIMIT: 50, OFFSET: 0} | params)
        if isinstance(stripper, tuple) and not resp:
            Printer.hashtaged(PrintChannel.WARNING, 'SEARCH FAILED\n' + 
                                                    'IF AN API ERROR INDICATED "Invalid Limit",\n' +
                                                    'RECOMMENDED TO SET CONFIG "API_CLIENT_LEGACY = False"')
            return {}
        elif isinstance(stripper, tuple): # resp is of form {TYPE : nextable_resp}, only used in search
            return {strip: handle_next(resp, strip) for strip in stripper}
        return handle_next(resp, stripper) if resp else []
    
    @classmethod
    def invoke_url_bulk(cls, url: str, bulk_items: list[str], stripper: str, limit: int = 50) -> list[dict[str, str | int | dict]]:
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
    def get_content_stream(cls, content, use_qual_pref: bool = True) -> Streamer | None:
        content_id = cls.to_libre_content(content.__class__, content.id)
        if not content_id: return
        qual = cls.DOWNLOAD_QUALITY if use_qual_pref else cls.parse_dl_quality()[1]
        Printer.logger(f'Fetching stream for {content.type_attr}:{content.id} at quality {qual.preferred.name}')
        try:
            if not content.file_ids or cls.FORCE_STREAM_API_CALLS:
                risky_method = False
                lds = cls.SESSION.content_feeder().load(content_id, qual, False, None)
                return lds.input_stream if lds else None
            risky_method = True
            if getattr(content, EXTERNAL_URL, None):
                url = cls.SESSION.client().head(content.external_url).url
                return cls.SESSION.cdn().stream_external_episode(content, url, None)
            file = qual.get_file([ParseDict(f, AudioFile()) for f in content.file_ids])
            key = cls.SESSION.audio_key().get_audio_key(content.gid, file.file_id)
            url = cls.SESSION.content_feeder().resolve_storage_interactive(file.file_id, False)
            streamer = cls.SESSION.cdn().stream_file(file, key, CdnFeedHelper.get_url(url), None)
            if streamer.stream().skip(0xA7) != 0xA7: raise IOError("Couldn't skip 0xa7 bytes!")
            return streamer
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
            error_arg = e.args[0]
            if isinstance(error_arg, str) and 'Failed fetching audio key!' in error_arg:
                gid, fileid = error_arg.split('! ')[1].split(', ')
                Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO KEY\n' +
                                                  'MAY BE CAUSED BY RATE LIMITS - CONSIDER INCREASING `BULK_WAIT_TIME`\n' +
                                                 f'GID: {gid[5:]} - File_ID: {fileid[8:]}')
                Printer.logger("\n".join(e.args), PrintChannel.ERROR)
            elif isinstance(error_arg, int):
                Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO KEY\n' +
                                                     f'(ASSUMED HTTP) RUNTIME ERROR - STATUS CODE {error_arg}')
                Printer.logger("\n".join(e.args), PrintChannel.ERROR)
            else: raise
        except ConnectionError as e:
            if "Status code " not in e.args[0]: raise
            status_code = e.args[0].split("Status code ")[1]
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO FILE\n' +
                                                 f'CONNECTION ERROR WHEN FETCHING CONTENT STREAM - STATUS CODE {status_code}')
            Printer.logger("\n".join(e.args), PrintChannel.ERROR)
        except Exception as e:
            if risky_method:
                cls.FORCE_STREAM_API_CALLS = True
                return cls.get_content_stream(content, use_qual_pref=use_qual_pref)
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO FETCH AUDIO STREAM\n' +
                                                  'AN UNEXPECTED ERROR OCCURED - CHECK LOGS FOR DETAILS')
            Printer.traceback(e)
        return None
    
    @classmethod
    def get_user_profile(cls, username: str) -> dict:
        try:
            return cls.SESSION.api().get_user_profile(username)
        except Exception as e:
            Printer.debug(f"Failed to fetch user profile for {username}")
            Printer.traceback(e)
            return {}
    
    @classmethod
    def end(cls) -> None:
        cls.start_stats()
        LogHandler.kill_logger()
        
        for dir in (Path(cls.CONFIG.get_root_path()), Path(cls.CONFIG.get_root_podcast_path())):
            for tempfile in dir.glob("*.tmp"):
                tempfile.unlink()
        
        print("\n")
