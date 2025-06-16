import json
import sys
from pathlib import Path, PurePath
from typing import Any

from zotify.const import *
from zotify.termoutput import Printer, PrintChannel


CONFIG_VALUES = {
    # Main Options
    ROOT_PATH:                  { 'default': '~/Music/Zotify Music',    'type': str,    'arg': ('-rp', '--root-path'                     ,) },
    SAVE_CREDENTIALS:           { 'default': 'True',                    'type': bool,   'arg': ('--save-credentials'                     ,) },
    CREDENTIALS_LOCATION:       { 'default': '',                        'type': str,    'arg': ('--creds', '--credentials-location'      ,) },
    
    # File Options
    OUTPUT:                     { 'default': '',                        'type': str,    'arg': ('--output'                               ,) },
    OUTPUT_PLAYLIST:            { 'default': '{playlist}/{artist}_{song_name}',
                                  'type': str, 
                                  'arg': ('-op', '--output-playlist' ,) },
    OUTPUT_PLAYLIST_EXT:        { 'default': '{playlist}/{playlist_num}_{artist}_{song_name}',
                                  'type': str,  
                                  'arg': ('-oe', '--output-ext-playlist' ,) },
    OUTPUT_LIKED_SONGS:         { 'default': 'Liked Songs/{artist}_{song_name}',
                                  'type': str,
                                  'arg': ('-ol', '--output-liked-songs' ,) },
    OUTPUT_SINGLE:              { 'default': '{artist}/{album}/{artist}_{song_name}',
                                  'type': str,
                                  'arg': ('-os', '--output-single' ,) },
    OUTPUT_ALBUM:               { 'default': '{artist}/{album}/{album_num}_{artist}_{song_name}',
                                  'type': str,
                                  'arg': ('-oa', '--output-album' ,) },
    ROOT_PODCAST_PATH:          { 'default': '~/Music/Zotify Podcasts', 'type': str,    'arg': ('-rpp', '--root-podcast-path'            ,) },
    SPLIT_ALBUM_DISCS:          { 'default': 'False',                   'type': bool,   'arg': ('--split-album-discs'                    ,) },
    MAX_FILENAME_LENGTH:        { 'default': '0',                       'type': int,    'arg': ('--max-filename-length'                  ,) },
    
    # Download Options
    BULK_WAIT_TIME:             { 'default': '1',                       'type': int,    'arg': ('--bulk-wait-time'                       ,) },
    DOWNLOAD_REAL_TIME:         { 'default': 'False',                   'type': bool,   'arg': ('-rt', '--download-real-time'            ,) },
    TEMP_DOWNLOAD_DIR:          { 'default': '',                        'type': str,    'arg': ('-td', '--temp-download-dir'             ,) },
    DOWNLOAD_PARENT_ALBUM:      { 'default': 'False',                   'type': bool,   'arg': ('--download-parent-album'                ,) },
    NO_COMPILATION_ALBUMS:      { 'default': 'False',                   'type': bool,   'arg': ('--no-compilation-albums'                ,) },
    
    # Encoding Options
    DOWNLOAD_FORMAT:            { 'default': 'copy',                    'type': str,    'arg': ('--codec', '--download-format'           ,) },
    DOWNLOAD_QUALITY:           { 'default': 'auto',                    'type': str,    'arg': ('-q', '--download-quality'               ,) },
    TRANSCODE_BITRATE:          { 'default': 'auto',                    'type': str,    'arg': ('-b', '--bitrate', '--transcode-bitrate' ,) },
    
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
    
    # Lyric File Options
    DOWNLOAD_LYRICS:            { 'default': 'True',                    'type': bool,   'arg': ('--download-lyrics'                      ,) },
    LYRICS_LOCATION:            { 'default': '',                        'type': str,    'arg': ('--lyrics-location'                      ,) },
    ALWAYS_CHECK_LYRICS:        { 'default': 'False',                   'type': bool,   'arg': ('--always-check-lyrics'                  ,) },
    
    # Metadata Options
    LANGUAGE:                   { 'default': 'en',                      'type': str,    'arg': ('--language'                             ,) },
    MD_DISC_TRACK_TOTALS:       { 'default': 'True',                    'type': bool,   'arg': ('--md-disc-track-totals'                 ,) },
    MD_SAVE_GENRES:             { 'default': 'False',                   'type': bool,   'arg': ('--md-save-genres'                       ,) },
    MD_ALLGENRES:               { 'default': 'False',                   'type': bool,   'arg': ('--md-allgenres'                         ,) },
    MD_GENREDELIMITER:          { 'default': ', ',                      'type': str,    'arg': ('--md-genredelimiter'                    ,) },
    MD_ARTISTDELIMITER:         { 'default': ', ',                      'type': str,    'arg': ('--md-artistdelimiter'                   ,) },
    MD_SAVE_LYRICS:             { 'default': 'True',                    'type': bool,   'arg': ('--md-save-lyrics'                       ,) },
    ALBUM_ART_JPG_FILE:         { 'default': 'False',                   'type': bool,   'arg': ('--album-art-jpg-file'                   ,) },
    
    # API Options
    RETRY_ATTEMPTS:             { 'default': '1',                       'type': int,    'arg': ('--retry-attempts'                       ,) },
    CHUNK_SIZE:                 { 'default': '20000',                   'type': int,    'arg': ('--chunk-size'                           ,) },
    OAUTH_ADDRESS:              { 'default': '0.0.0.0',                 'type': str,    'arg': ('--oauth-address'                        ,) },
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
    FFMPEG_LOG_LEVEL:           { 'default': 'error',                   'type': str,    'arg': ('--ffmpeg-log-level'                     ,) },
}  


DEPRECIATED_CONFIGS = {
    "SONG_ARCHIVE":               { 'default': '',                        'type': str,    'arg': ('--song-archive'                         ,) },
    "OVERRIDE_AUTO_WAIT":         { 'default': 'False',                   'type': bool,   'arg': ('--override-auto-wait'                   ,) },
    "REDIRECT_URI":               { 'default': '127.0.0.1:4381',          'type': str,    'arg': ('--redirect-uri'                         ,) },
}


class Config:
    Values = {}
    
    @classmethod
    def load(cls, args) -> None:
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
        
        cls.Values = {}
        
        # Debug Check (guarantee at top of config)
        cls.Values[DEBUG] = args.debug
        
        # Load default values
        for key in CONFIG_VALUES:
            cls.Values[key] = cls.parse_arg_value(key, CONFIG_VALUES[key]['default'])
        
        # Load config from config.json
        Path(PurePath(full_config_path).parent).mkdir(parents=True, exist_ok=True)
        if not Path(full_config_path).exists():
            if cls.Values[DEBUG] == False: del cls.Values[DEBUG]
            with open(full_config_path, 'w', encoding='utf-8') as config_file:
                json.dump(cls.get_default_json(), config_file, indent=4)
            Printer.print(PrintChannel.MANDATORY, f"###   config.json saved to {full_config_path.resolve().parent}   ###\n")
        else:
            with open(full_config_path, encoding='utf-8') as config_file:
                jsonvalues: dict[str, dict[str, Any]] = json.load(config_file)
            for key in jsonvalues:
                if key == DEBUG and not cls.Values[DEBUG]:
                    cls.Values[DEBUG] = str(jsonvalues[key]).lower() in ['yes', 'true', '1']
                elif key in CONFIG_VALUES:
                    cls.Values[key] = cls.parse_arg_value(key, jsonvalues[key])
                elif key in DEPRECIATED_CONFIGS: # keep, warn, and place at the bottom (don't delete)
                    Printer.depreciated_warning(key, f'Delete the "{key}": "{jsonvalues[key]}" line from your config.json')
                    cls.Values["vvv___DEPRECIATED_BELOW_HERE___vvv"] = "vvv___REMOVE_THESE___vvv"
                    cls.Values[key] = cls.parse_arg_value(key, jsonvalues[key], DEPRECIATED_CONFIGS)
        
        # Standardize config.json if debugging or refreshing 
        if cls.debug() or args.update_config:
            if cls.debug() and not full_config_path.name.endswith("_DEBUG.json"):
                full_config_path = full_config_path.parent / (full_config_path.stem + "_DEBUG.json")
            with open(full_config_path, 'w' if full_config_path.exists() else 'x', encoding='utf-8') as debug_file:
                json.dump(cls.parse_config_jsonstr(), debug_file, indent=4)
            Printer.print(PrintChannel.MANDATORY, f"###   {full_config_path.name} saved to {full_config_path.resolve().parent}   ###\n")
        
        # Override config from commandline arguments
        for key in CONFIG_VALUES:
            if key.lower() in vars(args) and vars(args)[key.lower()] is not None:
                # Printer.print(f"{key} {cls.Values[key]} -> {cls.parse_arg_value(key, vars(args)[key.lower()])}")
                cls.Values[key] = cls.parse_arg_value(key, vars(args)[key.lower()])
        
        if args.no_splash:
            cls.Values[PRINT_SPLASH] = False
    
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
    def parse_arg_value(cls, key: str, value: Any, dict_source = CONFIG_VALUES) -> Any:
        if isinstance(value, dict_source[key]['type']):
            return value
        if dict_source[key]['type'] == str:
            return str(value)
        if dict_source[key]['type'] == int:
            return int(value)
        if dict_source[key]['type'] == bool:
            if str(value).lower() in ['yes', 'true', '1']:
                return True
            if str(value).lower() in ['no', 'false', '0']:
                return False
            raise ValueError("Not a boolean: " + value)
        raise ValueError("Unknown Type: " + value)
    
    @classmethod
    def get(cls, key: str) -> Any:
        return cls.Values.get(key)
    
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
        Path(root_podcast_path).mkdir(parents=True, exist_ok=True)
        return root_podcast_path
    
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
    def get_download_lyrics(cls) -> bool:
        return cls.get(DOWNLOAD_LYRICS)
    
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
    def get_download_quality(cls) -> str:
        return cls.get(DOWNLOAD_QUALITY)
    
    @classmethod
    def get_transcode_bitrate(cls) -> str:
        return cls.get(TRANSCODE_BITRATE)
    
    @classmethod
    def get_song_archive_location(cls) -> PurePath:
        if cls.get(SONG_ARCHIVE_LOCATION) == '':
            system_paths = {
                'win32': Path.home() / 'AppData/Roaming/Zotify',
                'linux': Path.home() / '.local/share/zotify',
                'darwin': Path.home() / 'Library/Application Support/Zotify'
            }
            if sys.platform not in system_paths:
                song_archive =  PurePath(Path.cwd() / '.zotify/.song_archive')
            else:
                song_archive = PurePath(system_paths[sys.platform] / '.song_archive')
        else:
            song_archive_path: str = cls.get(SONG_ARCHIVE_LOCATION)
            if song_archive_path[0] == ".":
                song_archive_path = cls.get_root_path() / PurePath(song_archive_path).relative_to(".")
            song_archive = PurePath(Path(song_archive_path).expanduser() / ".song_archive")
        Path(song_archive.parent).mkdir(parents=True, exist_ok=True)
        return song_archive
    
    @classmethod
    def get_save_credentials(cls) -> bool:
        return cls.get(SAVE_CREDENTIALS)
    
    @classmethod
    def get_credentials_location(cls) -> PurePath:
        if cls.get(CREDENTIALS_LOCATION) == '':
            system_paths = {
                'win32': Path.home() / 'AppData/Roaming/Zotify',
                'linux': Path.home() / '.local/share/zotify',
                'darwin': Path.home() / 'Library/Application Support/Zotify'
            }
            if sys.platform not in system_paths:
                credentials = PurePath(Path.cwd() / '.zotify/credentials.json')
            else:
                credentials = PurePath(system_paths[sys.platform] / 'credentials.json')
        else:
            credentials_path: str = cls.get(CREDENTIALS_LOCATION)
            if credentials_path[0] == ".":
                credentials_path = cls.get_root_path() / PurePath(credentials_path).relative_to(".")
            credentials = PurePath(Path(credentials_path).expanduser() / 'credentials.json')
        Path(credentials.parent).mkdir(parents=True, exist_ok=True)
        return credentials
    
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
    def get_output(cls, mode: str) -> str:
        v = cls.get(OUTPUT)
        if v:
            return v
        
        if mode == 'playlist':
            v = cls.get(OUTPUT_PLAYLIST)
        elif mode == 'extplaylist':
            v = cls.get(OUTPUT_PLAYLIST_EXT)
        elif mode == 'liked':
            v = cls.get(OUTPUT_LIKED_SONGS)
        elif mode == 'single':
            v = cls.get(OUTPUT_SINGLE)
        elif mode == 'album':
            v = cls.get(OUTPUT_ALBUM)
        else:
            raise ValueError()
        
        if cls.get_split_album_discs():
            return str(PurePath(v).parent / 'Disc {disc_number}' / PurePath(v).name)
        return v
    
    @classmethod
    def get_retry_attempts(cls) -> int:
        return cls.get(RETRY_ATTEMPTS)
    
    @classmethod
    def get_disable_directory_archives(cls) -> bool:
        return cls.get(DISABLE_DIRECTORY_ARCHIVES)
    
    @classmethod
    def get_disable_song_archive(cls) -> bool:
        return cls.get(DISABLE_SONG_ARCHIVE)
    
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
    def get_ffmpeg_log_level(cls) -> str:
        level = cls.get(FFMPEG_LOG_LEVEL)
        if level not in {"trace", "verbose", "info", "warning", "error", "fatal", "panic", "quiet"}:
            raise ValueError()
        return level
    
    @classmethod
    def get_show_download_pbar(cls) -> bool:
        return cls.get(PRINT_DOWNLOAD_PROGRESS)
    
    @classmethod
    def get_show_url_pbar(cls) -> bool:
        return cls.get(PRINT_URL_PROGRESS)
    
    @classmethod
    def get_show_album_pbar(cls) -> bool:
        return cls.get(PRINT_ALBUM_PROGRESS)
    
    @classmethod
    def get_show_artist_pbar(cls) -> bool:
        return cls.get(PRINT_ARTIST_PROGRESS)
    
    @classmethod
    def get_show_playlist_pbar(cls) -> bool:
        return cls.get(PRINT_PLAYLIST_PROGRESS)
    
    @classmethod
    def get_show_any_progress(cls) -> bool:
        return cls.get(PRINT_DOWNLOAD_PROGRESS) or cls.get(PRINT_URL_PROGRESS) \
           or cls.get(PRINT_ALBUM_PROGRESS) or cls.get(PRINT_ARTIST_PROGRESS) \
        or cls.get(PRINT_PLAYLIST_PROGRESS)
    
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
    def get_save_lyrics_tags(cls) -> bool:
        return cls.get(MD_SAVE_LYRICS)
    
    @classmethod
    def get_always_check_lyrics(cls) -> bool:
        return cls.get(ALWAYS_CHECK_LYRICS)
    
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
    def get_download_parent_album(cls) -> bool:
        return cls.get(DOWNLOAD_PARENT_ALBUM)
    
    @classmethod
    def get_oauth_addresses(cls) -> tuple[str, str]:
        return cls.get(REDIRECT_ADDRESS), cls.get(OAUTH_ADDRESS)
    
    @classmethod
    def get_skip_comp_albums(cls) -> bool:
        return cls.get(NO_COMPILATION_ALBUMS)
