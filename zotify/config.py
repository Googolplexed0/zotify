import datetime
import logging
import json
import sys
import re
import requests
from librespot.audio.decoders import VorbisOnlyAudioQuality
from pathlib import Path, PurePath
from time import sleep
from typing import Any, Callable

from zotify import Session
from zotify.const import *
from zotify.termoutput import Printer, PrintChannel, Loader


CONFIG_VALUES = {
    # Main Options
    ROOT_PATH: {
        "default": "~/Music/Zotify Music",
        "type": str,
        "arg": (
            "-rp",
            "--root-path",
        ),
    },
    SAVE_CREDENTIALS: {"default": "True", "type": bool, "arg": ("--save-credentials",)},
    CREDENTIALS_LOCATION: {
        "default": "",
        "type": str,
        "arg": (
            "--creds",
            "--credentials-location",
        ),
    },
    # File Options
    OUTPUT: {"default": "", "type": str, "arg": ("--output",)},
    OUTPUT_PLAYLIST: {
        "default": "{playlist}/{artist}_{song_name}",
        "type": str,
        "arg": (
            "-op",
            "--output-playlist",
        ),
    },
    OUTPUT_PLAYLIST_EXT: {
        "default": "{playlist}/{playlist_num}_{artist}_{song_name}",
        "type": str,
        "arg": (
            "-oe",
            "--output-ext-playlist",
        ),
    },
    OUTPUT_LIKED_SONGS: {
        "default": "Liked Songs/{artist}_{song_name}",
        "type": str,
        "arg": (
            "-ol",
            "--output-liked-songs",
        ),
    },
    OUTPUT_SINGLE: {
        "default": "{artist}/{album}/{artist}_{song_name}",
        "type": str,
        "arg": (
            "-os",
            "--output-single",
        ),
    },
    OUTPUT_ALBUM: {
        "default": "{artist}/{album}/{album_num}_{artist}_{song_name}",
        "type": str,
        "arg": (
            "-oa",
            "--output-album",
        ),
    },
    ROOT_PODCAST_PATH: {
        "default": "~/Music/Zotify Podcasts",
        "type": str,
        "arg": (
            "-rpp",
            "--root-podcast-path",
        ),
    },
    SPLIT_ALBUM_DISCS: {
        "default": "False",
        "type": bool,
        "arg": ("--split-album-discs",),
    },
    MAX_FILENAME_LENGTH: {
        "default": "0",
        "type": int,
        "arg": ("--max-filename-length",),
    },
    # Download Options
    BULK_WAIT_TIME: {"default": "1", "type": int, "arg": ("--bulk-wait-time",)},
    DOWNLOAD_REAL_TIME: {
        "default": "False",
        "type": bool,
        "arg": (
            "-rt",
            "--download-real-time",
        ),
    },
    TEMP_DOWNLOAD_DIR: {
        "default": "",
        "type": str,
        "arg": (
            "-td",
            "--temp-download-dir",
        ),
    },
    DOWNLOAD_PARENT_ALBUM: {
        "default": "False",
        "type": bool,
        "arg": ("--download-parent-album",),
    },
    NO_COMPILATION_ALBUMS: {
        "default": "False",
        "type": bool,
        "arg": ("--no-compilation-albums",),
    },
    # Regex Options
    REGEX_ENABLED: {"default": "False", "type": bool, "arg": ("--regex-enabled",)},
    REGEX_TRACK_SKIP: {"default": "", "type": str, "arg": ("--regex-track-skip",)},
    REGEX_EPISODE_SKIP: {"default": "", "type": str, "arg": ("--regex-episode-skip",)},
    REGEX_ALBUM_SKIP: {"default": "", "type": str, "arg": ("--regex-album-skip",)},
    # Encoding Options
    DOWNLOAD_FORMAT: {
        "default": "copy",
        "type": str,
        "arg": (
            "--codec",
            "--download-format",
        ),
    },
    DOWNLOAD_QUALITY: {
        "default": "auto",
        "type": str,
        "arg": (
            "-q",
            "--download-quality",
        ),
    },
    TRANSCODE_BITRATE: {
        "default": "auto",
        "type": str,
        "arg": (
            "-b",
            "--bitrate",
            "--transcode-bitrate",
        ),
    },
    # Archive Options
    SONG_ARCHIVE_LOCATION: {
        "default": "",
        "type": str,
        "arg": ("--song-archive-location",),
    },
    DISABLE_SONG_ARCHIVE: {
        "default": "False",
        "type": bool,
        "arg": ("--disable-song-archive",),
    },
    DISABLE_DIRECTORY_ARCHIVES: {
        "default": "False",
        "type": bool,
        "arg": ("--disable-directory-archives",),
    },
    SKIP_EXISTING: {
        "default": "True",
        "type": bool,
        "arg": (
            "-ie",
            "--skip-existing",
        ),
    },
    SKIP_PREVIOUSLY_DOWNLOADED: {
        "default": "False",
        "type": bool,
        "arg": (
            "-ip",
            "--skip-prev-downloaded",
            "--skip-previously-downloaded",
        ),
    },
    # Playlist File Options
    EXPORT_M3U8: {"default": "False", "type": bool, "arg": ("-e, --export-m3u8",)},
    M3U8_LOCATION: {"default": "", "type": str, "arg": ("--m3u8-location",)},
    M3U8_REL_PATHS: {
        "default": "True",
        "type": bool,
        "arg": ("--m3u8-relative-paths",),
    },
    LIKED_SONGS_ARCHIVE_M3U8: {
        "default": "True",
        "type": bool,
        "arg": ("--liked-songs-archive-m3u8",),
    },
    # Lyric File Options
    DOWNLOAD_LYRICS: {"default": "True", "type": bool, "arg": ("--download-lyrics",)},
    LYRICS_LOCATION: {"default": "", "type": str, "arg": ("--lyrics-location",)},
    ALWAYS_CHECK_LYRICS: {
        "default": "False",
        "type": bool,
        "arg": ("--always-check-lyrics",),
    },
    LYRICS_MD_HEADER: {
        "default": "False",
        "type": bool,
        "arg": ("--lyrics-md-header",),
    },
    # Metadata Options
    LANGUAGE: {"default": "en", "type": str, "arg": ("--language",)},
    STRICT_LIBRARY_VERIFY: {
        "default": "True",
        "type": bool,
        "arg": ("--strict-library-verify",),
    },
    MD_DISC_TRACK_TOTALS: {
        "default": "True",
        "type": bool,
        "arg": ("--md-disc-track-totals",),
    },
    MD_SAVE_GENRES: {"default": "True", "type": bool, "arg": ("--md-save-genres",)},
    MD_ALLGENRES: {"default": "False", "type": bool, "arg": ("--md-allgenres",)},
    MD_GENREDELIMITER: {"default": ", ", "type": str, "arg": ("--md-genredelimiter",)},
    MD_ARTISTDELIMITER: {
        "default": ", ",
        "type": str,
        "arg": ("--md-artistdelimiter",),
    },
    MD_SAVE_LYRICS: {"default": "True", "type": bool, "arg": ("--md-save-lyrics",)},
    ALBUM_ART_JPG_FILE: {
        "default": "False",
        "type": bool,
        "arg": ("--album-art-jpg-file",),
    },
    # API Options
    RETRY_ATTEMPTS: {"default": "1", "type": int, "arg": ("--retry-attempts",)},
    CHUNK_SIZE: {"default": "20000", "type": int, "arg": ("--chunk-size",)},
    OAUTH_ADDRESS: {"default": "0.0.0.0", "type": str, "arg": ("--oauth-address",)},
    REDIRECT_ADDRESS: {
        "default": "127.0.0.1",
        "type": str,
        "arg": ("--redirect-address",),
    },
    # Terminal & Logging Options
    PRINT_SPLASH: {"default": "False", "type": bool, "arg": ("--print-splash",)},
    PRINT_PROGRESS_INFO: {
        "default": "True",
        "type": bool,
        "arg": ("--print-progress-info",),
    },
    PRINT_SKIPS: {"default": "True", "type": bool, "arg": ("--print-skips",)},
    PRINT_DOWNLOADS: {"default": "True", "type": bool, "arg": ("--print-downloads",)},
    PRINT_DOWNLOAD_PROGRESS: {
        "default": "True",
        "type": bool,
        "arg": ("--print-download-progress",),
    },
    PRINT_URL_PROGRESS: {
        "default": "True",
        "type": bool,
        "arg": ("--print-url-progress",),
    },
    PRINT_ALBUM_PROGRESS: {
        "default": "True",
        "type": bool,
        "arg": ("--print-album-progress",),
    },
    PRINT_ARTIST_PROGRESS: {
        "default": "True",
        "type": bool,
        "arg": ("--print-artist-progress",),
    },
    PRINT_PLAYLIST_PROGRESS: {
        "default": "True",
        "type": bool,
        "arg": ("--print-playlist-progress",),
    },
    PRINT_WARNINGS: {"default": "True", "type": bool, "arg": ("--print-warnings",)},
    PRINT_ERRORS: {"default": "True", "type": bool, "arg": ("--print-errors",)},
    PRINT_API_ERRORS: {"default": "True", "type": bool, "arg": ("--print-api-errors",)},
    FFMPEG_LOG_LEVEL: {"default": "error", "type": str, "arg": ("--ffmpeg-log-level",)},
}


DEPRECIATED_CONFIGS = {
    "SONG_ARCHIVE": {"default": "", "type": str, "arg": ("--song-archive",)},
    "OVERRIDE_AUTO_WAIT": {
        "default": "False",
        "type": bool,
        "arg": ("--override-auto-wait",),
    },
    "REDIRECT_URI": {
        "default": "127.0.0.1:4381",
        "type": str,
        "arg": ("--redirect-uri",),
    },
}


class Config:
    Values = {}
    logger = None

    @classmethod
    def load(cls, args) -> None:
        system_paths = {
            "win32": Path.home() / "AppData/Roaming/Zotify",
            "linux": Path.home() / ".config/zotify",
            "darwin": Path.home() / "Library/Application Support/Zotify",
        }
        if sys.platform not in system_paths:
            config_fp = Path.cwd() / ".zotify/config.json"
        else:
            config_fp = system_paths[sys.platform] / "config.json"
        if args.config_location:
            config_fp = Path(args.config_location)
            if config_fp.is_dir():
                config_fp = config_fp / "config.json"
        full_config_path = Path(config_fp).expanduser()

        cls.Values = {}

        # Debug Check (guarantee at top of config)
        cls.Values[DEBUG] = args.debug

        # Load default values
        for key in CONFIG_VALUES:
            cls.Values[key] = cls.parse_arg_value(key, CONFIG_VALUES[key]["default"])

        # Load config from config.json
        Path(PurePath(full_config_path).parent).mkdir(parents=True, exist_ok=True)
        if not Path(full_config_path).exists():
            if cls.Values[DEBUG] == False:
                del cls.Values[DEBUG]
            with open(full_config_path, "w", encoding="utf-8") as config_file:
                json.dump(cls.get_default_json(), config_file, indent=4)
            Printer.hashtaged(
                PrintChannel.MANDATORY,
                f"config.json saved to {full_config_path.resolve().parent}",
            )
        else:
            with open(full_config_path, encoding="utf-8") as config_file:
                jsonvalues: dict[str, dict[str, Any]] = json.load(config_file)
            for key in jsonvalues:
                if key == DEBUG and not cls.Values[DEBUG]:
                    cls.Values[DEBUG] = str(jsonvalues[key]).lower() in [
                        "yes",
                        "true",
                        "1",
                    ]
                elif key in CONFIG_VALUES:
                    cls.Values[key] = cls.parse_arg_value(key, jsonvalues[key])
                elif (
                    key in DEPRECIATED_CONFIGS
                ):  # keep, warn, and place at the bottom (don't delete)
                    Printer.depreciated_warning(
                        key,
                        f'Delete the "{key}": "{jsonvalues[key]}" line from your config.json',
                    )
                    cls.Values["vvv___DEPRECIATED_BELOW_HERE___vvv"] = (
                        "vvv___REMOVE_THESE___vvv"
                    )
                    cls.Values[key] = cls.parse_arg_value(
                        key, jsonvalues[key], DEPRECIATED_CONFIGS
                    )

        # Standardize config.json if debugging or refreshing
        if cls.debug() or args.update_config:
            if cls.debug() and not full_config_path.name.endswith("_DEBUG.json"):
                full_config_path = full_config_path.parent / (
                    full_config_path.stem + "_DEBUG.json"
                )
            with open(
                full_config_path,
                "w" if full_config_path.exists() else "x",
                encoding="utf-8",
            ) as debug_file:
                json.dump(cls.parse_config_jsonstr(), debug_file, indent=4)
            real_debug = cls.Values[DEBUG]
            cls.Values[DEBUG] = True
            Printer.hashtaged(
                PrintChannel.DEBUG,
                f"{full_config_path.name} saved to {full_config_path.resolve().parent}",
            )
            cls.Values[DEBUG] = real_debug

        # Override config from commandline arguments
        for key in CONFIG_VALUES:
            if key.lower() in vars(args) and vars(args)[key.lower()] is not None:
                cls.Values[key] = cls.parse_arg_value(key, vars(args)[key.lower()])

        # Handle sub-library logging
        if cls.debug():
            logfile = Path(
                cls.get_root_path() / f"zotify_DEBUG_{Zotify.DATETIME_LAUNCH}.log"
            )
            Printer.hashtaged(
                PrintChannel.DEBUG,
                f"{logfile.name} logging to {logfile.resolve().parent}",
            )
            logging.basicConfig(level=logging.DEBUG, filemode="x", filename=logfile)
            cls.logger = logging.getLogger("zotify.debug")
        else:
            logging.basicConfig(level=logging.CRITICAL)
            mutedLoggers = {
                "Librespot:Session",
                "Librespot:AudioKeyManager",
                "librespot.audio",
                "Librespot:CdnManager",
            }
            for logger in mutedLoggers:
                logging.getLogger(logger).disabled = True

        # Confirm regex patterns
        if cls.get_regex_enabled():
            for mode in ["Track", "Episode", "Album"]:
                regex_method: Callable[[None], None | re.Pattern] = getattr(
                    cls, f"get_regex_{mode.lower()}"
                )
                if regex_method():
                    Printer.hashtaged(
                        PrintChannel.DEBUG,
                        f'{mode} Regex Filter:  r"{regex_method().pattern}"',
                    )

        # Check no-splash
        if args.no_splash:
            cls.Values[PRINT_SPLASH] = False

    @classmethod
    def get_default_json(cls) -> dict:
        r = {}
        # if DEBUG in cls.Values and cls.Values[DEBUG]:
        #     r[DEBUG] = True
        for key in CONFIG_VALUES:
            r[key] = CONFIG_VALUES[key]["default"]
        return r

    @classmethod
    def parse_config_jsonstr(cls, key_subset: tuple | dict | None = None) -> dict:
        d = {}
        if key_subset is None:
            key_subset = cls.Values
        for key in key_subset:
            d[key] = str(cls.Values[key])
        return d

    @classmethod
    def parse_arg_value(cls, key: str, value: Any, dict_source=CONFIG_VALUES) -> Any:
        if isinstance(value, dict_source[key]["type"]):
            return value
        if dict_source[key]["type"] == str:
            return str(value)
        if dict_source[key]["type"] == int:
            return int(value)
        if dict_source[key]["type"] == bool:
            if str(value).lower() in ["yes", "true", "1"]:
                return True
            if str(value).lower() in ["no", "false", "0"]:
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
        if cls.get(ROOT_PATH) == "":
            root_path = PurePath(Path.home() / "Music/Zotify Music/")
        else:
            root_path = PurePath(Path(cls.get(ROOT_PATH)).expanduser())
        Path(root_path).mkdir(parents=True, exist_ok=True)
        return root_path

    @classmethod
    def get_root_podcast_path(cls) -> PurePath:
        if cls.get(ROOT_PODCAST_PATH) == "":
            root_podcast_path = PurePath(Path.home() / "Music/Zotify Podcasts/")
        else:
            root_podcast_path: str = cls.get(ROOT_PODCAST_PATH)
            if root_podcast_path[0] == ".":
                root_podcast_path = cls.get_root_path() / PurePath(
                    root_podcast_path
                ).relative_to(".")
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
        if cls.get(SONG_ARCHIVE_LOCATION) == "":
            system_paths = {
                "win32": Path.home() / "AppData/Roaming/Zotify",
                "linux": Path.home() / ".local/share/zotify",
                "darwin": Path.home() / "Library/Application Support/Zotify",
            }
            if sys.platform not in system_paths:
                song_archive = PurePath(Path.cwd() / ".zotify/.song_archive")
            else:
                song_archive = PurePath(system_paths[sys.platform] / ".song_archive")
        else:
            song_archive_path: str = cls.get(SONG_ARCHIVE_LOCATION)
            if song_archive_path[0] == ".":
                song_archive_path = cls.get_root_path() / PurePath(
                    song_archive_path
                ).relative_to(".")
            song_archive = PurePath(
                Path(song_archive_path).expanduser() / ".song_archive"
            )
        Path(song_archive.parent).mkdir(parents=True, exist_ok=True)
        return song_archive

    @classmethod
    def get_save_credentials(cls) -> bool:
        return cls.get(SAVE_CREDENTIALS)

    @classmethod
    def get_credentials_location(cls) -> PurePath:
        creds_location = cls.get(CREDENTIALS_LOCATION)
        if not creds_location or creds_location == "":
            system_paths = {
                "win32": Path.home() / "AppData/Roaming/Zotify",
                "linux": Path.home() / ".local/share/zotify",
                "darwin": Path.home() / "Library/Application Support/Zotify",
            }
            if sys.platform not in system_paths:
                credentials = PurePath(Path.cwd() / ".zotify/credentials.json")
            else:
                credentials = PurePath(system_paths[sys.platform] / "credentials.json")
        else:
            credentials_path: str = creds_location
            if credentials_path and credentials_path[0] == ".":
                credentials_path = cls.get_root_path() / PurePath(
                    credentials_path
                ).relative_to(".")
            credentials = PurePath(
                Path(credentials_path).expanduser() / "credentials.json"
            )
        Path(credentials.parent).mkdir(parents=True, exist_ok=True)
        return credentials

    @classmethod
    def get_temp_download_dir(cls) -> str | PurePath:
        if cls.get(TEMP_DOWNLOAD_DIR) == "":
            return ""
        temp_download_path: str = cls.get(TEMP_DOWNLOAD_DIR)
        if temp_download_path[0] == ".":
            temp_download_path = cls.get_root_path() / PurePath(
                temp_download_path
            ).relative_to(".")
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
            # User must include {disc_number} in OUTPUT if they want split album discs
            return v

        if mode == "playlist":
            v = cls.get(OUTPUT_PLAYLIST)
        elif mode == "extplaylist":
            v = cls.get(OUTPUT_PLAYLIST_EXT)
        elif mode == "liked":
            v = cls.get(OUTPUT_LIKED_SONGS)
        elif mode == "single":
            v = cls.get(OUTPUT_SINGLE)
        elif mode == "album":
            v = cls.get(OUTPUT_ALBUM)
        else:
            raise ValueError()

        if cls.get_split_album_discs() and mode == "album":
            return str(PurePath(v).parent / "Disc {disc_number}" / PurePath(v).name)
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
        if cls.get(LYRICS_LOCATION) == "":
            # Use OUTPUT path as default location
            return None
        else:
            lyrics_path = cls.get(LYRICS_LOCATION)
            if lyrics_path[0] == ".":
                lyrics_path = cls.get_root_path() / PurePath(lyrics_path).relative_to(
                    "."
                )
            lyrics_path = PurePath(Path(lyrics_path).expanduser())

        return lyrics_path

    @classmethod
    def get_ffmpeg_log_level(cls) -> str:
        level = cls.get(FFMPEG_LOG_LEVEL)
        # see https://ffmpeg.org/ffmpeg.html#Generic-options, -loglevel
        valid_levels = {
            "trace",
            "debug",
            "verbose",
            "info",
            "warning",
            "error",
            "fatal",
            "panic",
            "quiet",
        }

        if level == "warn":
            level += "ing"
        if level not in valid_levels:
            raise ValueError(
                f'FFMPEG LOGGING LEVEL "{level}" NOT VALID\n'
                + f"SELECT FROM: {valid_levels}"
            )
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
        return (
            cls.get(PRINT_DOWNLOAD_PROGRESS)
            or cls.get(PRINT_URL_PROGRESS)
            or cls.get(PRINT_ALBUM_PROGRESS)
            or cls.get(PRINT_ARTIST_PROGRESS)
            or cls.get(PRINT_PLAYLIST_PROGRESS)
        )

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
        if cls.get(M3U8_LOCATION) == "":
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
    def get_skip_comp_albums(cls) -> bool:
        return cls.get(NO_COMPILATION_ALBUMS)

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
    def get_lyrics_header(cls) -> bool:
        return cls.get(LYRICS_MD_HEADER)

    @classmethod
    def get_strict_library_verify(cls) -> bool:
        return cls.get(STRICT_LIBRARY_VERIFY)

class Zotify:
    SESSION: Session = None
    DOWNLOAD_QUALITY = None
    TOTAL_API_CALLS = 0
    DATETIME_LAUNCH = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    CONFIG: Config = Config()

    def __init__(self, args):
        Zotify.CONFIG.load(args)
        with Loader(PrintChannel.MANDATORY, "Logging in..."):
            import time

            time.sleep(0.5)

        Zotify.login(args)

    @classmethod
    def login(cls, args):
        """Authenticates and saves credentials to a file using Zeroconf method"""
        # Create session using Zeroconf method by default

        # Check if we have valid credentials first
        if cls.SESSION is None:
            # Try to load existing credentials
            credentials_path = cls.CONFIG.get_credentials_location()
            if credentials_path and Path(credentials_path).exists():
                try:
                    Printer.new_print(
                        PrintChannel.MANDATORY, "CHECKING EXISTING CREDENTIALS..."
                    )
                    cls.SESSION = (
                        Session.Builder().stored_file(str(credentials_path)).create()
                    )

                    # Test if the session has API access by trying to get a token
                    try:
                        cls.SESSION.tokens().get("user-read-private")
                        Printer.new_print(
                            PrintChannel.MANDATORY, "EXISTING CREDENTIALS ARE VALID"
                        )
                        return

                    except Exception as e:
                        Printer.new_print(
                            PrintChannel.WARNING,
                            f"CREDENTIALS EXIST BUT INVALID\n{str(e)}",
                        )
                        # Remove invalid credentials
                        try:
                            Path(credentials_path).unlink()
                            Printer.new_print(
                                PrintChannel.MANDATORY, "REMOVED INVALID CREDENTIALS"
                            )
                        except Exception:
                            pass
                        cls.SESSION = None

                except Exception as e:
                    Printer.new_print(
                        PrintChannel.WARNING, f"EXISTING CREDENTIALS INVALID\n{str(e)}"
                    )
                    # Remove invalid credentials
                    try:
                        Path(credentials_path).unlink()
                        Printer.new_print(
                            PrintChannel.MANDATORY, "REMOVED INVALID CREDENTIALS"
                        )
                    except Exception:
                        pass
            else:
                Printer.new_print(
                    PrintChannel.MANDATORY, "NO EXISTING CREDENTIALS FOUND"
                )

            # Generate new credentials via Zeroconf
            Printer.new_print(
                PrintChannel.MANDATORY,
                "GENERATING FRESH CREDENTIALS VIA ZEROCONF FOR API ACCESS...",
            )

            # Use Zeroconf method to generate credentials
            Printer.new_print(
                PrintChannel.MANDATORY,
                "STARTING ZEROCONF SERVER FOR CREDENTIAL GENERATION...",
            )
            Printer.new_print(PrintChannel.MANDATORY, "INSTRUCTIONS:")
            Printer.new_print(
                PrintChannel.MANDATORY,
                "1. OPEN SPOTIFY ON YOUR DESKTOP\n2. LOOK FOR 'ZOTIFY' IN YOUR DEVICES LIST\n3. CLICK ON 'ZOTIFY' TO TRANSFER PLAYBACK\n4. WAIT FOR CREDENTIALS TO BE GENERATED",
            )

            try:
                from zotify.credential_manager import generate_credentials_interactive

                # Generate credentials using Zeroconf
                credentials = generate_credentials_interactive(
                    credentials_path=str(cls.CONFIG.get_credentials_location())
                )

                if credentials:
                    Printer.new_print(
                        PrintChannel.MANDATORY, "CREDENTIALS GENERATED SUCCESSFULLY"
                    )
                    # Try to create session from the generated credentials
                    try:
                        credentials_path = cls.CONFIG.get_credentials_location()
                        cls.SESSION = (
                            Session.Builder()
                            .stored_file(str(credentials_path))
                            .create()
                        )
                        Printer.new_print(
                            PrintChannel.MANDATORY, "SESSION CREATED SUCCESSFULLY"
                        )
                    except Exception as e:
                        Printer.new_print(
                            PrintChannel.ERROR,
                            f"Failed to create session from generated credentials\n{str(e)}",
                        )
                        raise RuntimeError(
                            "Failed to create session from generated credentials"
                        )
                else:
                    Printer.new_print(
                        PrintChannel.ERROR, "FAILED TO GENERATE CREDENTIALS"
                    )
                    raise RuntimeError("Zeroconf credential generation failed")

            except Exception as e:
                Printer.new_print(
                    PrintChannel.ERROR, f"ZEROCONF AUTHENTICATION FAILED\n{str(e)}"
                )
                Printer.new_print(
                    PrintChannel.ERROR,
                    "ZOTIFY REQUIRES ZEROCONF AUTHENTICATION TO WORK",
                )
                Printer.new_print(
                    PrintChannel.ERROR,
                    "PLEASE ENSURE YOUR NETWORK SUPPORTS MDNS AND TRY AGAIN",
                )
                raise RuntimeError("Zeroconf authentication is required and failed")

    @classmethod
    def get_content_stream(cls, content_id, quality):
        try:
            return cls.SESSION.content_feeder().load(
                content_id, VorbisOnlyAudioQuality(quality), False, None
            )
        except RuntimeError as e:
            if "Failed fetching audio key!" in e.args[0]:
                gid, fileid = e.args[0].split("! ")[1].split(", ")
                Printer.hashtaged(
                    PrintChannel.ERROR,
                    "FAILED TO FETCH AUDIO KEY\n"
                    + "MAY BE CAUSED BY RATE LIMITS - CONSIDER INCREASING `BULK_WAIT_TIME`\n"
                    + f"GID: {gid[5:]} - File_ID: {fileid[8:]}",
                )
            else:
                raise e

    @classmethod
    def __get_auth_token(cls):
        return (
            cls.SESSION.tokens()
            .get_token(
                USER_READ_EMAIL,
                PLAYLIST_READ_PRIVATE,
                USER_LIBRARY_READ,
                USER_FOLLOW_READ,
            )
            .access_token
        )

    @classmethod
    def get_auth_header(cls):
        return {
            "Authorization": f"Bearer {cls.__get_auth_token()}",
            "Accept-Language": f"{cls.CONFIG.get_language()}",
            "Accept": "application/json",
            "app-platform": "WebPlayer",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
        }

    @classmethod
    def invoke_url(
        cls, url: str, _params: dict | None = None, expectFail: bool = False
    ) -> tuple[str, dict]:
        headers = cls.get_auth_header()

        tryCount = 0
        while tryCount <= cls.CONFIG.get_retry_attempts():
            response = requests.get(url, headers=headers, params=_params)
            cls.TOTAL_API_CALLS += 1

            try:
                responsetext = response.text
                responsejson = response.json()
                if not responsejson:
                    raise json.decoder.JSONDecodeError
                # responsejson = {"error": {"status": "Unknown", "message": "Received an empty response"}}
            except json.decoder.JSONDecodeError:
                responsejson = {
                    "error": {
                        "status": "Unknown",
                        "message": "Received an empty response",
                    }
                }

            if not responsejson or "error" in responsejson:
                if not expectFail:
                    Printer.hashtaged(
                        PrintChannel.WARNING,
                        f"API ERROR (TRY {tryCount}) - RETRYING\n"
                        + f'{responsejson["error"]["status"]}: {responsejson["error"]["message"]}',
                    )
                sleep(5 if not expectFail else 1)
                tryCount += 1
                continue
            else:
                return responsetext, responsejson

        if not expectFail:
            Printer.hashtaged(
                PrintChannel.API_ERROR,
                f"API ERROR (TRY {tryCount}) - RETRY LIMIT EXCEDED\n"
                + f'{responsejson["error"]["status"]}: {responsejson["error"]["message"]}',
            )

        return responsetext, responsejson

    @classmethod
    def invoke_url_with_params(cls, url, limit, offset, **kwargs):
        params = {LIMIT: limit, OFFSET: offset}
        params.update(kwargs)

        _, responsejson = cls.invoke_url(url, params)
        return responsejson

    @classmethod
    def invoke_url_nextable(
        cls,
        url: str,
        response_key: str = ITEMS,
        limit: int = 50,
        stripper: str | None = None,
        offset: int = 0,
    ) -> list[dict]:
        resp = cls.invoke_url_with_params(url, limit=limit, offset=offset)
        if stripper is not None:
            resp = resp[stripper]
        items: list = resp[response_key]

        while resp["next"] is not None:
            (raw, resp) = Zotify.invoke_url(resp["next"])
            items.extend(resp[response_key])
        return items

    @classmethod
    def invoke_url_bulk(
        cls, url: str, bulk_items: list[str], stripper: str, limit: int = 50
    ) -> list[dict]:
        items = []
        while len(bulk_items):
            items_batch = "%2c".join(bulk_items[:limit])
            bulk_items = bulk_items[limit:]

            (raw, resp) = Zotify.invoke_url(url + items_batch)
            items.extend(resp[stripper])
        return items

    @classmethod
    def check_premium(cls) -> bool:
        return cls.SESSION.get_user_attribute(TYPE) == PREMIUM
