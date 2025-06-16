# Zotify

## A highly customizable music and podcast downloader

<p align="center">
  <img src="https://i.imgur.com/hGXQWSl.png" width="50%" alt="Zotify logo">
</p>

## Features

- Downloads at up to 320kbps \*
- Downloads directly from the source \*\*
- Downloads podcasts, playlists, liked songs, albums, artists, singles.
- Downloads synced lyrics from the source
- Option to download in real time to reduce suspicious API request behavior \*\*\*
- Supports multiple audio formats
- Download directly from URL or use built-in in search
- Bulk downloads from a list of URLs in a text file or parsed directly as arguments

\* Free accounts are limited to 160kbps \*\
\*\* Audio files are NOT substituted with ones from other sources (such as YouTube or Deezer) \*\*\
\*\*\* 'Real time' downloading limits at the speed of data transfer to typical streaming rates (download time ≈  duration of the track) \*\*\*

## Dependencies

- Python 3.10 or greater
- FFmpeg

## Installation

<details open><summary><strong>Install as Executable</strong></summary>

*Useable across system from the command line*

`pipx install git+https://github.com/Googolplexed0/zotify.git`

</details>

<details><summary><strong>Install as Python Module</strong></summary>

*Useable when launched as a Python module*

`python -m pip install git+https://github.com/Googolplexed0/zotify.git`

</details>

### Advanced Installation Instructions

See [INSTALLATION](INSTALLATION.md) for a more detailed and opinionated installation walkthrough.

## Usage

`(python -m) zotify <track/album/playlist/episode/artist url>`

Download track(s), album(s), playlist(s), podcast episode(s), or artist(s) specified by the URL(s) passed as a command line argument(s).
If an artist's URL is given, all albums by the specified artist will be downloaded. Can take multiple URLs as multiple arguments.

### Basic Flags and Modes

`(python -m) zotify <{mode flag}> <{config flag} {config value}> <track/album/playlist/episode/artist url>`

| Command Line Config Flag           | Function                                                                                                                |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `-h`, `--help`                     | See this message                                                                                                        |
| `-v`, `--version`                  | Show the version of Zotify                                                                                              |
| `-c`, `--config-location`          | Specify a directory containing a Zotify `config.json` file to load settings (Also accepts a filepath to a `.json` file) |
| `-u`, `--username`                 | Account username                                                                                                        |
| `--token`                          | Authentication token                                                                                                    |
| `--debug`                          | Enable debug mode, prints extra information and creates a `config_DEBUG.json` file                                      |
| `--update-config`                  | Updates the `config.json` file while keeping all current settings unchanged                                             |

| Command Line Mode Flag (exclusive) | Mode                                                                                   |
|------------------------------------|----------------------------------------------------------------------------------------|
| `-s`, `--search`                   | Search tracks/albums/artists/playlists based on argument (interactive)                 |
| `-p`, `--playlist`                 | Download playlist(s) saved by your account (interactive)                               |
| `-l`, `--liked`                    | Download all Liked Songs on your account                                               |
| `-a`, `--artists`                  | Download all songs by all followed artists                                             |
| `-f`, `--file`                     | Download all tracks/albums/episodes/playlists URLs within the file passed as argument  |

<details><summary>

### Advanced Usage and Config Flags

</summary>

All options can be set via the commandline or in a [config.json file](#configuration-files). Commandline arguments take priority over config.json arguments.  
Set arguments in the commandline like this: `-ie False` or `--codec mp3`. Wrap commandline arguments containing spaces or non-alphanumeric characters (weird symbols) with quotes like this: `--output-liked-songs "Liked Songs/{song_name}"`

| Main Options                 | Command Line Config Flag            | Description                                                                  | Default Value             |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------|---------------------------|
| `ROOT_PATH`                  | `-rp`, `--root-path`                | Directory where music is saved (replaces "." in other path configs)          | `~/Music/Zotify Music`    |
| `SAVE_CREDENTIALS`           | `--save-credentials`                | Whether login credentials should be saved                                    | True                      |
| `CREDENTIALS_LOCATION`       | `--creds`, `--credentials-location` | Directory containing credentials.json                    | See [Path Option Parser](#path-option-parser) |

| File Options                 | Command Line Config Flag            | Description                                                                  | Default Value             |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------|---------------------------|
| `OUTPUT`                     | `--output`                          | Master output file pattern (overwrites all others)       | See [Output Format Examples](#output-formats) |
| `OUTPUT_PLAYLIST`            | `-op`, `--output-playlist`          | Output file pattern for playlists                        | See [Output Format Examples](#example-values) |
| `OUTPUT_PLAYLIST_EXT`        | `-oe`, `--output-ext-playlist`      | Output file pattern for extended playlists               | See [Output Format Examples](#example-values) |
| `OUTPUT_LIKED_SONGS`         | `-ol`, `--output-liked-songs`       | Output file pattern for user's Liked Songs               | See [Output Format Examples](#example-values) |
| `OUTPUT_SINGLE`              | `-os`, `--output-single`            | Output file pattern for single tracks                    | See [Output Format Examples](#example-values) |
| `OUTPUT_ALBUM`               | `-oa`, `--output-album`             | Output file pattern for albums                           | See [Output Format Examples](#example-values) |
| `ROOT_PODCAST_PATH`          | `-rpp`, `--root-podcast-path`       | Directory where podcasts are saved                                           | `~/Music/Zotify Podcasts` |
| `SPLIT_ALBUM_DISCS`          | `--split-album-discs`               | Saves each disk in its own folder                                            | False                     |
| `MAX_FILENAME_LENGTH`        | `--max-filename-length`             | Maximum character length of filenames, truncated to fit, 0 meaning no limit  | 0                         |

| Download Options             | Command Line Config Flag            | Description                                                                              | Default Value |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------------------|---------------|
| `BULK_WAIT_TIME`             | `--bulk-wait-time`                  | The wait time between track downloads, in seconds                                        | 1             |
| `DOWNLOAD_REAL_TIME`         | `-rt`, `--download-real-time`       | Downloads songs as fast as they would be played, should prevent account bans             | False         |
| `TEMP_DOWNLOAD_DIR`          | `-td`, `--temp-download-dir`        | Directory where tracks are temporarily downloaded first, `""` meaning disabled           | `""`          |
| `DOWNLOAD_PARENT_ALBUM`      | `--download-parent-album`           | Download a track's parent album, including itself (uses `OUTPUT_ALBUM` file pattern)     | False         |
| `NO_COMPILATION_ALBUMS`      | `--no-compilation-albums`           | Skip downloading an album if API metadata labels it a compilation (not recommended)      | False         |

| Encoding Options             | Command Line Config Flag            | Description                                                                              | Default Value |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------------------|---------------|
| `DOWNLOAD_FORMAT`            | `--codec`, `--download-format`      | Audio codec of downloads, copy avoids remuxing (aac, fdk_aac, mp3, ogg, opus, vorbis)    | copy          |
| `DOWNLOAD_QUALITY`           | `-q`, `--download-quality`          | Audio quality of downloads, auto selects highest available (normal, high, very_high*)    | auto          |
| `TRANSCODE_BITRATE`          | `-b`, `--bitrate`                   | Overwrite the bitrate for FFMPEG encoding (not recommended)                              |               |

| Archive Options              | Command Line Config Flag            | Description                                                                  | Default Value             |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------|---------------------------|
| `SONG_ARCHIVE_LOCATION`      | `--song-archive-location`           | Directory for storing a global song_archive file         | See [Path Option Parser](#path-option-parser) |
| `DISABLE_SONG_ARCHIVE`       | `--disable-song-archive`            | Disable global song_archive for `SKIP_PREVIOUSLY_DOWNLOADED` checks          | False                     |
| `DISABLE_DIRECTORY_ARCHIVES` | `--disable-directory-archives`      | Disable local song_archive in download directories                           | False                     |
| `SKIP_EXISTING`              | `-ie`, `--skip-existing`            | Skip songs already present in the expected output directory                  | True                      |
| `SKIP_PREVIOUSLY_DOWNLOADED` | `-ip`, `--skip-prev-downloaded`     | Use the global song_archive file to skip previously downloaded songs         | False                     |

| Playlist File Config Key     | Command Line Config Flag            | Description                                                                  | Default Value             |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------|---------------------------|
| `EXPORT_M3U8`                | `-e`, `--export-m3u8`               | Export tracks/albums/episodes/playlists with an accompanying .m3u8 file      | False                     |
| `M3U8_LOCATION`              | `--m3u8-location`                   | Directory where .m3u8 files are saved, `""` being the output directory       | `""`                      |
| `M3U8_REL_PATHS`             | `--m3u8-relative-paths`             | List .m3u8 track paths relative to the .m3u8 file's directory                | True                      |
| `LIKED_SONGS_ARCHIVE_M3U8`   | `--liked-songs-archive-m3u8`        | Use cumulative/archiving method when exporting .m3u8 file for Liked Songs    | True                      |

| Lyric File Options           | Command Line Config Flag            | Description                                                                  | Default Value             |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------|---------------------------|
| `DOWNLOAD_LYRICS`            | `--download-lyrics`                 | Whether lyrics should be downloaded (synced, with unsynced as fallback)      | True                      |
| `LYRICS_LOCATION`            | `--lyrics-location`                 | Directory where .lrc files are saved, `""` being the output directory        | `""`                      |
| `ALWAYS_CHECK_LYRICS`        | `--always-check-lyrics`             | Always try to download a song's lyrics, even if skipping the song            | False                     |

| Metadata Options             | Command Line Config Flag            | Description                                                                              | Default Value |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------------------|---------------|
| `LANGUAGE`                   | `--language`                        | Language in which metadata/tags are requested                                            | en            |
| `MD_DISC_TRACK_TOTALS`       | `--md-disc-track-totals`            | Whether track totals and disc totals should be saved in metadata                         | True          |
| `MD_SAVE_GENRES`             | `--md-save-genres`                  | Whether genres should be saved in metadata                                               | False         |
| `MD_ALLGENRES`               | `--md-allgenres`                    | Save all relevant genres in metadata                                                     | False         |
| `MD_GENREDELIMITER`          | `--md-genredelimiter`               | Delimiter character to split genres in metadata, use `""` if array-like tags desired     | `", "`        |
| `MD_ARTISTDELIMITER`         | `--md-artistdelimiter`              | Delimiter character to split artists in metadata, use `""` if array-like tags desired    | `", "`        |
| `MD_SAVE_LYRICS`             | `--md-save-lyrics`                  | Whether lyrics should be saved in metadata, requires `--download-lyrics` be True         | True          |
| `ALBUM_ART_JPG_FILE`         | `--album-art-jpg-file`              | Save album art as a separate .jpg file                                                   | False         |

| API Options                  | Command Line Config Flag            | Description                                                                  | Default Value             |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------|---------------------------|
| `RETRY_ATTEMPTS`             | `--retry-attempts`                  | Number of times to retry failed API requests                                 | 1                         |
| `CHUNK_SIZE`                 | `--chunk-size`                      | Chunk size for downloading                                                   | 20000                     |
| `OAUTH_ADDRESS`              | `--redirect-uri`                    | Local server address listening for OAuth login requests                      | 0.0.0.0                   |
| `REDIRECT_ADDRESS`           | `--redirect-address`                | Local callback point for OAuth login requests                                | 127.0.0.1                 |

| Terminal & Logging Options   | Command Line Config Flag            | Description                                                                  | Default Value             |
|------------------------------|-------------------------------------|------------------------------------------------------------------------------|---------------------------|
| `PRINT_SPLASH`               | `--print-splash`                    | Show the Zotify logo at startup                                              | False                     |
| `PRINT_PROGRESS_INFO`        | `--print-progress-info`             | Show message contianing download progress information                        | True                      |
| `PRINT_SKIPS`                | `--print-skips`                     | Show message when a track is skipped                                         | True                      |
| `PRINT_DOWNLOADS`            | `--print-downloads`                 | Show message when a track is downloaded successfully                         | True                      |
| `PRINT_DOWNLOAD_PROGRESS`    | `--print-download-progress`         | Show track download progress bar                                             | True                      |
| `PRINT_URL_PROGRESS`         | `--print-url-progress`              | Show url progress bar                                                        | True                      |
| `PRINT_ALBUM_PROGRESS`       | `--print-album-progress`            | Show album progress bar                                                      | True                      |
| `PRINT_ARTIST_PROGRESS`      | `--print-artist-progress`           | Show artist progress bar                                                     | True                      |
| `PRINT_PLAYLIST_PROGRESS`    | `--print-playlist-progress`         | Show playlist progress bar                                                   | True                      |
| `PRINT_WARNINGS`             | `--print-warnings`                  | Show warnings                                                                | True                      |
| `PRINT_ERRORS`               | `--print-errors`                    | Show errors                                                                  | True                      |
| `PRINT_API_ERRORS`           | `--print-api-errors`                | Show API errors                                                              | True                      |
| `FFMPEG_LOG_LEVEL`           | `--ffmpeg-log-level`                | FFMPEG's logged level of detail when completing a transcoded download        | error                     |

\* very_high (320k) is limited to Premium accounts only  

</details>

## Configuration Files

Using the `-c` (`--config-location`) flag does not set an alternate config location permanently. Alternate config locations must be specified in the command line each time Zotify is run. When unspecified, the configuration file will be read from and saved to the following default locations based on your operating system:

| OS              | Location                                                           |
|-----------------|--------------------------------------------------------------------|
| Windows         | `C:\Users\<USERNAME>\AppData\Roaming\Zotify\config.json`           |
| MacOS           | `/Users/<USERNAME>/Library/Application Support/Zotify/config.json` |
| Linux           | `/home/<USERNAME>/.config/zotify/config.json`                      |

To log out, just remove the configuration file and credentials file. Uninstalling Zotify does ***not*** remove either.

## Path Option Parser

All pathing-related options (`CREDENTIALS_LOCATION`, `ROOT_PODCAST_PATH`, `TEMP_DOWNLOAD_DIR`, `SONG_ARCHIVE_LOCATION`, `M3U8_LOCATION`, `LYRICS_LOCATION`) accept absolute paths.
They will substitute an initial `"."` with `ROOT_PATH` and properly expand both `"~"` & `"~user"` constructs.

The options `CREDENTIALS_LOCATION` and `SONG_ARCHIVE_LOCATION` use the following default locations depending on operating system:

| OS              | Location                                                |
|-----------------|---------------------------------------------------------|
| Windows         | `C:\Users\<USERNAME>\AppData\Roaming\Zotify\`           |
| MacOS           | `/Users/<USERNAME>/Library/Application Support/Zotify/` |
| Linux           | `/home/<USERNAME>/.local/share/zotify/`                 |

## Output Formats

With the option `OUTPUT` (or the commandline parameter `--output`) you can specify the pattern for the file structure of downloaded songs (not podcasts).  
The value is relative to the `ROOT_PATH` directory and may contain the following placeholders:

| Placeholder       | Description                                                  |
|-------------------|--------------------------------------------------------------|
| `{artist}`        | The song artist                                              |
| `{album_artist}`  | The album artist                                             |
| `{album}`         | The song album                                               |
| `{song_name}`     | The song name                                                |
| `{release_year}`  | The song release year                                        |
| `{disc_number}`   | The disc number                                              |
| `{track_number}`  | The track number                                             |
| `{id}`            | The song id                                                  |
| `{track_id}`      | The track id                                                 |
| `{album_id}`      | (only when downloading albums) ID of the album               |
| `{album_num}`     | (only when downloading albums) Incrementing track number     |
| `{playlist}`      | (only when downloading playlists) Name of the playlist       |
| `{playlist_num}`  | (only when downloading playlists) Incrementing track number  |

### Example Values

`OUTPUT_PLAYLIST`       :   `{playlist}/{artist}_{song_name}`

`OUTPUT_PLAYLIST_EXT`   :   `{playlist}/{playlist_num}_{artist}_{song_name}`

`OUTPUT_LIKED_SONGS`    :   `Liked Songs/{artist}_{song_name}`

`OUTPUT_SINGLE`         :   `{artist}/{album}/{artist}_{song_name}`

`OUTPUT_ALBUM`          :   `{album_artist}/{album}/{album_num}_{artist}_{song_name}`

## Docker Usage

### Build the docker image from the Dockerfile

`docker build -t zotify .`

### Create and run a container from the image

`docker run --rm -p 4381:4381 -v "$PWD/Zotify Music:/root/Music/Zotify Music" -v "$PWD/Zotify Podcasts:/root/Music/Zotify Podcasts" -it zotify`

## Common Questions

<details><summary>

### What do I do if I see "Your session has been terminated"?

</summary>

If you see this, don't worry! Just try logging back in. If you see the incorrect username or token error, delete your `credentials.json` and you should be able to log back in.

</details>

<details><summary>

### What do I do if I see repeated "Failed fetching audio key!" errors?

</summary>

If you see this, don't worry! Recent API changes have introduced rate limits, where requests for track info or audio streams may be rejected if too many requests are sent in a short time period. This can be mitigated by enabling `DOWNLOAD_REAL_TIME` and/or setting a nonzero `BULK_WAIT_TIME`. A recommended `BULK_WAIT_TIME` of `30` seconds has been shown to significantly minimize, if not completely negate, audio key request denials (see [this analysis by HxDxRx](https://github.com/zotify-dev/zotify/issues/186#issuecomment-2608381052))

</details>

<details><summary>

### Will my account get banned if I use this tool?

</summary>

Currently no user has reported their account getting banned after using Zotify.

It is recommended you use Zotify with a burner account.
Alternatively, there is a configuration option labeled `DOWNLOAD_REAL_TIME`, this limits the download speed to the duration of the song being downloaded thus appearing less suspicious.
This option is much slower and is only recommended for premium users who wish to download songs in 320kbps without buying premium on a burner account.

</details>

## Disclaimer

Zotify is intended to be used in compliance with DMCA, Section 1201, for educational, private and fair use. \
Zotify contributors are not responsible for any misuse of the program or source code.

## Contributing

Please refer to [CONTRIBUTING](CONTRIBUTING.md)
