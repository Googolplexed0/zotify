# Metadata musictag keys
ALBUMARTIST = 'albumartist'
ARTWORK = 'artwork'
DISCNUMBER = 'discnumber'
GENRE = 'genre'
LYRICS = 'lyrics'
TOTALDISCS = 'totaldiscs'
TOTALTRACKS = 'totaltracks'
TRACKNUMBER = 'tracknumber'
TRACKTITLE = 'tracktitle'
YEAR = 'year'
MP3_CUSTOM_TAG_PREFIX = 'TXXX:'
M4A_CUSTOM_TAG_PREFIX = '----:com.apple.iTunes:'

# Both
ALBUM = 'album'
ARTIST = 'artist'
COMPILATION = 'compilation'

# API Dictionary Keys
ADDED_AT = 'added_at'
ADDED_BY = 'added_by'
ALBUMS = 'albums'
ALBUM_ARTISTS = 'album_artists'
ALBUM_TYPE = 'album_type'
AUDIO = 'audio'
AVAIL_MARKETS = 'available_markets'
ARTISTS = 'artists'
ARTIST_IDS = 'artist_ids'
AUDIOBOOK = 'audiobook'
CHAPTERS = 'chapters'
COLLABORATIVE = 'collaborative'
DATA = 'data'
DESCRIPTION = 'description'
DISC_NUMBER = 'disc_number'
DISPLAY_NAME = 'display_name'
DURATION_MS = 'duration_ms'
ERROR = 'error'
EPISODE = 'episode'
EPISODES = 'episodes'
EXPLICIT = 'explicit'
EXTERNAL_URLS = 'external_urls'
FOLLOWERS = 'followers'
GENRES = 'genres'
HREF = 'href'
ID = 'id'
IMAGES = 'images'
IMAGE_URL = 'image_url'
IS_EXTERNALLY_HOSTED = 'is_externally_hosted'
IS_LOCAL = 'is_local'
IS_PLAYABLE = 'is_playable'
ITEMS = 'items'
LABEL = 'label'
LINES = 'lines'
LINE_SYNCED = 'LINE_SYNCED'
LIMIT = 'limit'
NAME = 'name'
NEXT = 'next'
OFFSET = 'offset'
OWNER = 'owner'
PLAYLIST = 'playlist'
PLAYLISTS = 'playlists'
PREMIUM = 'premium'
PREVIEW_URL = 'preview_url'
PUBLIC = 'public'
PUBLISHER = 'publisher'
RELEASE_DATE = 'release_date'
SHOW = 'show'
SHOWS = 'shows'
STARTTIMEMS = 'startTimeMs'
SNAPSHOT_ID = 'snapshot_id'
SYNCTYPE = 'syncType'
TOTAL = 'total'
TOTAL_EPISODES = 'total_episodes'
TOTAL_TRACKS = 'total_tracks'
TRACK = 'track'
TRACKS = 'tracks'
TRACK_NUMBER = 'track_number'
TYPE = 'type'
UNSYNCED = 'UNSYNCED'
URL = 'url'
URI = 'uri'
WIDTH = 'width'
WORDS = 'words'

# API URLs
BASE_URL = 'https://api.sp' + 'otify.com/v1/'
BULK_APPEND = 'ids='
MARKET_APPEND = 'market=from_token'
ALBUM_URL = BASE_URL + ALBUMS
ALBUM_BULK_URL = ALBUM_URL + '?' + BULK_APPEND
ARTIST_URL = BASE_URL + ARTISTS
ARTIST_BULK_URL = ARTIST_URL + '?' + BULK_APPEND
AUDIOBOOK_URL = BASE_URL + AUDIOBOOK
CHAPTER_URL = BASE_URL + CHAPTERS
EPISODE_URL = BASE_URL + EPISODES
PLAYLIST_URL = BASE_URL + PLAYLISTS
SEARCH_URL = BASE_URL + 'search'
SHOW_URL = BASE_URL + SHOWS
TRACK_URL = BASE_URL + TRACKS
TRACK_BULK_URL = TRACK_URL + '?' + BULK_APPEND
TRACK_STATS_URL = BASE_URL + 'audio-features/'
USER_URL = BASE_URL + 'me/'
USER_FOLLOWED_ARTISTS_URL = USER_URL + 'following?type=' + ARTIST
USER_PLAYLISTS_URL = USER_URL + PLAYLISTS
USER_SAVED_TRACKS_URL = USER_URL + TRACKS
USER_SAVED_ALBUMS_URL = USER_URL + ALBUMS
LYRICS_URL = 'https://spclient.wg.sp' + 'otify.com/color-lyrics/v2/track/'
PARTNER_URL = 'https://api-partner.sp' + 'otify.com/pathfinder/v1/query?operationName=getEpisode&variables={"uri":"sp' + 'otify:episode:'
PERSISTED_QUERY = '{"persistedQuery":{"version":1,"sha256Hash":"224ba0fd89fcfdfb'+'3a15fa2d82a6112d'+'3f4e2ac88fba5c67'+'13de04d1b72cf482"}}'

# API Scopes
PLAYLIST_READ_PRIVATE = 'playlist-read-private'
USER_FOLLOW_READ = 'user-follow-read'
USER_LIBRARY_READ = 'user-library-read'
USER_READ_EMAIL = 'user-read-email'
SCOPES = [
    "app-remote-control",
    "playlist-modify",
    "playlist-modify-private",
    "playlist-modify-public",
    "playlist-read",
    "playlist-read-collaborative",
    "playlist-read-private",
    "streaming",
    "ugc-image-upload",
    "user-follow-modify",
    "user-follow-read",
    "user-library-modify",
    "user-library-read",
    "user-modify",
    "user-modify-playback-state",
    "user-modify-private",
    "user-personalized",
    "user-read-birthdate",
    "user-read-currently-playing",
    "user-read-email",
    "user-read-play-history",
    "user-read-playback-position",
    "user-read-playback-state",
    "user-read-private",
    "user-read-recently-played",
    "user-top-read",
]

# System Constants
LINUX_SYSTEM = 'Linux'
WINDOWS_SYSTEM = 'Windows'

# FFMPEG
CODEC_MAP = {
    'aac': 'aac',
    'fdk_aac': 'libfdk_aac',
    'mp3': 'libmp3lame',
    'ogg': 'copy',
    'opus': 'libopus',
    'vorbis': 'copy',
    'copy': 'copy'
}
EXT_MAP = {
    'aac': 'm4a',
    'fdk_aac': 'm4a',
    'mp3': 'mp3',
    'ogg': 'ogg',
    'opus': 'ogg',
    'vorbis': 'ogg',
    'copy': 'ogg'
}

# Config Keys
MANDATORY = 'MANDATORY'
DEBUG = "DEBUG"
ROOT_PATH = 'ROOT_PATH'
ROOT_PODCAST_PATH = 'ROOT_PODCAST_PATH'
SKIP_EXISTING = 'SKIP_EXISTING'
SKIP_PREVIOUSLY_DOWNLOADED = 'SKIP_PREVIOUSLY_DOWNLOADED'
DOWNLOAD_FORMAT = 'DOWNLOAD_FORMAT'
BULK_WAIT_TIME = 'BULK_WAIT_TIME'
CHUNK_SIZE = 'CHUNK_SIZE'
SPLIT_ALBUM_DISCS = 'SPLIT_ALBUM_DISCS'
DOWNLOAD_REAL_TIME = 'DOWNLOAD_REAL_TIME'
LANGUAGE = 'LANGUAGE'
DOWNLOAD_QUALITY = 'DOWNLOAD_QUALITY'
TRANSCODE_BITRATE = 'TRANSCODE_BITRATE'
SONG_ARCHIVE_LOCATION = 'SONG_ARCHIVE_LOCATION'
SAVE_CREDENTIALS = 'SAVE_CREDENTIALS'
CREDENTIALS_LOCATION = 'CREDENTIALS_LOCATION'
OUTPUT = 'OUTPUT'
PRINT_SPLASH = 'PRINT_SPLASH'
PRINT_SKIPS = 'PRINT_SKIPS'
PRINT_DOWNLOAD_PROGRESS = 'PRINT_DOWNLOAD_PROGRESS'
PRINT_ERRORS = 'PRINT_ERRORS'
PRINT_DOWNLOADS = 'PRINT_DOWNLOADS'
PRINT_API_ERRORS = 'PRINT_API_ERRORS'
TEMP_DOWNLOAD_DIR = 'TEMP_DOWNLOAD_DIR'
MD_DISC_TRACK_TOTALS = "MD_DISC_TRACK_TOTALS"
MD_SAVE_GENRES = 'MD_SAVE_GENRES'
MD_ALLGENRES = 'MD_ALLGENRES'
MD_GENREDELIMITER = 'MD_GENREDELIMITER'
MD_ARTISTDELIMITER = 'MD_ARTISTDELIMITER'
MD_SAVE_LYRICS = 'MD_SAVE_LYRICS'
PRINT_PROGRESS_INFO = 'PRINT_PROGRESS_INFO'
PRINT_WARNINGS = 'PRINT_WARNINGS'
RETRY_ATTEMPTS = 'RETRY_ATTEMPTS'
CONFIG_VERSION = 'CONFIG_VERSION'
DOWNLOAD_LYRICS = 'DOWNLOAD_LYRICS'
OUTPUT_PLAYLIST = 'OUTPUT_PLAYLIST'
OUTPUT_PLAYLIST_EXT = 'OUTPUT_PLAYLIST_EXT'
OUTPUT_LIKED_SONGS = 'OUTPUT_LIKED_SONGS'
OUTPUT_SINGLE = 'OUTPUT_SINGLE'
OUTPUT_ALBUM = 'OUTPUT_ALBUM'
DISABLE_DIRECTORY_ARCHIVES = 'DISABLE_DIRECTORY_ARCHIVES'
LYRICS_LOCATION = 'LYRICS_LOCATION'
FFMPEG_LOG_LEVEL = 'FFMPEG_LOG_LEVEL'
PRINT_URL_PROGRESS = 'PRINT_URL_PROGRESS'
PRINT_ALBUM_PROGRESS = 'PRINT_ALBUM_PROGRESS'
PRINT_ARTIST_PROGRESS = 'PRINT_ARTIST_PROGRESS'
PRINT_PLAYLIST_PROGRESS = 'PRINT_PLAYLIST_PROGRESS'
EXPORT_M3U8 = 'EXPORT_M3U8'
LIKED_SONGS_ARCHIVE_M3U8 = 'LIKED_SONGS_ARCHIVE_M3U8'
ALBUM_ART_JPG_FILE = 'ALBUM_ART_JPG_FILE'
MAX_FILENAME_LENGTH = 'MAX_FILENAME_LENGTH'
ALWAYS_CHECK_LYRICS = 'ALWAYS_CHECK_LYRICS'
M3U8_LOCATION = 'M3U8_LOCATION'
M3U8_REL_PATHS = 'M3U8_REL_PATHS'
DOWNLOAD_PARENT_ALBUM = 'DOWNLOAD_PARENT_ALBUM'
DISABLE_SONG_ARCHIVE = 'DISABLE_SONG_ARCHIVE'
REDIRECT_ADDRESS = 'REDIRECT_ADDRESS'
NO_COMPILATION_ALBUMS = 'NO_COMPILATION_ALBUMS'
REGEX_ENABLED = 'REGEX_ENABLED'
REGEX_TRACK_SKIP = 'REGEX_TRACK_SKIP'
REGEX_EPISODE_SKIP = 'REGEX_EPISODE_SKIP'
REGEX_ALBUM_SKIP = 'REGEX_ALBUM_SKIP'
LYRICS_MD_HEADER = 'LYRICS_MD_HEADER'
STRICT_LIBRARY_VERIFY = 'STRICT_LIBRARY_VERIFY'
