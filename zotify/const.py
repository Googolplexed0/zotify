# Metadata musictag keys
ALBUMARTIST = 'albumartist'
ARTWORK = 'artwork'
DISCNUMBER = 'discnumber'
TOTALDISCS = 'totaldiscs'
TOTALTRACKS = 'totaltracks'
TRACKID = 'trackid'
TRACKNUMBER = 'tracknumber'
TRACKTITLE = 'tracktitle'
MP3_CUSTOM_TAG_PREFIX = 'TXXX:'
M4A_CUSTOM_TAG_PREFIX = '----:com.apple.iTunes:'

# Both
ALBUM = 'album'
ARTIST = 'artist'
COMPILATION = 'compilation'
GENRE = 'genre'
LYRICS = 'lyrics'
YEAR = 'year'

# API Dictionary Keys
ACTIVITY_PERIOD = 'activity_period'
ADDED_AT = 'added_at'
ADDED_BY = 'added_by'
ALBUMS = 'albums'
ALBUM_ARTISTS = 'album_artists'
ALBUM_GROUP = 'album_group'
ALBUM_TYPE = 'album_type'
ALTERNATIVE = 'alternative'
APPEARS_ON = 'appears_on'
APPEARS_ON_GROUP = 'appears_on_group'
ARTISTS = 'artists'
ARTIST_IDS = 'artist_ids'
ATTRIBUTES = 'attributes'
AUDIO = 'audio'
AUDIOBOOK = 'audiobook'
AVAIL_MARKETS = 'available_markets'
BIOGRAPHY = 'biography'
CHAPTERS = 'chapters'
COLLABORATIVE = 'collaborative'
CONTENTS = 'contents'
COVER_GROUP = 'cover_group'
DATA = 'data'
DATE = 'date'
DAY = 'day'
DELETED_BY_OWNER = 'deleted_by_owner'
DESCRIPTION = 'description'
DISC = 'disc'
DISC_NUMBER = 'disc_number'
DISPLAY_NAME = 'display_name'
DURATION = 'duration'
DURATION_MS = 'duration_ms'
END_YEAR = 'end_year'
ERROR = 'error'
EPISODE = 'episode'
EPISODES = 'episodes'
EXPLICIT = 'explicit'
EXTERNAL_URL = 'external_url'
EXTERNAL_URLS = 'external_urls'
FILE = 'file'
FILE_ID = 'file_id'
FOLLOWERS = 'followers'
FORMAT = 'format'
GENRES = 'genres'
GID = 'gid'
HOUR = 'hour'
HREF = 'href'
ID = 'id'
IMAGE = 'image'
IMAGES = 'images'
IMAGE_URL = 'image_url'
INCLUDE_EXTERNAL = 'include_external'
IS_EXTERNALLY_HOSTED = 'is_externally_hosted'
IS_LOCAL = 'is_local'
IS_PLAYABLE = 'is_playable'
ITEM = 'item'
ITEMS = 'items'
ITEM_ID = 'item_id'
LABEL = 'label'
LENGTH = 'length'
LINES = 'lines'
LINE_SYNCED = 'LINE_SYNCED'
LIMIT = 'limit'
MESSAGE = 'message'
MINUTE = 'minute'
MONTH = 'month'
NAME = 'name'
NEXT = 'next'
NUMBER = 'number'
OFFSET = 'offset'
OWNER = 'owner'
OWNER_USERNAME = 'owner_username'
PLAYLIST = 'playlist'
PLAYLISTS = 'playlists'
POPULARITY = 'popularity'
PREMIUM = 'premium'
PREVIEW_URL = 'preview_url'
PUBLIC = 'public'
PUBLISHER = 'publisher'
PUBLISH_TIME = 'publish_time'
RELEASE_DATE = 'release_date'
REVISION = 'revision'
SINGLES = 'singles'
SINGLE_GROUP = 'single_group'
SHOW = 'show'
SHOWS = 'shows'
STARTTIMEMS = 'startTimeMs'
START_YEAR = 'start_year'
STATUS = 'status'
SNAPSHOT_ID = 'snapshot_id'
SYNCTYPE = 'syncType'
TEXT = 'text'
TIMESTAMP = 'timestamp'
TOP_TRACK = 'top_track'
TOP_TRACKS = 'top-tracks'
TOTAL = 'total'
TOTAL_EPISODES = 'total_episodes'
TOTAL_TRACKS = 'total_tracks'
TRACK = 'track'
TRACKS = 'tracks'
TRACK_NUMBER = 'track_number'
TRUNCATED = 'truncated'
TYPE = 'type'
UNSYNCED = 'UNSYNCED'
URL = 'url'
URI = 'uri'
USER = 'user'
WIDTH = 'width'
WORDS = 'words'

# API URLs
BASE_URL = 'https://api.sp' + 'otify.com/v1/'
BULK_APPEND = 'ids='
MARKET_APPEND = 'market=from_token'
ALBUM_URL = BASE_URL + ALBUMS
ARTIST_URL = BASE_URL + ARTISTS
AUDIOBOOK_URL = BASE_URL + AUDIOBOOK
CHAPTER_URL = BASE_URL + CHAPTERS
EPISODE_URL = BASE_URL + EPISODES
PLAYLIST_URL = BASE_URL + PLAYLISTS
SEARCH_URL = BASE_URL + 'search'
SHOW_URL = BASE_URL + SHOWS
TRACK_URL = BASE_URL + TRACKS
TRACK_STATS_URL = BASE_URL + 'audio-features/'
USER_URL = BASE_URL + 'me/'
USER_FOLLOWED_ARTISTS_URL = USER_URL + f'following?{TYPE}=' + ARTIST
USER_PLAYLISTS_URL = USER_URL + PLAYLISTS
USER_SAVED_TRACKS_URL = USER_URL + TRACKS
USER_SAVED_ALBUMS_URL = USER_URL + ALBUMS
IMAGE_URL_PREFIX = 'https://i.sc' + f'dn.co/{IMAGE}/'
LYRICS_URL = 'https://spc' + 'lient.wg.sp' + f'otify.com/color-lyrics/v2/{TRACK}/'
PARTNER_URL = 'https://api-partner.sp' + 'otify.com/pathfinder/v1/query?operationName=getEpisode&variables={"uri":"sp' + f'otify:{EPISODE}:'
PERSISTED_QUERY = '{"persistedQuery":{"version":1,"sha256Hash":"224ba0fd89fcfdfb'+'3a15fa2d82a6112d'+'3f4e2ac88fba5c67'+'13de04d1b72cf482"}}'
STREAMABLE_PODCAST = 'anon-podcast.sc' + 'dn.co'

# API Scopes
SCOPES = [
    'streaming',
    'playlist-read-private',
    'playlist-read-collaborative',
    'user-follow-read',
    'user-read-playback-position',
    'user-top-read',
    'user-read-recently-played',
    'user-library-read',
    'user-read-email',
    'user-read-private']

# System Constants
LINUX_SYSTEM = 'Linux'
WINDOWS_SYSTEM = 'Windows'

# FFMPEG
CODEC_MAP_TRACK = {
    'aac': 'aac',
    'fdk_aac': 'libfdk_aac',
    'mp3': 'libmp3lame',
    'ogg': 'copy',
    'opus': 'libopus',
    'vorbis': 'copy',
    'copy': 'copy'
}
CODEC_MAP_EPISODE = {
    'aac': 'aac',
    'fdk_aac': 'libfdk_aac',
    'mp3': 'libmp3lame',
    'ogg': 'libvorbis',
    'opus': 'libopus',
    'vorbis': 'libvorbis',
    'copy': 'copy'
}
EXT_MAP = {
    'aac': 'm4a',
    'fdk_aac': 'm4a',
    'mp3': 'mp3',
    'ogg': 'ogg',
    'opus': 'ogg',
    'vorbis': 'ogg',
}

# Config Keys
MANDATORY = 'MANDATORY'
DEBUG = 'DEBUG'
ROOT_PATH = 'ROOT_PATH'
ROOT_PODCAST_PATH = 'ROOT_PODCAST_PATH'
SKIP_EXISTING = 'SKIP_EXISTING'
SKIP_PREVIOUSLY_DOWNLOADED = 'SKIP_PREVIOUSLY_DOWNLOADED'
DOWNLOAD_FORMAT = 'DOWNLOAD_FORMAT'
BULK_WAIT_TIME = 'BULK_WAIT_TIME'
CHUNK_SIZE = 'CHUNK_SIZE'
SPLIT_ALBUM_DISCS = 'SPLIT_ALBUM_DISCS'
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
MD_DISC_TRACK_TOTALS = 'MD_DISC_TRACK_TOTALS'
MD_SAVE_GENRES = 'MD_SAVE_GENRES'
MD_ALLGENRES = 'MD_ALLGENRES'
MD_GENREDELIMITER = 'MD_GENREDELIMITER'
MD_ARTISTDELIMITER = 'MD_ARTISTDELIMITER'
PRINT_PROGRESS_INFO = 'PRINT_PROGRESS_INFO'
PRINT_WARNINGS = 'PRINT_WARNINGS'
RETRY_ATTEMPTS = 'RETRY_ATTEMPTS'
CONFIG_VERSION = 'CONFIG_VERSION'
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
OPTIMIZED_DOWNLOADING = 'OPTIMIZED_DOWNLOADING'
UPDATE_ARCHIVE = 'UPDATE_ARCHIVE'
SEARCH_QUERY_SIZE = 'SEARCH_QUERY_SIZE'
CUSTOM_FFMEPG_ARGS = 'CUSTOM_FFMEPG_ARGS'
STANDARD_INTERFACE = 'STANDARD_INTERFACE'
LYRICS_TO_METADATA = 'LYRICS_TO_METADATA'
LYRICS_TO_FILE = 'LYRICS_TO_FILE'
OUTPUT_LYRICS = 'OUTPUT_LYRICS'
NO_ARTIST_APPEARS_ON = 'NO_ARTIST_APPEARS_ON'
NO_VARIOUS_ARTISTS = 'NO_VARIOUS_ARTISTS'
DISCOG_BY_ALBUM_ARTIST = 'DISCOG_BY_ALBUM_ARTIST'
API_CLIENT_ID = 'API_CLIENT_ID'
DOWNLOAD_RATE_LIMITER = 'DOWNLOAD_RATE_LIMITER'