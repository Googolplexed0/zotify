import json
from base64 import b64encode, b64decode
from music_tag import AudioFile, load_file
from music_tag.file import TAG_MAP_ENTRY, MetadataItem
from music_tag.mp4 import freeform_set
from mutagen.id3 import TXXX

from zotify.api import *


class MetadataIO:
    PARSE_AS_STR        = {ADDED_AT, ALBUM_TYPE, DESCRIPTION, DISC_NUMBER, DISPLAY_NAME, EXTERNAL_URL,
                           ID, ITEM_ID, LABEL, NAME, PUBLISHER, RELEASE_DATE, REVISION, SNAPSHOT_ID,}
    INT_PARSE_AS_STR    = {TOTAL_EPISODES, TOTAL_TRACKS, TRACK_NUMBER,}
    PARSE_AS_INT        = {DURATION_MS, LENGTH, POPULARITY, TIMESTAMP,}
    PARSE_AS_BOOL       = {COLLABORATIVE, DELETED_BY_OWNER, EXPLICIT,
                           IS_EXTERNALLY_HOSTED, IS_LOCAL, IS_PLAYABLE, PUBLIC,}
    
    def from_resp(self, obj: Content, relative: Content, resp: dict) -> dict[str]:
        for attr in self.PARSE_AS_STR:
            setattr(self, attr, safe_typecast(resp, attr, str))
        for attr in self.INT_PARSE_AS_STR:
            raw_val: str | None = safe_typecast(resp, attr, str)
            setattr(self, attr, None if raw_val is None else raw_val.zfill(2))
        for attr in self.PARSE_AS_INT:
            setattr(self, attr, safe_typecast(resp, attr, int))
        for attr in self.PARSE_AS_BOOL:
            setattr(self, attr, safe_typecast(resp, attr, bool))
        self.external_urls          : dict              = resp.get(EXTERNAL_URLS)
        self.gid                    : bytes             = resp.get(GID)
        self.genres                 : list[str]         = resp.get(GENRES)
        # self.lyrics               : list[str]         = resp.get(LYRICS)
        
        def ensure_uri(item: dict | None, type_attr_and_ind: str):
            if item is None: return
            gid = item.get(GID);  uri = item.get(URI)
            name = item.get(NAME); typ = item.get(TYPE)
            
            # handle missing TYPE
            if not typ: item[TYPE] = type_attr_and_ind.lower().strip("0123456789")
            
            # handle METADATA_PREFETCH
            if gid and not uri:
                uri = f":{item[TYPE]}:{Zotify.id_from_gid(gid)}"
                self._needs_recursion = True
            
            # handle local files
            if not name: name = f"noname-{uuid4()}"
            if not uri:  uri = f":local:{type_attr_and_ind.lower()}:{name}:::"
            item[URI] = uri
        
        def ensure_user_resp(username: str | None) -> dict | None:
            if not username: return None
            return {URI         : f":{USER}:{username}",
                    TYPE        : USER,
                    DISPLAY_NAME: User.fetch_display_name(username)}
        
        activity_period             : list[dict]        = resp.get(ACTIVITY_PERIOD)
        if activity_period:
            periods = {k: v for period in activity_period for k, v in period.items()}
            self.start_year         : str               = safe_typecast(periods, START_YEAR, str)
            self.end_year           : str               = safe_typecast(periods, END_YEAR, str)
        
        added_by                    : dict              = resp.get(ADDED_BY)
        if added_by:
            self.added_by           : User              = obj.parse_relatives([added_by], User, make_parent=True)[0]
        
        album                       : dict              = resp.get(ALBUM)
        if isinstance(obj, Track) and isinstance(relative, Album):
            self.album              : Album             = relative
        elif album:
            ensure_uri(album, ALBUM)
            parent = isinstance(obj, Track)
            self.album              : Album             = obj.parse_relatives([album], Album, make_parent=parent)[0]
        
        album_group                 : list[dict] | str  = resp.get(ALBUM_GROUP)
        if album_group:
            if isinstance(obj, Artist):
                album_entries = [a[ALBUM][0] for a in album_group if a.get(ALBUM)]
                for a in album_entries:                 ensure_uri(a, ALBUM)
                self.albums         : list[Album]       = obj.parse_relatives(album_entries, Album)
            elif isinstance(obj, Album):
                self.album_group    : str               = safe_typecast(resp, attr, str)
                self._needs_expansion = True
        
        appears_on                  : list[dict]        = resp.get(APPEARS_ON_GROUP)
        if appears_on:
            appears_entries = [a[ALBUM][0] for a in appears_on if a.get(ALBUM)]
            for a in appears_entries:                   ensure_uri(a, ALBUM)
            self.appears_on         : list[Album]       = obj.parse_relatives(appears_entries, Album)
        
        artist                      : list[dict]        = resp.get(ARTIST)
        artists                     : list[dict]        = resp.get(ARTISTS)
        if artist or artists:
            artists = artist if artist else artists
            parent = isinstance(obj, (Track, Album))
            for i, a in enumerate(artists):             ensure_uri(a, ARTIST + str(i+1))
            self.artists            : list[Artist]      = obj.parse_relatives(artists, Artist, make_parent=parent)
        
        audio                       : list[dict]        = resp.get(AUDIO)
        files                       : list[dict]        = resp.get(FILE)
        alternatives                : list[dict]        = resp.get(ALTERNATIVE)
        if any((audio, files, alternatives)):
            files = files if files is not None else audio
            if not files and alternatives:
                for alt in alternatives:
                    files = alt.get(FILE)
                    if files: break
            if files:
                self.is_playable = True
                self.file_ids = files
        
        biography                   : list[dict]        = resp.get(BIOGRAPHY)
        if biography:
            self.biography          : str               = biography[0].get(TEXT)
        
        self.compilation            : bool              = self.album_type == COMPILATION if self.album_type else None
        
        contents                    : dict              = resp.get(CONTENTS) 
        if contents:
            items                   : list[dict]        = contents.get(ITEMS)
            if items:
                for i, item in enumerate(items):
                    attr: dict = item.pop(ATTRIBUTES, None)
                    if attr is None: continue
                    ensure_uri(item, TRACK + str(i+1))
                    item[ADDED_AT] = timestamp_utc(attr.get(TIMESTAMP))
                    item[ADDED_BY] = ensure_user_resp(attr.get(ADDED_BY))
                    item[ITEM_ID] = attr.get(ITEM_ID)
                self.tracks_or_eps = obj.parse_relatives(items, (Track, Episode))
                self._needs_recursion = True
                if contents.get(TRUNCATED):
                    self._needs_expansion = True
                    Printer.hashtaged(PrintChannel.WARNING, f'PLAYLIST {self.name} MISSING FINAL {self.length - len(items)} ITEMS\n' +
                                                                'NOT RECOVERABLE WITHOUT A LEGACY DEVELOPER CLIENT')
        
        cover_group                 : dict              = resp.get(COVER_GROUP)
        images                      : list[dict]        = resp.get(IMAGES)
        if cover_group or images:
            covers = images if images else cover_group.get(IMAGE, [])
            largest_cover           : dict              = max(covers, key=lambda img: safe_typecast(img, WIDTH, int),
                                                            default={URL: None, FILE_ID: None})
            if largest_cover.get(FILE_ID):
                largest_cover[URL] = IMAGE_URL_PREFIX + Zotify.hex_id_from_file_id(largest_cover.get(FILE_ID))
            self.image_url          : str               = largest_cover.get(URL)
        
        date                        : dict              = resp.get(DATE)
        if date and not self.release_date:
            self.release_date       : str               = "-".join(str(v) for v in date.values())
        
        discs                       : list[dict]        = resp.get(DISC)
        if discs:
            track_entries           : list[dict]        = []
            for disc in discs:
                for i, t in enumerate(disc.get(TRACK, [])):
                    ensure_uri(t, TRACK + str(i+1))
                    t[DISC_NUMBER]  = disc.get(NUMBER)
                    t[TRACK_NUMBER] = i + 1
                track_entries.extend(disc.get(TRACK, []))
            resp.update({TRACKS: {ITEMS: track_entries, NEXT: None}})
        
        episodes                    : dict              = resp.get(EPISODES)
        if episodes:
            items                   : list[dict]        = episodes.get(ITEMS)
            if items:
                for i, e in enumerate(items):     ensure_uri(e, EPISODE + str(i+1))
                self.episodes       : list[Episode]     = obj.parse_relatives(items, Episode)
                self._needs_expansion = episodes[NEXT] is not None
            else:
                self._needs_expansion = True
        
        external_id                 : list[dict]        = resp.get(EXTERNAL_ID)
        external_ids                : dict              = resp.get(EXTERNAL_IDS)
        if external_id or external_ids:
            if external_id:
                external_ids = {eid.get(TYPE): eid.get(ID) for eid in external_id}
            self.ean                : str               = external_ids.get(EAN)
            self.isrc               : str               = external_ids.get(ISRC)
            self.upc                : str               = external_ids.get(UPC)
        
        owner_username              : str               = resp.get(OWNER_USERNAME)
        if owner_username:
            resp[OWNER]                                 = ensure_user_resp(owner_username)
        
        owner                       : dict              = resp.get(OWNER)
        if owner:
            self.owner              : User              = obj.parse_relatives([owner], User, make_parent=True)[0]
            self.owner.name                             = self.owner.display_name
        
        playlist_items              : dict              = resp.get(ITEMS)
        if playlist_items and isinstance(obj, Playlist):
            self.length             : int               = resp.get(TOTAL)
            items                   : list[dict]        = playlist_items.get(ITEMS)
            if items:
                tracks_eps_empty = obj.unwrap(items)
                for i, t_or_e in enumerate(tracks_eps_empty):
                    ensure_uri(t_or_e, TRACK + str(obj.ccount+i+1))
                self.tracks_or_eps = obj.parse_relatives(tracks_eps_empty, (Track, Episode))
                if not any(self.tracks_or_eps):
                    Printer.hashtaged(PrintChannel.WARNING,
                                        f'PLAYLIST "{self.name}" ({obj.uri})\n' +
                                        '[Playlist.Items.Items] METADATA ENTIRELY ABSENT\n' +
                                        'RECOMMENDED TO SET CONFIG "API_CLIENT_LEGACY = False"')
            else: # should never be called
                Printer.hashtaged(PrintChannel.WARNING,
                                    f'PLAYLIST "{self.name}" ({obj.uri})\n' +
                                    'HAS [Playlist.Items] BUT NO [Playlist.Items.Items]\n' +
                                    'RECOMMENDED TO SET CONFIG "API_CLIENT_LEGACY = False"')
            self._needs_expansion = not items or playlist_items.get(NEXT) is not None
        
        publish_time                : dict[str, int]    = resp.get(PUBLISH_TIME)
        if publish_time:
            dt = datetime(publish_time.get(YEAR), publish_time.get(MONTH), publish_time.get(DAY),
                            publish_time.get(HOUR, 0), publish_time.get(MINUTE, 0))
            self.publish_time = dt_to_str(dt)
            self.release_date = dt_to_str(dt.date())
        
        show                        : dict              = resp.get(SHOW)
        if isinstance(obj, Episode) and isinstance(relative, Show):
            self.show               : Show              = relative
        elif show:
            ensure_uri(show, SHOW)
            parent = isinstance(obj, Episode)
            self.show               : Show              = obj.parse_relatives([show], Show, make_parent=parent)[0]
        
        singles                     : list[dict]        = resp.get(SINGLE_GROUP)
        if singles:
            single_entries = [a[ALBUM][0] for a in singles if a.get(ALBUM)]
            for a in single_entries:                    ensure_uri(a, ALBUM)
            self.singles            : list[Album]       = obj.parse_relatives(single_entries, Album)
        
        timestamp                   : str               = resp.get(TIMESTAMP)
        if timestamp:
            self.timestamp          : str               = timestamp_utc(timestamp)
        
        followers                   : dict              = resp.get(FOLLOWERS)
        if followers:
            self.followers          : int               = safe_typecast(followers, TOTAL, int)
        
        top_tracks                  : list[dict]        = resp.get(TOP_TRACK)
        if top_tracks:
            track_entries = top_tracks[0].get(TRACK)
            if track_entries:
                for i, t in enumerate(track_entries):   ensure_uri(t, TRACK + str(i+1))
                self.top_tracks     : list[Track]       = obj.parse_relatives(track_entries, Track)
        
        tracks                      : dict              = resp.get(TRACKS)
        if tracks and isinstance(obj, Album):
            items                   : list[dict]        = tracks.get(ITEMS)
            if items:
                for i, t in enumerate(items): ensure_uri(t, TRACK + str(obj.ccount+i+1))
                self.tracks: list[Track] = obj.parse_relatives(items, Track)
                self._needs_expansion = tracks.get(NEXT) is not None
                if not self._needs_expansion:
                    # set in Album.grab_more_children() later if album incomplete
                    self.total_discs = safe_typecast(items[-1], DISC_NUMBER, int)
                    self.duration_ms = sum(int(t.duration_ms) if t.duration_ms else 0 for t in self.tracks)
            else:
                self._needs_expansion = True
        
        self.year                   : str               = self.release_date.split('-')[0] if self.release_date else None
        
        if isinstance(obj, Artist):
            self.all_albums = getattr(self, ALBUMS, []) + getattr(self, SINGLES, []) + getattr(self, APPEARS_ON, [])
        
        return self.__dict__.items()
    
    EXPORT_QUERY: bool = True
    BYTES_HEADER = "</bytes/>"
    SET_ATTRS = {"parents", "children"}
    SKIP_ATTRS = {"ALL_NODES", URI}
    PARSING: dict[str, int | str | dict] = None
    ZMD_V1 = {ZMD_VERSION: 1,
              ZMD_LINK: "<{__class__}@{uri}>",
              ZMD_ENTRIES: {}}
    LATEST_ZMD = ZMD_V1
    
    @classmethod
    def _set_zmd_version(cls, zmd: dict) -> dict[str, int | str | list | dict]:
        # fallbacl to ZMD_V1 if unsure
        cls.PARSING = getattr(cls, f"ZMD_V{zmd.get(ZMD_VERSION, 1)}", cls.ZMD_V1)
        return cls.PARSING
    
    @classmethod
    def _to_link(cls, obj: Content) -> str:
        match cls.PARSING[ZMD_VERSION]:
            case 1:
                return cls.ZMD_V1[ZMD_LINK].format(__class__=obj.__class__.__name__, uri=obj.uri)
    
    @classmethod
    def _from_link(cls, link: str) -> tuple[str, str] | tuple[None, None]:
        match cls.PARSING[ZMD_VERSION]:
            case 1:
                pattern = cls.ZMD_V1[ZMD_LINK]
                if link[0] == pattern[0] and link[-1] == pattern[-1] and "@" in link:
                    return link[1:-1].split("@")
        return None, None
    
    @classmethod
    def _deref(cls, obj: Content | list | dict) -> str | list | dict:
        if isinstance(obj, Content):    return cls._to_link(obj)
        elif isinstance(obj, list):     return [cls._deref(i) for i in obj]
        elif not isinstance(obj, dict): return obj
        
        derefed = {}
        for k, v in obj.items():
            if k[0] == "_" or k in cls.SKIP_ATTRS or not v:         continue
            elif k == ID and v == obj.get(URI, "").split(":")[-1]:  continue
            elif k in cls.SET_ATTRS:                                derefed[k] = [cls._deref(n) for n in v]
            elif isinstance(v, bytes):                              derefed[k] = cls.BYTES_HEADER + b64encode(v).decode('ascii')
            elif isinstance(v, (Content, list, dict)):              derefed[k] = cls._deref(v)
            else:                                                   derefed[k] = v
        return derefed
    
    @classmethod
    def _reref(cls, obj: str | list | dict, dests: dict[str, Content]) -> Content | list | dict:
        if isinstance(obj, str) and obj.startswith(cls.BYTES_HEADER):
            return b64decode(obj.removeprefix(cls.BYTES_HEADER).encode('ascii'))
        if isinstance(obj, str):        return dests.get(obj, obj)
        elif isinstance(obj, list):     return [cls._reref(i, dests) for i in obj]
        elif not isinstance(obj, dict): return obj
        
        rerefed = {}
        for k, v in obj.items():
            if k in cls.SET_ATTRS:                  rerefed[k] = set(cls._reref(v, dests))
            if isinstance(v, (str, list, dict)):    rerefed[k] = cls._reref(v, dests)
            else:                                   rerefed[k] = v
        return rerefed
    
    @classmethod
    def _instantiate_zmd(cls, zmd: dict):
        cls._set_zmd_version(zmd)
        zmd_entries: dict[str, dict] = zmd.get(ZMD_ENTRIES)
        if not zmd_entries: return
        
        dests: dict[str, Content] = {} # must be created before relinking to handle arbitrary references
        for obj_link in zmd_entries:
            classname, uri = cls._from_link(obj_link)
            if classname and uri and classname in globals():
                cont_type: type[Content] = globals()[classname]
                if cont_type is Query: continue
                dests[obj_link] = cont_type(uri) # intrinsically adds to ALL_NODES
        
        for obj_link, md in zmd_entries.items():
            if obj_link not in dests: continue
            obj = dests[obj_link]
            for k, v in cls._reref(md, dests).items():
                setattr(obj, k, v)
            obj._hasMetadata = obj.full_metadata()
        cls.PARSING = None
    
    @classmethod
    def from_zmd(cls):
        import_path = Zotify.CONFIG.get_zmd_import_location()
        if not Path(import_path).exists():  return
        elif Path(import_path).is_dir():    zmd_files = Path(import_path).rglob("*.zmd")
        else:                               zmd_files = (import_path,)
        
        zmds = []
        for zmdf in zmd_files:
            if not file_has_content(zmdf): continue
            with open(zmdf) as f:
                zmds.append(json.load(f))
        if not zmds: return
        
        with Loader(f"Importing ZMD file(s)"):
            for zmd in zmds: cls._instantiate_zmd(zmd) # to ALL_NODES
    
    @classmethod
    def to_zmd(cls, timestamp: str, cont: set[Content]):
        zmd_dir_or_file = Zotify.CONFIG.get_zmd_export_location()
        zmd_path = ensure_is_file(Path(zmd_dir_or_file), timestamp + ".zmd")
        
        cls.PARSING = cls.LATEST_ZMD
        zmd = {**cls.PARSING}
        if file_has_content(zmd_path):
            with open(zmd_path) as f:
                zmd.update(json.load(f))
            cls._set_zmd_version(zmd)
        
        if cls.EXPORT_QUERY:
            qs = {c for c in cont if isinstance(c, Query) and any(c.requested_objs)}
            zmd[ZMD_ENTRIES].update({cls._to_link(q): {"requested_objs": cls._deref(q.requested_objs)} for q in qs})
        entries = {cls._to_link(c): cls._deref(c.__dict__) for c in cont if not isinstance(c, Query)}
        zmd[ZMD_ENTRIES].update(entries)
        cls.PARSING = None
        
        with open(zmd_path, "w") as f:
            json.dump(zmd, f, indent=4)


class Tagger:
    def __init__(self, path: PurePath):
        self.path = path
        self.file_tags: AudioFile = load_file(path)
    
    @classmethod
    def _content_to_tags(cls, obj: Content | None) -> tuple[dict[str], dict[str], dict[str]]:
        reliable_tags, optional_tags, custom_tags = {}, {}, {}
        if isinstance(obj, Track):
            reliable_tags = {
                TRACKTITLE:     obj.name,
                DISCNUMBER:     obj.disc_number,
                TRACKNUMBER:    obj.track_number,
                ARTIST:         obj.artist_names(),
                GENRE:          obj.genre_names(),
                ISRC:           obj.isrc,
            }
            optional_tags = {
                LYRICS:         "".join(obj.lyrics) if Zotify.CONFIG.get_lyrics_to_metadata() and obj.lyrics else None
            }
            custom_tags = { 
                TRACKID:        obj.id,
                URI:            obj.uri,
                EAN:            obj.ean,
                UPC:            obj.upc,
            }
            rt, ot, ct = cls._content_to_tags(obj.album)
            for k, v in rt.items():
                if k in {ALBUM, ALBUMARTIST, YEAR, ARTWORK}: reliable_tags.update({k: v})
                elif k == COMPILATION and v: reliable_tags.update({k: v})
            for k, v in ot.items():
                if k in {TOTALTRACKS, TOTALDISCS}: optional_tags.update({k: v})
        elif isinstance(obj, Album):
            reliable_tags = {
                ALBUM:          obj.name,
                ALBUMARTIST:    obj.artist_names(),
                COMPILATION:    obj.compilation,
                YEAR:           obj.year,
                ARTWORK:        requests.get(obj.image_url).content if obj.image_url else None, # expect jpeg
            }
            optional_tags = {
                TOTALTRACKS:    obj.total_tracks if Zotify.CONFIG.get_disc_track_totals() else None,
                TOTALDISCS:     obj.total_discs if Zotify.CONFIG.get_disc_track_totals() else None,
            }
            custom_tags = {
                LABEL:          obj.label,
                DATE:           obj.release_date
            }
        return reliable_tags, optional_tags, custom_tags
    
    def _write_tag_raw(self, norm_key, key, md_val, appendable):
        self.file_tags.set_raw(norm_key, key, md_val, appendable)
        self.file_tags.save()
    
    def _custom_mp3_tag(self, tag: str, md_val):
        vallist = md_val if isinstance(md_val, list) else [md_val]
        self.file_tags.mfile.tags.add(TXXX(encoding=3, desc=tag.upper(), text=vallist))
    
    def _custom_m4a_tag(self, tag: str, md_val):
        atomic_tag = M4A_CUSTOM_TAG_PREFIX + tag
        vallist = md_val if isinstance(md_val, list) else [md_val]
        freeform_set(self.file_tags, atomic_tag, type('tag', (object,), {'values': vallist})())
    
    def _custom_ogg_tag(self, tag: str, md_val):
        self.file_tags.tag_map[tag] = TAG_MAP_ENTRY(getter=tag, setter=tag, type=type(md_val))
        self.file_tags[tag] = md_val
    
    def _set_custom(self, tag: str, md_val):
        if self.path.suffix.lower() == ".mp3":      self._custom_mp3_tag(tag, md_val)
        elif self.path.suffix.lower() == ".m4a":    self._custom_m4a_tag(tag, md_val)
        else:                                       self._custom_ogg_tag(tag, md_val)
    
    def write_tags(self, obj: Content):
        reliable_tags, optional_tags, custom_tags = self._content_to_tags(obj)
        for tag, md_val in reliable_tags.items():
            if md_val is None: continue
            self.file_tags[tag] = md_val
        for tag, md_val in optional_tags.items():
            if md_val is None: continue
            self.file_tags[tag] = md_val
        for tag, md_val in custom_tags.items():
            if md_val is None: continue
            self._set_custom(tag, md_val)
        self.file_tags.save()
        return self
    
    def _get_raw(self, tag) -> list | None:
        tag_obj = self.file_tags.mfile.tags.get(tag)
        if not tag_obj:                     return None
        elif MP3_CUSTOM_TAG_PREFIX in tag:  return tag_obj.text
        elif M4A_CUSTOM_TAG_PREFIX in tag:  return [v.decode() for v in tag_obj]
        else:                               return tag_obj
    
    def _match_tag_group(self, tag_group: dict[str]) -> dict[str]:
        mismatches = {}
        for tag, md_val in tag_group.items():
            try:    mditem = self.file_tags.get(tag)
            except: mditem = self._get_raw(tag)
            if not mditem:
                mismatches[tag] = False if md_val is None else f' Missing from File, Metadata: "{md_val}"'
                continue
            if not isinstance(mditem, MetadataItem):    on_file = mditem if isinstance(md_val, list) else mditem[0]
            elif isinstance(md_val, list):              on_file = mditem.values
            elif isinstance(md_val, bytes):             on_file = mditem.val.data
            else:                                       on_file = mditem.val
            match = (md_val is None and not bool(on_file)) or str(on_file) == str(md_val)
            mismatches[tag] = False if match else f' on File: "{on_file}", in Metadata: "{md_val}"'
        return mismatches
    
    def matches_metadata(self, obj: Content) -> bool:
        reliable_tags, optional_tags, custom_tags = self._content_to_tags(obj)
        
        reliable_misses = False
        for k, v in self._match_tag_group(reliable_tags).items():
            if v: Printer.hashtaged(PrintChannel.DEBUG, k.upper() + v); reliable_misses = True
        
        strict_misses = reliable_misses
        for k, v in self._match_tag_group(optional_tags).items():
            if v: Printer.hashtaged(PrintChannel.DEBUG, k.upper() + v); strict_misses = True
        
        custom_keys = [k for k in custom_tags]
        if self.path.suffix.lower() == ".mp3":
            custom_tags = {MP3_CUSTOM_TAG_PREFIX + k.upper(): v for k, v in custom_tags.items()}
        elif self.path.suffix.lower() == ".m4a":
            custom_tags = {M4A_CUSTOM_TAG_PREFIX + k: v for k, v in custom_tags.items()}
        for k, v in zip(custom_keys, self._match_tag_group(custom_tags).values()):
            if v: Printer.hashtaged(PrintChannel.DEBUG, k.upper() + v); strict_misses = True
        
        return strict_misses if Zotify.CONFIG.get_strict_library_verify() else reliable_misses


class SongArchive:
    """ Entry: id, date, author, name, path (only filename if from legacy archive) """
    UPDATE_ARCHIVE: bool = Zotify.CONFIG.get_update_archive()
    
    def __init__(self, dir_path: PurePath | None = None):
        self._global = dir_path is None
        self.path = Zotify.CONFIG.get_song_archive_location() if dir_path is None else dir_path / '.song_ids'
        self.mode = 'a' if file_has_content(self.path) else 'w' # should always exist from Content.create_download_directory()
        self.disabled = Zotify.CONFIG.get_no_song_archive() if self._global else Zotify.CONFIG.get_no_dir_archives()
    
    def upgrade_legacy_archive(self, entries: list[str]) -> None:
        """ Attempt to match a legacy archive's filename to a full path """
        
        def find_artist_names(artists: list[str] | str) -> list[str]:
            if Zotify.CONFIG.get_artist_delimiter() == "": return artists
            return artists.split(Zotify.CONFIG.get_artist_delimiter())
        
        rewrite_legacy = False
        for i, entry in enumerate(entries):
            entry_items = entry.strip().split('\t')
            filename_or_path = PurePath(entry_items[-1])
            if filename_or_path.is_absolute():
                entries[i] = entry_items
                continue
            
            rewrite_legacy = True
            path_entry = filename_or_path
            for glob_path in Path(Zotify.CONFIG.get_root_path()).glob('**/' + str(filename_or_path)):
                reliable_tags, unreliable_tags = Track.read_audio_tags(PurePath(glob_path))
                if ("trackid" in unreliable_tags and unreliable_tags["trackid"] == entry_items[0]
                or  find_artist_names(reliable_tags[0])[0] == entry_items[2]
                or  reliable_tags[2] == entry_items[3]):
                    path_entry = PurePath(glob_path)
                    break
            
            entries[i] = entry_items[:-1] + [path_entry]
        
        if rewrite_legacy:
            Path(self.path).unlink()
            mode = 'w'
            for entry in entries:
                self.add_entry(*entry, mode)
                mode = 'a'
    
    def read_entries(self) -> list[str]:
        if self.disabled or not file_has_content(self.path):
            return []
        with open(self.path, 'r', encoding='utf-8') as f:
            entries = f.readlines()
        if self._global and SongArchive.UPDATE_ARCHIVE:
            SongArchive.UPDATE_ARCHIVE = False
            self.upgrade_legacy_archive(entries)
            return self.read_entries()
        return entries
    
    def ids(self) -> list[str]:
        return [e.strip().split('\t')[0] for e in self.read_entries()]
    
    def paths(self) -> list[PurePath]:
        return [PurePath(e.strip().split('\t')[-1]) for e in self.read_entries()]
    
    def id_path(self, item_id: str) -> PurePath:
        return self.paths()[self.ids().index(item_id)]
    
    def add_entry(self, item_id: str, timestamp: str, author_name: str, item_name: str, item_path: PurePath, mode: str) -> None:
        if not timestamp:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f'{item_id}\t{timestamp}\t{author_name}\t{item_name}\t{item_path}\n'
        with open(self.path, mode, encoding='utf-8') as file:
            file.write(entry)
    
    def add_obj(self, obj: Track | Episode, item_path: PurePath) -> None:
        if self.disabled: return
        author_name = obj.artists[0].name if isinstance(obj, Track) else obj.show.publisher
        item_name = obj.name if isinstance(obj, Track) else str(obj)
        self.add_entry(obj.id, "", author_name, item_name, item_path, self.mode)


class M3U8:
    def __init__(self, cont_paths: list[PurePath | None], cont_type: type[Content], parent_cont: Container | Query):
        self.cont_type = cont_type
        self.name = self.cont_type.uppers if isinstance(parent_cont, Query) else f'"{parent_cont.name}"'
        
        dir = Zotify.CONFIG.get_m3u8_location()
        if not dir: dir = self.dynamic_dir(cont_paths)
        self.path = dir / (self.fill_output_template(parent_cont) + ".m3u8") if dir else None
    
    def fill_output_template(self, parent_cont: Container | Query):
        output_template = Zotify.CONFIG.get_m3u8_filename()
        if not output_template or isinstance(parent_cont, Query):
            return Content.fix_filename(f"{parent_cont.id}_{self.cont_type.lowers}")
        
        repl_dict: dict[str, str] = {}
        def update_repl(md_val, *replstrs: str):
            repl_dict.update(zip(replstrs, [md_val]*len(replstrs)))
        
        update_repl(self.cont_type,                     "{content_type}")
        update_repl(parent_cont.id,                     "{id}")
        update_repl(parent_cont.name,                   "{name}")
        
        if isinstance(parent_cont, Playlist):
            if parent_cont.owner:
                update_repl(parent_cont.owner.id,       "{owner_id}")
                update_repl(parent_cont.owner.name,     "{owner_name}")
            update_repl(parent_cont.snapshot_id,        "{snapshot_id}")
        
        for replstr, md_val in repl_dict.items():
            output_template = output_template.replace(replstr, Content.fix_filename(md_val)) 
        
        return output_template
    
    def dynamic_dir(self, cont_paths: list[PurePath | None]) -> PurePath | None:
        paths = {path for path in cont_paths if isinstance(path, PurePath) and path.is_relative_to(self.cont_type._path_root)}
        return get_common_dir(paths) if any(paths) else None
    
    @staticmethod
    def fetch_songs(m3u8_path: PurePath) -> list[str]:
        if not file_has_content(m3u8_path):
            return []
        with open(m3u8_path, 'r', encoding='utf-8') as file:
            linesraw = file.readlines()[2:]
            # songsgrouped = [] # group by song and path
            # for i in range(len(linesraw)//3):
            #     songsgrouped.append(linesraw[3*i:3*i+3])
        return linesraw
    
    @staticmethod
    def find_sync_point(paths: list[PurePath | None], m3u8_entry_path: str) -> int | None:
        for i, path in enumerate(paths):
            Printer.logger(f"{path} == {m3u8_entry_path}")
            if str(path) == m3u8_entry_path:
                return i
            elif str(path) in m3u8_entry_path:
                Printer.hashtaged(PrintChannel.WARNING, 'TRACK FILEPATH WITHIN LIKED SONG M3U8 ENTRY\n' +
                                                        'M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n' +
                                                        'POSSIBLY FROM NON-UPDATED SONG ARCHIVE FILE\n' +
                                                        "(CONSIDER RUNNING --update-archive)")
                return i
            elif m3u8_entry_path in str(path):
                Printer.hashtaged(PrintChannel.WARNING, 'LIKED SONG M3U8 ENTRY WITHIN TRACK FILEPATH\n' +
                                                        'M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n' +
                                                        'POSSIBLY FROM M3U8 USING RELATIVE PATHS\n' +
                                                        '(CONSIDER USING FULL PATHS FOR LIKED SONGS M3U8)')
                return i
    
    def write(self, dlcs: list[DLContent | None], cont_paths: list[PurePath | None]):
        if self.path is None:
            Printer.hashtaged(PrintChannel.WARNING, f'SKIPPING M3U8 CREATION FOR {self.name}\n' +
                                                     'NO CONTENT WITH VALID FILEPATHS FOUND')
            return
        elif Zotify.CONFIG.get_m3u8_relative_paths():
            cont_paths = [try_rel_path(p, self.path.parent) for p in cont_paths]
        
        missing_name = f"{self.cont_type.clsn}"
        if isinstance(self.cont_type, Container): missing_name += f" {self.cont_type._contains}"
        
        Path(self.path.parent).mkdir(parents=True, exist_ok=True)
        with open(self.path, 'w', encoding='utf-8') as file:
            file.write("#EXTM3U\n\n")
            for i, dlc, path in zip(range(len(dlcs)), dlcs, cont_paths):
                file.write(f"#EXTINF:{dlc.duration_ms // 1000}, {dlc}\n" if dlc else f"# Missing {missing_name} {i+1}\n")
                file.write(f"{path}\n\n" if path else "# None\n\n")
        
        Printer.hashtaged(PrintChannel.MANDATORY, f'M3U8 CREATED FOR {self.name}\n' +
                                                  f'SAVED TO: {self.cont_type.rel_path(self.path)}')
    
    def append(self, append_strs: list[str]):
        if self.path is None or not file_has_content(self.path) or not append_strs:
            return
        with open(self.path, 'a', encoding='utf-8') as file:
            file.writelines(append_strs)
