from __future__ import annotations
import ffmpy
import functools
import music_tag
import requests
import subprocess
import uuid

from zotify import __version__
from zotify.const import *
from zotify.termoutput import PrintChannel, Printer, Loader, Interface
from zotify.utils import *


class DynamicClassNameAttrs(type):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        cls.clsn = "".join([" " + c if c.isupper() else c for c in cls.__name__])[1:]
        cls.type_attr = cls.clsn.lower()
        cls.lowers = cls.type_attr.removesuffix("y") +\
                     ("ie" if cls.type_attr.endswith("y") else "") +\
                     ("s" if not cls.type_attr.endswith("s") else "")
        cls.uppers = cls.lowers.upper()


class HierarchicalNode(metaclass=DynamicClassNameAttrs):
    _root_node = False
    ALL_NODES: dict[HierarchicalNode, HierarchicalNode] = {}
    
    def __init__(self):
        self.parents:           set[HierarchicalNode] = set()
        self.children:          set[HierarchicalNode] = set()
        self.ALL_NODES[self] = self
    
    def get_if_exists(self, node_comparable) -> HierarchicalNode | None:
        return self.ALL_NODES.get(node_comparable)
    
    def adopt(self, child_to_be: HierarchicalNode):
        self.children.add(child_to_be)
        child_to_be.parents.add(self)
    
    def be_supervised(self, parent_to_be: HierarchicalNode):
        self.parents.add(parent_to_be)
        parent_to_be.children.add(self)


class Content(HierarchicalNode):
    # CONFIG must be loaded with args before any Content classes are instantiated/imported
    _path_root: PurePath = Zotify.CONFIG.get_root_path()
    _regex_flag: re.Pattern | None = None
    _to_str_attrs = [URI, NAME]
    _fetch_args = ""
    url = ""
    
    def __init__(self, uri: str):
        # uri == zot : type : id
        # self.uri = uri.split(":", 1)[-1]
        self.uri = uri
        super().__init__()
        # local file URIs will have more than 2 colons
        self.id = self.uri.split(":", 1)[-1]
        self.local_file = self.id.count(":") > 0
        
        self.downloaded = False
        self.hasMetadata = False
        self.skippable = None
        
        self.name = ""
    
    def __eq__(self, other) -> bool:
        if isinstance(other, Content): return self.uri == other.uri
        elif isinstance(other, str): return self.uri == other
        return False
    
    def __hash__(self):
        return hash(self.uri)
    
    def __str__(self):
        default = fix_filename(f"({self.type_attr}){self.id}")
        if Zotify.CONFIG.get_bypass_metadata(): return default
        vals = []
        for attr in self._to_str_attrs:
            val = getattr(self, attr, None)
            if isinstance(val, list):       val = val[0] if isinstance(val[0], Content) else ", ".join(str(v) for v in val)
            if isinstance(val, Content):    val = getattr(val, NAME, None)
            if val:                         vals.append(str(val))
        return fix_filename(" - ".join(vals)) if vals else default
    
    def dashboard(self, extra_attrs: list[str] = [], suppress_id: bool = False, force_clsn: bool = False) -> str:
        db = ""
        attrs = (["id", "name"] if not suppress_id else ["name",]) + extra_attrs
        for attr in attrs:
            adds = Interface.parse_dbs(self, attr)
            if force_clsn:
               adds = "\n".join([f"{self.clsn} {line}" if self.clsn not in line else line for line in adds.split("\n")])
            db += adds
            if attr != attrs[-1]: db += "\n"
        return db
    
    def regex_check(self, skip_debug_print: bool = False) -> bool:
        if self._regex_flag is None: return False
        regex_match = self._regex_flag.search(self.name)
        if not skip_debug_print:
            Printer.debug("Regex Check\n" +
                         f"Pattern: {self._regex_flag.pattern}\n" +
                         f"{self.clsn} Name: {self.name}\n" +
                         f"Match Object: {regex_match}")
        if regex_match:
            Printer.hashtaged(PrintChannel.SKIPPING, f'{self.clsn.upper()} MATCHES REGEX FILTER\n' +
                                                     f'{self.clsn}_Name: {self.name} - {self.clsn}_ID: {self.id}' +
                                                    (f'\nRegex Groups: {regex_match.groupdict()}' if regex_match.groups() else ""))
        return regex_match
    
    def fetch_metadata(self, args: list[str] = []) -> dict[str]:
        with Loader(f"Fetching {self.type_attr} information..."):
            if self._fetch_args or args:
                args = "&" + "&".join([self._fetch_args] + args)
            _, resp = Zotify.invoke_url(f'{self.url}/{self.id}?{MARKET_APPEND}{args}')
        if resp: return resp
        else:    raise ValueError("No Metadata Fetched")
    
    def parse_metadata(self, relative: Content | None, resp: dict):
        class Metadata():
            # ignore URI
            PARSE_AS_STR        = {ADDED_AT, ALBUM_GROUP, ALBUM_TYPE, DESCRIPTION, DISC_NUMBER,
                                   DISPLAY_NAME, ID, LABEL, NAME, PUBLISHER, RELEASE_DATE, SNAPSHOT_ID,}
            INT_PARSE_AS_STR    = {TOTAL_EPISODES, TOTAL_TRACKS, TRACK_NUMBER,}
            PARSE_AS_INT        = {DURATION_MS, POPULARITY,}
            PARSE_AS_BOOL       = {COLLABORATIVE, EXPLICIT, IS_EXTERNALLY_HOSTED,
                                   IS_LOCAL, IS_PLAYABLE, PUBLIC,}
            
            def __init__(self, obj: Content, resp: dict):
                for attr in self.PARSE_AS_STR:
                    setattr(self, attr, safe_typecast(resp, attr, str))
                for attr in self.INT_PARSE_AS_STR:
                    raw_val: str | None = safe_typecast(resp, attr, str)
                    setattr(self, attr, None if raw_val is None else raw_val.zfill(2))
                for attr in self.PARSE_AS_INT:
                    setattr(self, attr, safe_typecast(resp, attr, int))
                for attr in self.PARSE_AS_BOOL:
                    setattr(self, attr, safe_typecast(resp, attr, bool))
                
                self.compilation        : bool          = self.album_type == COMPILATION if self.album_type else None
                self.external_urls      : dict          = resp.get(EXTERNAL_URLS)
                self.genres             : list[str]     = resp.get(GENRES)
                # self.lyrics           : list[str]     = resp.get(LYRICS)
                self.year               : str           = self.release_date.split('-')[0] if self.release_date else None
                
                def uri_local_backup(item: dict | None, key: str):
                    if item is None: return
                    uri = item.get(URI); name = item.get(NAME)
                    if not name: name = f"noname-{uuid.uuid4()}"
                    if not uri: uri = f":local:{key.lower()}:{name}:::"
                    item[URI] = uri
                
                added_by                : dict          = resp.get(ADDED_BY)
                if added_by:
                    self.added_by       : User          = obj.parse_relatives([added_by], User, make_parent=True)[0]
                
                album                   : dict          = resp.get(ALBUM)
                if album:
                    uri_local_backup(album, ALBUM)
                    parent = isinstance(obj, Track)
                    self.album          : Album         = obj.parse_relatives([album], Album, make_parent=parent)[0]
                elif isinstance(obj, Track) and isinstance(relative, Album):
                    self.album          : Album         = relative
                
                artists                 : list[dict]    = resp.get(ARTISTS)
                if artists:
                    for i, artist in enumerate(artists):
                        uri_local_backup(artist, ARTIST + str(i+1))
                    parent = isinstance(obj, (Track, Album))
                    self.artists        : list[Artist]  = obj.parse_relatives(artists, Artist, make_parent=parent)
                
                episodes                : dict          = resp.get(EPISODES)
                if episodes:
                    items               : list[dict]    = episodes.get(ITEMS)
                    if items:
                        for i, episode in enumerate(items):
                            uri_local_backup(episode, EPISODE + str(i+1))
                        self.episodes   : list[Episode] = obj.parse_relatives(items, Episode)
                        self.needs_expansion = episodes[NEXT] is not None
                    else:
                        self.needs_expansion = True
                
                images                  : list[dict]    = resp.get(IMAGES)
                if images:
                    largest_image       : dict          = max(images, key=lambda img: safe_typecast(img, WIDTH, int),
                                                              default={URL: None})
                    self.image_url      : str           = largest_image.get(URL)
                
                owner                   : dict          = resp.get(SHOW)
                if owner:
                    self.owner          : User          = obj.parse_relatives([owner], User, make_parent=True)[0]
                    self.owner.name = self.owner.display_name
                
                show                    : dict          = resp.get(SHOW)
                if show:
                    uri_local_backup(show, SHOW)
                    parent = isinstance(obj, Episode)
                    self.show           : Show          = obj.parse_relatives([show], Show, make_parent=parent)[0]
                elif isinstance(obj, Episode) and isinstance(relative, Show):
                    self.show           : Show          = relative
                
                followers               : dict          = resp.get(FOLLOWERS)
                if followers:
                    self.followers: int           = safe_typecast(followers, TOTAL, int)
                
                tracks                  : dict          = resp.get(TRACKS)
                if tracks:
                    items               : list[dict]    = tracks.get(ITEMS)
                    if items:
                        if isinstance(obj, Album):
                            for i, track in enumerate(items):
                                uri_local_backup(track, TRACK + str(i+1))
                            # possible underflow if len(items) > 100
                            self.tracks: list[Track] = obj.parse_relatives(items, Track)
                            self.needs_expansion = tracks.get(NEXT) is not None
                            if not self.needs_expansion:
                                # set in Album.grab_more_children() if album incomplete
                                self.total_discs = safe_typecast(items[-1], DISC_NUMBER, int)
                                self.duration_ms = sum((int(t.duration_ms) for t in self.tracks))
                            self.hasMetadata = True
                        elif isinstance(obj, Playlist):
                            tracks_eps_empty: list[dict] = [item.get(TRACK) for item in items]
                            for i, track_or_ep, item in zip(range(len(items)), tracks_eps_empty, items):
                                if track_or_ep is None: continue
                                uri_local_backup(track_or_ep, TRACK + str(i+1))
                                track_or_ep[ADDED_AT] = item.get(ADDED_AT)
                                track_or_ep[ADDED_BY] = item.get(ADDED_BY)
                                track_or_ep[IS_LOCAL] = item.get(IS_LOCAL)
                            # possible underflow if len(items) > 100
                            self.tracks_or_eps = obj.parse_relatives(tracks_eps_empty, (Track, Episode))
                            self.needs_expansion = tracks.get(NEXT) is not None
                    else:
                        self.needs_expansion = True
                
                if isinstance(obj, (DLContent, Playlist, User, Show)):
                    self.hasMetadata = True
                elif isinstance(obj, Artist):
                    self.hasMetadata = self.genres is not None
                    self.needs_expansion = True
                elif isinstance(obj, Album) and self.album_group:
                    self.needs_expansion = True
        
        for k, v in Metadata(self, resp).__dict__.items():
            if v is None: continue
            elif k == ID and self.id != v:
                Printer.debug(f"Updated {self.clsn} {self.name} ({self.uri}) ID to {self.id}")
                setattr(self, ID, v)
            elif isinstance(self, Container) and k in self.__dict__ and getattr(self, k) == getattr(self, "_main_items"):
                self._main_items.extend(v)
            elif relative and k in {ADDED_AT, ADDED_BY, ALBUM_GROUP, IS_LOCAL}:
                relational_attr: dict[Container, str | bool | User] = getattr(self, k)
                relational_attr.update({relative: v})
            elif not self.hasMetadata:
                setattr(self, k, v)
    
    def make_or_link_relative(self, relative_uri: str, RelativeClass: type, make_parent: bool = False) -> Content | Container:
        relative_to_be = self.get_if_exists(relative_uri)
        if relative_to_be is None:
            relative_to_be: Content | Container = RelativeClass(relative_uri)
        
        self.be_supervised(relative_to_be) if make_parent else self.adopt(relative_to_be)
        return relative_to_be
    
    def parse_relatives(self, resps: list[dict[str, str] | None], RelativeClasses: type[Content] | tuple[type[Content]],
                        make_parent: bool = False) -> list[Content | Container | None]:
        RelativeClasses = RelativeClasses if isinstance(RelativeClasses, tuple) else (RelativeClasses,)
        type_selector = tuple(cls.type_attr for cls in RelativeClasses)
        
        new_relatives: list[Content | Container] = []
        for i, resp in enumerate(resps):
            if not resp or not resp.get(URI) or not resp.get(TYPE):
                Printer.hashtaged(PrintChannel.WARNING, 'Missing Expected Response for Related Metadata Object\n' +
                                                       f'Parsing {"Parent" if make_parent else "Child"} #{i} of {self.clsn} ({self.id})\n' +
                                                       f'Expected Relative Types: {[c.clsn for c in RelativeClasses]}')
                if resp: Printer.json_dump(resp, PrintChannel.WARNING)
                new_relatives.append(None)
                continue
            RelativeClass: type[Content | Container] = RelativeClasses[type_selector.index(resp[TYPE])]
            new_relative = self.make_or_link_relative(resp[URI].split(":", 1)[-1], RelativeClass, make_parent)
            new_relative.parse_metadata(self, resp)
            new_relatives.append(new_relative)
        
        return new_relatives
    
    def check_skippable(self, parent_stack: ParentStack) -> bool:
        if self.skippable is not None: return self.skippable
        self.skippable = False
        return self.skippable
    
    def mark_downloaded(self, parent_stack: ParentStack | None = None, path: PurePath | None = None):
        if isinstance(self, Container) and not isinstance(self, Query):
            self.downloaded = all(c.downloaded for c in self._main_items)
        elif not isinstance(self, DLContent):
            self.downloaded = True
        elif path:
            self.downloaded = True
            self.real_filepaths[parent_stack] = path


class DLContent(Content):
    _codecs: dict[str, str] = {}
    _ext = ""
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.dl_status = ""
        self.in_dir_archive = False
        self.in_global_archive = self.id in get_archived_item_ids()
        self.real_filepaths: dict[ParentStack, PurePath] = {}
        self._clone_to: set[ParentStack] = set()
        
        self.duration_ms    : int   = None
        self.is_playable    : bool  = None
    
    def set_dl_status(self, str_status) -> Loader:
        self.dl_status = str_status
        if Zotify.CONFIG.get_standard_interface():
            Interface.refresh()
        return Loader(str_status + "...")
    
    def fill_output_template(self, parent_stack: ParentStack, output_template: str = "") -> PurePath:
        if Zotify.CONFIG.get_bypass_metadata():
            return self._path_root / f"{self.id}.{self._ext}"
        return None
    
    def rel_path(self, p: PurePath | ParentStack) -> PurePath:
        if isinstance(p, ParentStack):
            p = check_path_dupes(self.fill_output_template(p))
        return p.relative_to(self._path_root)
    
    def check_skippable(self, parent_stack: ParentStack) -> bool:
        self.skippable = super().check_skippable(parent_stack)
        if self.skippable: return self.skippable
        
        if self.regex_check(skip_debug_print=Zotify.CONFIG.get_optimized_dl()):
            self.skippable = True
        elif not self.is_playable and not Zotify.CONFIG.get_bypass_metadata():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} IS UNAVAILABLE)')
            self.skippable = True
        
        if self.skippable: return self.skippable
        
        path = self.fill_output_template(parent_stack)
        path_exists = Path(path).is_file() and Path(path).stat().st_size
        if isinstance(self, Episode) and path.suffix == ".copy":
            # file suffix agnostic check
            for file_match in Path(path.parent).glob(path.stem + ".*", case_sensitive=True):
                if file_match.stat().st_size:
                    path_exists = True
                    break
        
        in_dir_archive = self.id in get_archived_item_ids(path.parent)
        if not Zotify.CONFIG.get_optimized_dl():
            Printer.debug("Duplicate Check\n" +
                         f"File Already Exists: {path_exists}\n" +
                         f"id in Local Archive: {in_dir_archive}\n" +
                         f"id in Global Archive: {self.in_global_archive}")
        
        if path_exists and Zotify.CONFIG.get_skip_existing() and Zotify.CONFIG.get_disable_directory_archives():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.rel_path(path)}" (FILE ALREADY EXISTS)')
            self.mark_downloaded(parent_stack, path)
            self.skippable = True
        elif in_dir_archive and Zotify.CONFIG.get_skip_existing() and not Zotify.CONFIG.get_disable_directory_archives():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} ALREADY EXISTS)\n'
                                                     f'FILE: "{self.rel_path(path)}"')
            archived_path = get_archived_item_paths(path.parent)[get_archived_item_ids(path.parent).index(self.id)]
            self.mark_downloaded(parent_stack, archived_path)
            self.skippable = True
        elif self.in_global_archive and Zotify.CONFIG.get_skip_previously_downloaded():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} DOWNLOADED PREVIOUSLY)')
            archived_path = get_archived_item_paths()[get_archived_item_ids().index(self.id)]
            self.mark_downloaded(parent_stack, archived_path)
            self.skippable = True
        
        return self.skippable
    
    def fetch_content_stream(self, stream, temppath: PurePath, parent_stack: ParentStack) -> str:
        time_start = time.time()
        total_size = stream.input_stream.size
        downloaded = 0
        disable = Zotify.CONFIG.get_standard_interface() or not Zotify.CONFIG.get_show_download_pbar()
        pbar = Printer.pbar(desc=str(self), total=total_size, unit='B', unit_scale=True,
                            unit_divisor=1024, disable=disable, pbar_stack=parent_stack.PBARS)
        
        Path(temppath.parent).mkdir(parents=True, exist_ok=True)
        try:
            with open(temppath, 'wb') as file:
                b = 0
                while b < 5:
                    data = stream.input_stream.stream().read(Zotify.CONFIG.get_chunk_size())
                    pbar.update(file.write(data))
                    downloaded += len(data)
                    b += 1 if data == b'' else 0
                    if Zotify.CONFIG.get_download_real_time():
                        delta_real = time.time() - time_start
                        delta_want = (downloaded / total_size) * (self.duration_ms/1000)
                        if delta_want > delta_real:
                            time.sleep(delta_want - delta_real)
        finally:
            pbar.close(); pbar.clear()
        time_dl_end = time.time()
        
        return fmt_duration(time_dl_end - time_start)
    
    def get_audio_duration(self, path: PurePath) -> float:
        """ Returns the downloaded file's duration in seconds """
        ff_m = ffmpy.FFprobe(
            global_options=['-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
            inputs={path: ["-show_entries", "format=duration"]},
        )
        stdout, stderr = ff_m.run(stdout=subprocess.PIPE)
        loggable_output = [stdout.decode() if stdout else "",
                           stderr.decode() if stderr else ""]
        Printer.logger("\n\n".join(loggable_output), PrintChannel.DEBUG)
        duration = re.search(r'[\D]=([\d\.]*)', str(stdout)).groups()[0]
        return float(duration)
    
    def get_audio_codec(self, path: PurePath) -> str:
        ff_m = ffmpy.FFprobe(
            global_options=['-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
            inputs={path: ["-show_entries", "stream=codec_name"]},
        )
        stdout, stderr = ff_m.run(stdout=subprocess.PIPE)
        loggable_output = [stdout.decode() if stdout else "",
                           stderr.decode() if stderr else ""]
        Printer.logger("\n\n".join(loggable_output), PrintChannel.DEBUG)
        return stdout.decode().strip().split("=")[1].split("\r")[0].split("\n")[0]
    
    def convert_audio_format(self, temppath: PurePath, path: PurePath) -> str | None:
        file_codec = self._codecs.get(Zotify.CONFIG.get_download_format().lower(), 'copy')
        output_params = ['-c:a', file_codec]
        
        if file_codec != 'copy':
            bitrate = Zotify.CONFIG.get_transcode_bitrate()
            if bitrate in {"auto", ""}:
                bitrate = Zotify.DOWNLOAD_BITRATE
            if bitrate:
                output_params += ['-b:a', bitrate]
        
        def run_ffmpeg(output_params: list[str], error_str: str) -> float | Exception:
            try:
                ff_m = ffmpy.FFmpeg(
                global_options=['-y', '-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
                inputs={temppath: None},
                outputs={path: output_params}
                )
                
                stdout, stderr = ff_m.run(stdout=subprocess.PIPE)
                loggable_output = [stdout.decode() if stdout else "",
                                   stderr.decode() if stderr else ""]
                Printer.logger("\n\n".join(loggable_output), PrintChannel.DEBUG)
                
                if Path(temppath).exists():
                    Path(temppath).unlink()
                
                return time.time()
                
            except Exception as e:
                if isinstance(e, ffmpy.FFExecutableNotFoundError):
                    Printer.hashtaged(PrintChannel.WARNING,  'FFMPEG NOT FOUND\n' +
                                                            f'SKIPPING CONVERSION TO {file_codec.upper()}')
                else:
                    Printer.hashtaged(PrintChannel.WARNING, str(e) + "\n" + error_str)
                return e
        
        time_ffmpeg_start = time.time(); time_ffmpeg_end = None
        
        custom_ffmpeg_args = Zotify.CONFIG.get_custom_ffmpeg_args()
        if custom_ffmpeg_args:
            customized_output_params = custom_ffmpeg_args if file_codec == 'copy' else output_params + custom_ffmpeg_args
            time_ffmpeg_end = run_ffmpeg(customized_output_params, 'CUSTOM FFMPEG ARGUMENTS FAILED')
            if isinstance(time_ffmpeg_end, ffmpy.FFExecutableNotFoundError):
                return
        
        if time_ffmpeg_end is None or isinstance(time_ffmpeg_end, Exception):
            time_ffmpeg_end = run_ffmpeg(output_params, f'SKIPPING CONVERSION TO {file_codec.upper()}')
            if isinstance(time_ffmpeg_end, Exception):
                return
        
        return fmt_duration(time_ffmpeg_end - time_ffmpeg_start)
    
    # placeholder func, overwrite in each child class
    def download(self, parent_stack: ParentStack):
        pass
    
    def clone_file(self, parent_stack: ParentStack) -> bool:
        """ Attempt to clone and return if clone succeeded """
        clone_path = check_path_dupes(self.fill_output_template(parent_stack))
        if not self.real_filepaths:
            Printer.hashtaged(PrintChannel.WARNING, f'ATTEMPT TO CLONE {self.clsn.upper()} "{self}" FAILED\n' + 
                                                     'FILE NOT YET DOWNLOADED, THIS SHOULD NOT HAPPEN')
        for filepath in self.real_filepaths.values():
            if not Path(filepath).exists(): continue
            pathlike_move_safe(filepath, clone_path, copy=True)
            self.mark_downloaded(parent_stack, clone_path)
            return True
        Printer.hashtaged(PrintChannel.WARNING, f'ATTEMPT TO CLONE {self.clsn.upper()} "{self}" FAILED\n' + 
                                                f'FALLING BACK TO REDOWNLOAD\n' +
                                                f'EXPECTED SOURCE FILES THAT DO NOT EXIST:')
        Printer.json_dump(self.real_filepaths)
        return False
    
    def clone_to_all(self) -> bool:
        """ Attempt to clone all and return if all clones succeeded """
        for ps in self._clone_to:
            if ps in self.real_filepaths: continue
            if not self.clone_file(ps):
                return False
        return True


class Track(DLContent):
    _regex_flag = Zotify.CONFIG.get_regex_track()
    _to_str_attrs = [ARTISTS, NAME]
    _codecs = CODEC_MAP_TRACK
    _ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "ogg")
    url = TRACK_URL
    
    def __init__(self, uri: str) -> None:
        super().__init__(uri)
        self.disc_number    : int                   = None
        self.track_number   : str                   = None
        self.album          : Album                 = None
        self.artists        : list[Artist]          = None
        
        # only fetched if config set
        self.genres         : list[str]             = None
        self.lyrics         : list[str]             = None
        
        # only set by Playlist API or UserItem API
        self.added_at       : dict[Container, str]  = {}
        # only set by Playlist API
        self.added_by       : dict[Playlist, User]  = {}
        self.is_local       : dict[Playlist, str]   = {}
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["track_number", "artists", "album"], suppress_id=suppress_id)
    
    def fill_output_template(self, parent_stack: ParentStack, output_template: str = "") -> PurePath:
        path = super().fill_output_template(parent_stack)
        if path: return path
        
        parent: Container = parent_stack[-2]
        if not output_template:
            try:
                output_template = Zotify.CONFIG.get_output(parent.clsn)
            except:
                Printer.debug(f"Unexpected Track Parent: {parent.clsn}")
                output_template = Zotify.CONFIG.get_output('Query')
        
        repl_dict: dict[str, str] = {}
        def update_repl(md_val, *replstrs: str):
            repl_dict.update(zip(replstrs, [md_val]*len(replstrs)))
        
        update_repl(self.id,            "{id}", "{track_id}", "{song_id}")
        update_repl(self.name,          "{name}", "{song_name}", "{track_name}", "{song_title}", "{track_title}")
        update_repl(self.track_number,  "{track_number}", "{song_number}", "{track_num}", "{song_num}", "{album_number}", "{album_num}")
        update_repl(self.disc_number,   "{disc_number}", "{disc_num}")
        
        if self.artists:
            artists_names = conv_artist_format(self.artists, FORCE_NO_LIST=True)
            update_repl(self.artists[0].name,   "{artist}", "{track_artist}", "{song_artist}", "{main_artist}", "{primary_artist}")
            update_repl(artists_names,          "{artists}", "{track_artists}", "{song_artists}")
        
        if self.album:
            album_artists_names = conv_artist_format(self.album.artists, FORCE_NO_LIST=True)
            update_repl(self.album.id,              "{album_id}")
            update_repl(self.album.name,            "{album}", "{album_name}")
            update_repl(self.album.artists[0].name, "{album_artist}")
            update_repl(album_artists_names,        "{album_artists}")
            update_repl(self.album.release_date,    "{date}", "{release_date}")
            update_repl(self.album.year,            "{year}", "{release_year}")
        
        if Zotify.CONFIG.get_disc_track_totals():
            if self.album.needs_expansion:
                self.album.grab_more_children(hide_loader=True) # moved from Query.fetch_extra_metadata()
            update_repl(self.album.total_tracks,    "{total_tracks}")
            update_repl(self.album.total_discs,     "{total_discs}")
        
        if isinstance(parent, Playlist):
            playlist_number = str(parent.tracks_or_eps.index(self) + 1).zfill(2)
            update_repl(parent.name,        "{playlist}")
            update_repl(parent.id,          "{playlist_id}")
            update_repl(playlist_number,    "{playlist_number}", "{playlist_num}")
        
        for replstr, md_val in repl_dict.items():
            output_template = output_template.replace(replstr, fix_filename(md_val))
        
        return Zotify.CONFIG.get_root_path() / f"{output_template}.{self._ext}"
    
    def check_skippable(self, parent_stack: ParentStack) -> bool:      
        self.skippable = super().check_skippable(parent_stack)
        if self.skippable: return self.skippable
        
        if self.album:
            self.skippable = self.album.check_skippable(parent_stack)
        
        return self.skippable
    
    def fetch_lyrics(self, parent_stack: ParentStack) -> None:
        if self.lyrics:
            return
        elif Zotify.CONFIG.get_bypass_metadata():
            return
        elif not Zotify.CONFIG.get_lyrics_to_file() and not Zotify.CONFIG.get_lyrics_to_metadata():
            return
        
        try:
            with Loader("Fetching lyrics..."):
                # expect failure here, lyrics are not guaranteed to be available
                _, lyrics_dict = Zotify.invoke_url(LYRICS_URL + self.id, expectFail=True)
                if not lyrics_dict:
                    raise ValueError('FAILED TO FETCH')
                try:
                    formatted_lyrics = lyrics_dict[LYRICS][LINES]
                except KeyError:
                    raise ValueError('LYRICS NOT AVAILABLE')
                
                if lyrics_dict[LYRICS][SYNCTYPE] == UNSYNCED:
                    lyrics = [line[WORDS] + '\n' for line in formatted_lyrics]
                elif lyrics_dict[LYRICS][SYNCTYPE] == LINE_SYNCED :
                    lyrics = []
                    tss = []
                    for line in formatted_lyrics:
                        timestamp = int(line[STARTTIMEMS]) // 10
                        ts = fmt_duration(timestamp // 1, (60, 100), (':', '.'), "cs", True)
                        tss.append(f"{timestamp}".zfill(5) + f" {ts.split(':')[0]} {ts.split(':')[1].replace('.', ' ')}\n")
                        lyrics.append(f'[{ts}]' + line[WORDS] + '\n')
                    # Printer.debug("Synced Lyric Timestamps:\n" + "".join(tss))
                else:
                    raise ValueError('UNKNOWN SYNC TYPE')
                
                self.lyrics = lyrics
        except ValueError as e:
            Printer.hashtaged(PrintChannel.SKIPPING, f'LYRICS FOR "{self}" ({e.args[0]})')
            return
        
        if Zotify.CONFIG.get_lyrics_to_file():
            lyricdir = Zotify.CONFIG.get_lyrics_location()
            if lyricdir is None:
                lyricdir = self.fill_output_template(parent_stack).parent
            Path(lyricdir).mkdir(parents=True, exist_ok=True)
            
            lrc_filename = self.fill_output_template(parent_stack, Zotify.CONFIG.get_lyrics_filename()).stem
            
            with open(lyricdir / f"{lrc_filename}.lrc", 'w', encoding='utf-8') as file:
                if Zotify.CONFIG.get_lyrics_header():
                    lrc_header = [f"[ti: {self.name}]\n",
                                  f"[ar: {conv_artist_format(self.artists, FORCE_NO_LIST=True)}]\n",
                                  f"[al: {self.album.name}]\n",
                                  f"[length: {self.duration_ms // 60000}:{(self.duration_ms % 60000) // 1000}]\n",
                                  f"[by: Zotify v{__version__}]\n",
                                  "\n"]
                    file.writelines(lrc_header)
                file.writelines(self.lyrics)
    
    def write_audio_tags(self, filepath: PurePath) -> None:
        file_tags: music_tag.AudioFile = music_tag.load_file(filepath)
        img = None # expect jpeg
        
        def set_tag_safe(FILETAG, tag_value):
            if not tag_value:
                return
            try:
                file_tags[FILETAG] = tag_value
            except Exception as e:
                Printer.hashtaged(PrintChannel.WARNING, f'FAILED TO SET TAG {FILETAG} TO "{tag_value}" FOR "{self.rel_path()}"\n' +
                                                        f'ERROR: {str(e)}')
        
        def custom_tag(tag: str, val: str):
            def custom_mp3_tag(tag: str, val: str):
                from mutagen.id3 import TXXX
                file_tags.mfile.tags.add(TXXX(encoding=3, desc=tag.upper(), text=[val]))
            
            def custom_m4a_tag(tag: str, val: str):
                from music_tag.mp4 import freeform_set
                atomic_tag = M4A_CUSTOM_TAG_PREFIX + tag
                freeform_set(file_tags, atomic_tag, type('tag', (object,), {'values': [val]})())
            
            def custom_ogg_tag(tag: str, val: str):
                from music_tag.file import TAG_MAP_ENTRY
                file_tags.tag_map[tag] = TAG_MAP_ENTRY(getter=tag, setter=tag, type=type(val))
                set_tag_safe(tag, val)
            
            if self._ext == "mp3":
                custom_mp3_tag(tag, val)
            elif self._ext == "m4a":
                custom_m4a_tag(tag, val)
            else:
                custom_ogg_tag(tag, val)
        
        # Reliable Tags
        set_tag_safe(       ARTIST,         conv_artist_format(self.artists))
        set_tag_safe(       GENRE,          conv_genre_format(self.genres))
        set_tag_safe(       TRACKTITLE,     self.name)
        set_tag_safe(       DISCNUMBER,     self.disc_number)
        set_tag_safe(       TRACKNUMBER,    self.track_number)
        if self.album:
            set_tag_safe(   ALBUM,          self.album.name)
            set_tag_safe(   ALBUMARTIST,    conv_artist_format(self.album.artists))
            set_tag_safe(   YEAR,           self.album.year)
            img =                           requests.get(self.album.image_url).content
            set_tag_safe(   ARTWORK,        img)
        
        # Unreliable Tags
        custom_tag(         TRACKID,        self.id)
        custom_tag(         URI,            self.uri)
        
        if self.album and Zotify.CONFIG.get_disc_track_totals():
            set_tag_safe(TOTALTRACKS,    self.album.total_tracks)
            set_tag_safe(TOTALDISCS,     self.album.total_discs)
        
        if self.album and self.album.compilation:
            set_tag_safe(COMPILATION,    self.album.compilation)
        
        if self.lyrics and Zotify.CONFIG.get_lyrics_to_metadata():
            set_tag_safe(LYRICS,         "".join(self.lyrics))
        
        if self._ext == "mp3" and not Zotify.CONFIG.get_disc_track_totals() and self.disc_number and self.track_number:
            # music_tag python library writes DISCNUMBER and TRACKNUMBER as X/Y instead of X for mp3
            # this method bypasses all internal formatting, probably not resilient against arbitrary inputs
            file_tags.set_raw("mp3", "TPOS", str(self.disc_number))
            file_tags.set_raw("mp3", "TRCK", str(self.track_number))
        
        file_tags.save()
        
        # save trach image art to file
        if not Zotify.CONFIG.get_album_art_jpg_file() or img is None:
            return
        jpg_path = filepath.parent / ('cover.jpg' if isinstance(self.parent, Album) else filepath.stem + '.jpg')
        if not Path(jpg_path).exists():
            with open(jpg_path, 'wb') as jpg_file:
                jpg_file.write(img)
    
    def download(self, parent_stack: ParentStack) -> None:
        if not Zotify.CONFIG.get_optimized_dl():
            if Zotify.CONFIG.get_download_parent_album():
                with Zotify.CONFIG.temporary_config(DOWNLOAD_PARENT_ALBUM, False):
                    self.album.download(ParentStack([parent_stack[0], self.album]))
                return
            elif self.downloaded and self.clone_file(parent_stack):
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} ALREADY DOWNLOADED THIS SESSION)\n' + 
                                                         f'FILE COPIED TO NEW DESTINATION "{self.rel_path(parent_stack)}"')
                return
        elif Zotify.CONFIG.get_optimized_dl() and self.downloaded:
            if self.clone_to_all(): return
        
        if Zotify.CONFIG.get_always_check_lyrics():
            self.fetch_lyrics(parent_stack)
        
        if parent_stack.check_skippable():
            return
        
        Interface.bind(parent_stack)
        with self.set_dl_status("Preparing Download"):
            path = check_path_dupes(self.fill_output_template(parent_stack))
            if path != self.fill_output_template(parent_stack): # path exists but id isn't archived OR skipping disabled
                Printer.debug('Path Duplicate Not Being Skipped:\n' +
                              'ID not Archived' if Zotify.CONFIG.get_skip_existing() else 'Skipping Disabled')
            temppath = path.with_suffix(".tmp")
            if Zotify.CONFIG.get_temp_download_dir():
                temppath = Zotify.CONFIG.get_temp_download_dir() / f'zotify_{str(uuid.uuid4())}_{self.id}.tmp'
        
        stream = Zotify.get_content_stream(self)
        if stream is None:
            Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING TRACK - FAILED TO GET CONTENT STREAM\n' +
                                                 f'Track_ID: {self.id}')
            return
        
        self.set_dl_status("Downloading Stream")
        time_elapsed_dl = self.fetch_content_stream(stream, temppath, parent_stack)
        
        if not Zotify.CONFIG.get_always_check_lyrics():
            self.fetch_lyrics(parent_stack)
        
        with self.set_dl_status("Converting File"):
            create_download_directory(path.parent)
            time_elapsed_ffmpeg = self.convert_audio_format(temppath, path) # temppath -> path here
            if time_elapsed_ffmpeg is None:
                path = pathlike_move_safe(temppath, path.with_suffix(".ogg"))
            self.mark_downloaded(parent_stack, path)
        
        try:
            self.write_audio_tags(path)
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO WRITE METADATA\n' +
                                                  'Ensure FFMPEG is installed and added to your PATH')
            Printer.traceback(e)
        
        Printer.dl_complete(self, path, time_elapsed_dl, time_elapsed_ffmpeg)
        if not Zotify.CONFIG.get_bypass_metadata():
            if not self.in_dir_archive:
                add_obj_to_song_archive(self, path, path.parent)
            if not self.in_global_archive:
                add_obj_to_song_archive(self, path)
        
        if Zotify.CONFIG.get_optimized_dl(): self.clone_to_all()
        wait_between_downloads()
    
    @staticmethod
    def read_audio_tags(filepath: PurePath) -> tuple[tuple, dict]:
        tags = music_tag.load_file(filepath)
        
        artists = conv_artist_format(tags[ARTIST].values)
        genres = conv_genre_format(tags[GENRE].values)
        track_name = tags[TRACKTITLE].val
        album_name = tags[ALBUM].val
        album_artist = conv_artist_format(tags[ALBUMARTIST].values)
        release_year = tags[YEAR].val
        disc_number = tags[DISCNUMBER].val
        track_number = tags[TRACKNUMBER].val
        
        unreliable_tags = [TOTALTRACKS, TOTALDISCS, COMPILATION, LYRICS]
        custom_tags = [TRACKID, URI]
        if filepath.suffix.lower() == ".mp3":
            formatted_custom_tags = [MP3_CUSTOM_TAG_PREFIX + tag.upper() for tag in custom_tags]
        elif filepath.suffix.lower() == ".m4a":
            formatted_custom_tags = [M4A_CUSTOM_TAG_PREFIX + tag for tag in custom_tags]
        else:
            formatted_custom_tags = custom_tags.copy()
        taglabels = unreliable_tags + formatted_custom_tags
        
        tag_dict = dict(tags.mfile.tags)
        # Printer.debug(tags.mfile.tags.__dict__)
        def fetch_unreliable_tag(utag: str):
            val = None
            try:
                fetch_method = "legit"
                val = tags[utag].val
            except:
                fetch_method = "hacky"
                if utag in tag_dict: val = tag_dict[utag]
            
            if val is None:                         pass
            elif utag == LYRICS:                    val = [line + "\n" for line in val.splitlines()] if val else None
            elif utag == COMPILATION:               val = bool(val)
            elif MP3_CUSTOM_TAG_PREFIX in utag:     val = val.text[0]     if len(val.text) == 1 else val.text
            elif M4A_CUSTOM_TAG_PREFIX in utag:     val = val[0].decode() if len(val) == 1      else [v.decode() for v in val]
            else:
                val = val[0] if isinstance(val, (list, tuple)) and len(val) == 1 else val
                val = val if val else None
            Printer.logger(f"{fetch_method} {utag} {val}", PrintChannel.DEBUG)
            return val
        
        utag_vals = {}
        for taglabel, utag in zip(taglabels, unreliable_tags + custom_tags):
            utag_vals[utag] = fetch_unreliable_tag(taglabel)
        
        return (artists, genres, track_name, album_name, album_artist, release_year, disc_number, track_number), \
                utag_vals
    
    def compare_metadata(self, filepath: PurePath):
        """ Compares metadata in self (just fetched) against metadata on file\n
            returns Truthy value if discrepancy is found """
        
        reliable_tags = (
            conv_artist_format(self.artists), conv_genre_format(self.genres), self.name, self.album.name, 
            conv_artist_format(self.album.artists), self.album.year, int(self.disc_number), int(self.track_number)
            )
        unreliable_tags = {
            TOTALTRACKS: int(self.album.total_tracks) if Zotify.CONFIG.get_disc_track_totals() else None,
            TOTALDISCS: int(self.album.total_discs) if Zotify.CONFIG.get_disc_track_totals() else None,
            COMPILATION: self.album.compilation,
            LYRICS: self.lyrics,
            TRACKID: self.id,
            URI: self.uri
            }
        reliable_tags_onfile, unreliable_tags_onfile = self.read_audio_tags(filepath)
        
        mismatches = []
        # Definite tags must match
        if len(reliable_tags) != len(reliable_tags_onfile):
            if not Zotify.CONFIG.debug():
                return True
        
        for i in range(len(reliable_tags)):
            if isinstance(reliable_tags[i], list) and isinstance(reliable_tags_onfile[i], list):
                if sorted(reliable_tags[i]) != sorted(reliable_tags_onfile[i]):
                    mismatches.append( (reliable_tags[i], reliable_tags_onfile[i]) )
            else:
                if str(reliable_tags[i]) != str(reliable_tags_onfile[i]):
                    mismatches.append( (reliable_tags[i], reliable_tags_onfile[i]) )
        
        if mismatches:
            return mismatches
        
        # If more unreliable tags are received from API than found on file, assume the file is outdated
        if sum([bool(tag) for tag in unreliable_tags]) > sum([bool(tag) for tag in unreliable_tags_onfile]):
            if not Zotify.CONFIG.get_strict_library_verify() and not Zotify.CONFIG.debug():
                return True
        
        # stickler check for unreliable tags
        for tag in unreliable_tags:
            if tag not in unreliable_tags_onfile:
                mismatches.append({tag: (unreliable_tags[tag], None)})
                continue
            t1 = unreliable_tags[tag]; t2 = unreliable_tags_onfile[tag]
            if isinstance(t1, list) and isinstance(t2, list):
                # do not sort lyrics, since order matters
                if t1 != t2: mismatches.append({tag: (t1, t2)})
            else:
                if str(t1) != str(t2): mismatches.append({tag: (t1, t2)})
        
        return mismatches


class Episode(DLContent):
    _path_root: PurePath = Zotify.CONFIG.get_root_podcast_path()
    _regex_flag = Zotify.CONFIG.get_regex_episode()
    _to_str_attrs = [SHOW, NAME]
    _codecs = CODEC_MAP_EPISODE
    _ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "copy")
    url = EPISODE_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.description            : str       = None
        self.explicit               : bool      = None
        self.is_externally_hosted   : bool      = None
        self.partner_url            : str       = None
        self.release_date           : str       = None
        self.show                   : Show      = None
        
        # only set by Playlist API
        self.added_at       : dict[Playlist, str]   = {}
        self.added_by       : dict[Playlist, User]  = {}
        self.is_local       : dict[Playlist, str]   = {}
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["show",], suppress_id=suppress_id)
    
    def fill_output_template(self, parent_stack: list[Container], output_template: str = "") -> PurePath:
        path = super().fill_output_template(parent_stack)
        if path: return path
        return self._path_root / fix_filename(self.show.name) / f"{self}.{self._ext}"
    
    def fetch_partner_url(self) -> str | None:
        _, resp = Zotify.invoke_url(PARTNER_URL + self.id + '"}&extensions=' + PERSISTED_QUERY, force_login5=False)
        if resp[DATA][EPISODE] is None:
            Printer.hashtaged(PrintChannel.WARNING, 'EPISODE PARTNER DATA MISSING - ASSUMING PLATFORM HOSTED\n' +
                                                   f'Episode_ID: {self.id}')
            return None
        direct_download_url = resp[DATA][EPISODE][AUDIO][ITEMS][-1][URL]
        if STREAMABLE_PODCAST not in direct_download_url and "audio_preview_url" in resp:
            self.partner_url = direct_download_url
        return self.partner_url
    
    def download_directly(self, path: PurePath) -> str:
        time_start = time.time()
        
        r = requests.get(self.partner_url, stream=True, allow_redirects=True)
        if r.status_code != 200:
            r.raise_for_status()  # Will only raise for 4xx codes, so...
            raise RuntimeError(f"Request to {self.partner_url} returned status code {r.status_code}")
        file_size = int(r.headers.get('Content-Length', 0))
        desc = "" if file_size else "(Unknown total file size)"
        
        path = Path(path).expanduser().resolve()
        r.raw.read = functools.partial(r.raw.read, decode_content=True)  # Decompress if needed
        with Printer.pbar_stream(r.raw, desc=desc, total=file_size) as r_raw:
            pathlike_move_safe(r_raw, path)
        
        time_dl_end = time.time()
        return fmt_duration(time_dl_end - time_start)
    
    def download(self, parent_stack: ParentStack):
        if not Zotify.CONFIG.get_optimized_dl() and self.downloaded and self.clone_file(parent_stack):
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} ALREADY DOWNLOADED THIS SESSION)\n' +
                                                     f'FILE COPIED TO NEW DESTINATION "{self.rel_path(parent_stack)}"')
            return
        elif Zotify.CONFIG.get_optimized_dl() and self.downloaded:
            if self.clone_to_all(): return
        elif parent_stack.check_skippable():
            return
        
        Interface.bind(parent_stack)
        with self.set_dl_status("Preparing Download"):
            path = check_path_dupes(self.fill_output_template(parent_stack))
            if path != self.fill_output_template(parent_stack): # path exists but id isn't archived OR skipping disabled
                Printer.debug('Path Duplicate Not Being Skipped:\n' +
                              'ID not Archived' if Zotify.CONFIG.get_skip_existing() else 'Skipping Disabled')
            temppath = path.with_suffix(".tmp")
            if Zotify.CONFIG.get_temp_download_dir():
                temppath = Zotify.CONFIG.get_temp_download_dir() / f'zotify_{str(uuid.uuid4())}_{self.id}.tmp'
        
        self.set_dl_status("Downloading Stream")
        if not self.fetch_partner_url():
            stream = Zotify.get_content_stream(self)
            if stream is None:
                Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING EPISODE - FAILED TO GET CONTENT STREAM\n' +
                                                     f'Episode_ID: {self.id}')
                return
            time_elapsed_dl = self.fetch_content_stream(stream, temppath)
        else:
            try:
                time_elapsed_dl = self.download_directly(temppath)
            except Exception as e:
                Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO DOWNLOAD EPISODE DIRECTLY')
                Printer.traceback(e)
                return
        
        try:
            with self.set_dl_status("Identifying Episode Audio Codec"):
                codec = self.get_audio_codec(temppath)
                ext = "." + EXT_MAP.get(codec, codec)
            Printer.debug(f'Detected Codec: {codec}\n' +
                          f'File Extension Matched to: {ext}')
        except Exception as e:
            # assume default codec since that's what the original library did
            ext = ".mp3"
            if isinstance(e, ffmpy.FFExecutableNotFoundError):
                Printer.hashtaged(PrintChannel.WARNING, 'FFMPEG NOT FOUND\n'+
                                                        'SKIPPING CODEC ANALYSIS - OUTPUT ASSUMED MP3')
            else:
                Printer.hashtaged(PrintChannel.WARNING, 'UNKNOWN ERROR\n' +
                                                        'SKIPPING CODEC ANALYSIS - OUTPUT ASSUMED MP3')
                Printer.traceback(e)
        if path.suffix == ".copy":
            path = path.with_suffix(ext)
        
        with self.set_dl_status("Converting File"):
            create_download_directory(path.parent)
            time_elapsed_ffmpeg = self.convert_audio_format(temppath, path)
            if time_elapsed_ffmpeg is None:
                path = pathlike_move_safe(temppath, path.with_suffix(ext))
            self.mark_downloaded(parent_stack, path)
        
        Printer.dl_complete(self, path, time_elapsed_dl, time_elapsed_ffmpeg)
        if not Zotify.CONFIG.get_bypass_metadata() and not self.in_dir_archive:
            add_obj_to_song_archive(self, path, path.parent)
        
        if Zotify.CONFIG.get_optimized_dl(): self.clone_to_all()
        wait_between_downloads()


class Container(Content):
    _show_pbar = not Zotify.CONFIG.get_standard_interface()
    _contains = Content
    _preloaded = 0
    _fetch_q = 50
    _nextable = True
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self._main_items: list[DLContent | Container | None]   = []
        self.needs_expansion = False
        self.needs_recursion = False
    
    def fetch_items(self, args: list[str] = [], hide_loader: bool = False) -> list[dict]:
        item_key = self._contains[0].lowers if isinstance(self._contains, tuple) else self._contains.lowers
        with Loader(f'Fetching {self.type_attr} {item_key}...', disabled=hide_loader):
            argstr = ""
            if self._fetch_args: argstr += "&" + self._fetch_args
            if args: argstr += "&" + "&".join(args)
            if self._nextable:
                resp = Zotify.invoke_url_nextable(f'{self.url}/{self.id}/{item_key.replace(" ", "-")}?{MARKET_APPEND}{argstr}',
                                                # limit=self._fetch_q, offset=self._next_index())
                                                limit=self._fetch_q, offset=len(self._main_items))
            else:
                _, resp = Zotify.invoke_url(f'{self.url}/{self.id}/{item_key.replace(" ", "-")}?{MARKET_APPEND}{argstr}')
                _, resp = resp.popitem()
            return resp
    
    def recurse_DLC(self) -> list[DLContent]:
        dlc = []
        for c in self._main_items:
            dlc.append(c) if isinstance(c, DLContent) else dlc.extend(c.recurse_DLC())
        return dlc
    
    def grab_more_children(self, hide_loader: bool = False) -> list[dict]:
        # assumes all items inside objs are the same class
        item_resps = self.fetch_items(hide_loader=hide_loader)
        item_objs = self.parse_relatives(item_resps, self._contains)
        self._main_items.extend(item_objs)
        self.needs_expansion = False
    
    def pbar(self, items: list[DLContent | Container | None], ps: ParentStack) -> list[DLContent | Container]:
        real_items: list[DLContent | Container] = [c for c in items if c is not None]
        if not real_items: return []
        parent: DLContent | Container = ps[-1]
        unit = "Content" if isinstance(parent._contains, tuple) else parent._contains.__name__
        if isinstance(parent, Query) and not isinstance(parent, UserItem): # avoid overwriting UserItem._contains
            unit = "Content" if Zotify.CONFIG.get_optimized_dl() else "URL"
        pbar: list[DLContent | Container] = Printer.pbar(real_items, parent.name, unit=unit, default_pos=7,
                                                         disable=not parent._show_pbar, pbar_stack=ps.PBARS)
        ps.PBARS.append(pbar)
        return pbar
    
    def download(self, parent_stack: ParentStack):
        # if not self._main_items:
        #     Printer.hashtaged(PrintChannel.WARNING, f'CONTAINER "{self.name}" HAS NO ITEMS\n' +
        #                                             f'(EXPECTING TYPE {self._contains.clsn})')
        #     return
        
        for item in self.pbar(self._main_items, parent_stack):
            parent_stack.extend([item])
            item.download(parent_stack)
            parent_stack.pop()
            Printer.refresh_all_pbars(parent_stack.PBARS)
        self.mark_downloaded()


class Playlist(Container):
    _show_pbar = Zotify.CONFIG.get_show_playlist_pbar()
    _to_str_attrs = [OWNER, NAME]
    _contains = (Track, Episode)
    _preloaded = 100
    _fetch_q = 100
    _fetch_args = "additional_types=track%2Cepisode"
    
    url = PLAYLIST_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.collaborative  : bool                      = None
        self.description    : str                       = None
        self.image_url      : str                       = None
        self.public         : bool                      = None
        self.snapshot_id    : str                       = None
        self.owner          : User                      = None
        self.tracks_or_eps  : list[Track | Episode]     = self._main_items
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        playlist_items: list[dict[str, dict]] = super().fetch_items(hide_loader=hide_loader)
        for item in playlist_items:
            if not item.get(TRACK): continue
            item[TRACK][ADDED_AT] = item.get(ADDED_AT)
            item[TRACK][ADDED_BY] = item.get(ADDED_BY)
            item[TRACK][IS_LOCAL] = item.get(IS_LOCAL)
        track_or_episode_resps = [item[TRACK] if item.get(TRACK) and item[TRACK].get(URI) else None for item in playlist_items]
        return track_or_episode_resps


class User(Container):
    _contains = Playlist
    def __init__(self, uri: str):
        super().__init__(uri)
        self.display_name   : str   = None
        self.external_urls  : dict  = None


class Album(Container):
    _regex_flag = Zotify.CONFIG.get_regex_album()
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _to_str_attrs = [ARTISTS, NAME]
    _contains = Track
    _preloaded = 50
    url = ALBUM_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.album_type     : str                   = None
        self.compilation    : bool                  = None
        self.duration_ms    : int                   = None
        self.image_url      : str                   = None
        self.label          : str                   = None
        self.release_date   : str                   = None
        self.total_discs    : int                   = None
        self.total_tracks   : str                   = None
        self.year           : str                   = None
        self.artists        : list[Artist]          = None
        self.tracks         : list[Track]           = self._main_items
        
        # only set by Artist Albums API
        self.album_group    : dict[Container, str]  = {}
        # only set by UserItem API
        self.added_at       : dict[Container, str]  = {}
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["total_tracks", "artists"], suppress_id=suppress_id, force_clsn=True)
    
    def grab_more_children(self, hide_loader: bool = False) -> list[dict]:
        super().grab_more_children(hide_loader=hide_loader)
        self.total_discs = str(self.tracks[-1].disc_number)
        self.duration_ms = sum((int(t.duration_ms) for t in self.tracks))
    
    def check_skippable(self, parent_stack: ParentStack) -> bool:
        self.skippable = super().check_skippable(parent_stack)
        if self.skippable: return self.skippable
        
        discog_artist = next((p for p in parent_stack if isinstance(p, Artist)), None)
        album_group = self.album_group.get(discog_artist)
        if album_group:
            if Zotify.CONFIG.get_skip_comp_albums() and album_group == COMPILATION:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ARTIST ONLY COMPILED INTO ALBUM)')
                self.skippable = True
            elif Zotify.CONFIG.get_skip_appears_on_album() and album_group == APPEARS_ON:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ARTIST ONLY APPEARS ON ALBUM)')
                self.skippable = True
            elif Zotify.CONFIG.get_discog_by_album_artist() and self.artists[0].name == discog_artist.name:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ARTIST NOT ALBUM ARTIST)')
                self.skippable = True
            if self.skippable: return self.skippable
        
        if Zotify.CONFIG.get_skip_comp_albums() and self.compilation:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (COMPILATION ALBUM)')
            self.skippable = True
        elif Zotify.CONFIG.get_skip_various_artists() and "".join(self.artists[0].name.lower().split()) == "variousartists":
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ALBUM OF VARIOUS ARTISTS)')
            self.skippable = True
        
        return self.skippable


class Artist(Container):
    _show_pbar = Zotify.CONFIG.get_show_artist_pbar()
    _to_str_attrs = [NAME, FOLLOWERS, GENRES]
    _toptrackmode: bool = False # Zotify.get_artist_fetch_top_tracks(), not implemented
    _contains = Album if not _toptrackmode else TopTrack
    _fetch_q = 20 if not _toptrackmode else 100
    _nextable = not _toptrackmode
    url = ARTIST_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.needs_expansion = True
        self.needs_recursion = not self._toptrackmode
        
        self.followers      : int               = None
        self.albums         : list[Album]       = self._main_items if not self._toptrackmode else None
        self.genres         : list[str]         = None
        self.top_tracks     : list[TopTrack]    = self._main_items if self._toptrackmode else None
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["genres"], suppress_id=suppress_id)


class Show(Container):
    _path_root: PurePath = Zotify.CONFIG.get_root_podcast_path()
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _to_str_attrs = [PUBLISHER, NAME]
    _contains = Episode
    _preloaded = 50
    url = SHOW_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.description            : str               = None
        self.explicit               : bool              = None
        self.is_externally_hosted   : bool              = None
        self.image_url              : str               = None
        self.publisher              : str               = None
        self.total_episodes         : str               = None
        self.episodes               : list[Episode]     = self._main_items
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["total_episodes",], suppress_id=suppress_id)


# start not implemented
class TopTrack(Track):
    pass


class Chapter(DLContent):
    url = CHAPTER_URL


class Audiobook(Container):
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _contains = Chapter
    _preloaded = 50
    url = AUDIOBOOK_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.chapters       : list[Chapter]         = self._main_items
# end not implemented


ITEM_FETCH: dict[type[DLContent] | type[Container], int] = {
    Playlist:   0,
    Artist:    50,
    Album:     20,
    Audiobook: 50,
    Show:      50,
    Chapter:   50,
    Episode:   50,
    Track:    100
}


class ParentStack(list):
    """ Will contain DLContent as last item in self if possible """
    PBARS = []
    
    def __hash__(self: ParentStack | list[Content]):
        return hash("&".join(c.uri for c in self))
    
    def __eq__(self: ParentStack | list[Content], other: ParentStack | list[Content]):
        return len(self) == len(other) and all(a == b for a, b in zip(self, other))
    
    def __str__(self: ParentStack | list[Content]) -> str:
        return f"[{' -> '.join([c.clsn for c in self])}]"
    
    def check_skippable(self: ParentStack | list[Content]) -> bool:
        return any(c.check_skippable(self) for c in self[::-1])
    
    def download(self: ParentStack | list[DLContent | Container], _: ParentStack):
        self[-1].download(self)


class Query(Container):
    _root_node = True
    _show_pbar = Zotify.CONFIG.get_show_url_pbar()
    name = "Total Progress"
    
    def __init__(self, timestamp: str):
        super().__init__(f"{self.type_attr}:{timestamp}" )
        del self.name
        
        self.requested_urls = "" # for debug only
        self.parsed_request : list[list[str]]                                   = []
        self.requested_objs : list[list[DLContent | Container]]                 = []
        self._main_items    : list[DLContent | Container] | list[ParentStack]   = []
    
    def request(self, requested_urls: str) -> Query:
        self.requested_urls = requested_urls # only used here, can remove later
        self.parsed_request = bulk_regex_urls(self.requested_urls)
        n_urls = len(set.union(*[set(l) for l in self.parsed_request]))
        Printer.debug(f'Request Parsed as {n_urls} URL' + ("s" if n_urls > 1 else ""))
        return self
    
    def fetch_uris_metadata(self, uris: list[str], cont_type: type[Content], hide_loader: bool = False) -> list[dict]:
        if not uris:
            return []
        elif Zotify.CONFIG.get_bypass_metadata():
            return [{URI: uri, TYPE: cont_type.type_attr} for uri in uris]
        elif cont_type is Playlist:
            return [self.make_or_link_relative(uri, Playlist).fetch_metadata() for uri in uris]
        else:
            with Loader(f"Fetching bulk {cont_type.type_attr} information...", disabled=hide_loader):
                url = f"{cont_type.url}?{MARKET_APPEND}&{BULK_APPEND}"
                ids = [uri.split(":")[-1] for uri in uris]
                return Zotify.invoke_url_bulk(url, ids, cont_type.lowers, ITEM_FETCH[cont_type])
    
    def fetch_query_metadata(self) -> list[list[dict]]:
        item_resps_by_type: list[list[dict]] = []
        for uris, cont_type in zip(self.parsed_request, ITEM_FETCH):
            item_resps_by_type.append(self.fetch_uris_metadata(uris, cont_type))
        return item_resps_by_type
    
    def parse_query_metadata(self, item_resps_by_type: list[list[dict]], item_types: list[type[Content]] = ITEM_FETCH,
                             hide_loader: bool = False) -> None:
        """ Writes list[list[Content]] to self.requested_objs """
        for item_resps, item_type in zip(item_resps_by_type, item_types):
            if not item_resps:
                self.requested_objs.append([])
                continue
            
            with Loader(f"Parsing {item_type.type_attr} information...", disabled=hide_loader):
                objs: list[Content | Container] = self.parse_relatives(item_resps, item_type)
                self.requested_objs.append(objs)
                
                if not objs or not any(objs):
                    continue
                elif Zotify.CONFIG.get_bypass_metadata() or not isinstance(objs[0], Container):
                    continue
                
                for obj in objs:
                    if obj.needs_expansion: obj.grab_more_children(hide_loader=True)
                
                recurs_objs = [o for o in objs if isinstance(o, Container) and o.needs_recursion]
                if recurs_objs:
                    recurs_children: list[Container] = []
                    for recurs_obj in recurs_objs:
                        recurs_children.extend(recurs_obj._main_items)
                    recurse_type = recurs_objs[0]._contains
                    recurs_item_resps = self.fetch_uris_metadata([c.uri for c in recurs_children], recurse_type, hide_loader=True)
                    self.parse_query_metadata([recurs_item_resps], [recurse_type], hide_loader=True)
        return self.requested_objs
    
    def fetch_extra_metadata(self):
        alltracks = {t for t in self.ALL_NODES if isinstance(t, Track) and not t.local_file}
        
        artists = set().union(*(set(track.artists) for track in alltracks))
        artist_uris: dict[str, Artist] = {a.uri: a for a in artists if not a.local_file and not a.hasMetadata}
        if Zotify.CONFIG.get_save_genres() and artist_uris:
            with Loader(f"Fetching bulk genre information..."):
                artist_resps = self.fetch_uris_metadata(artist_uris.keys(), Artist, hide_loader=True)
                for artist, artist_resp in zip(artist_uris.values(), artist_resps):
                    artist.parse_metadata(None, artist_resp)
                    artist.needs_expansion = False
                for track in alltracks:
                    genres: list[str] = [*set().union(*[set(artist.genres) for artist in track.artists if artist.genres])]
                    genres.sort()
                    track.genres = genres
        
        albums = {track.album for track in alltracks if track.album and not track.album.local_file}
        album_uris: dict[str, Album] = {a.uri: a for a in albums if not a.hasMetadata}
        if (Zotify.CONFIG.get_disc_track_totals() or Zotify.CONFIG.get_download_parent_album()) and albums:
            loader_text = "parent album" if Zotify.CONFIG.get_download_parent_album() else "track/disc total"
            with Loader(f"Fetching bulk {loader_text} information..."):
                album_resps = self.fetch_uris_metadata(album_uris.keys(), Album, hide_loader=True)
                for album, album_resp in zip(album_uris.values(), album_resps):
                    album.parse_metadata(None, album_resp)
                    if album.needs_expansion: album.grab_more_children(hide_loader=True)
    
    def get_m3u8_dir(self, paths: set[PurePath | None], _path_root: PurePath, force_common_dir: bool = False) -> PurePath | None:
        m3u8_dir = Zotify.CONFIG.get_m3u8_location()
        if m3u8_dir and not force_common_dir: return m3u8_dir
        
        paths = {path for path in paths if isinstance(path, PurePath) and path.is_relative_to(_path_root)}
        if paths: return get_common_dir(paths)
    
    def create_m3u8_playlists(self, force_path: PurePath | None = None, force_name: str = "", append: list[str] = []) -> None:        
        def create_m3u8(obj_items: list[DLContent], filepaths: list[PurePath], m3u8_filename: str, print_name: str):
            m3u8_filename = force_name if force_name else fix_filename(m3u8_filename) + ".m3u8"
            m3u8_dir = self.get_m3u8_dir(filepaths, obj_items[0]._path_root)
            if m3u8_dir is None:
                Printer.hashtaged(PrintChannel.WARNING, f'SKIPPING M3U8 CREATION FOR "{m3u8_filename}"\n' +
                                                            'NO CONTENT WITH VALID FILEPATHS FOUND')
                return
            
            m3u8_path = fix_filepath(force_path, obj_items[0]._path_root) if force_path else m3u8_dir / m3u8_filename
            Path(m3u8_path).unlink(missing_ok=True)
            add_to_m3u8(m3u8_path, obj_items, filepaths, append)
            Printer.hashtaged(PrintChannel.MANDATORY, f'M3U8 CREATED FOR {print_name}\n' +
                                                      f'SAVED TO: {obj_items[0].rel_path(m3u8_path)}')
        
        for obj_list in self.requested_objs:
            if not obj_list: continue
            
            if not isinstance(obj_list[0], Container):
                filepaths = [dlc.real_filepaths.get(ParentStack([self, dlc])) for dlc in obj_list]
                create_m3u8(obj_list, filepaths, f"{self.id}_{obj_list[0].lowers}", obj_list[0].uppers)
                continue
            
            for obj in obj_list:
                if not obj._main_items:
                    Printer.hashtaged(PrintChannel.WARNING, f'SKIPPING M3U8 CREATION FOR "{obj.name}"\n' +
                                                            f'{obj.clsn.upper()} CONTAINS NO CONTENT')
                    continue
                
                dlc = obj.recurse_DLC()
                parent_stacks = [ps for c in dlc for ps in c.real_filepaths if obj == ps[1]]
                filepaths = [c.real_filepaths.get(ps) for c, ps in zip(dlc, parent_stacks)]
                create_m3u8(dlc, filepaths, obj.name, f'"{obj.name}"')
    
    def download(self):
        self._main_items = [c for content_type in self.requested_objs for c in content_type]
        if Zotify.CONFIG.get_optimized_dl():
            def build_parent_stacks(c: DLContent | Container) -> list[ParentStack]:
                if not isinstance(c, Container): return [[c]]
                return (ParentStack([c] + cs) for i in c._main_items for cs in build_parent_stacks(i))
            
            dlc_mapping: dict[DLContent, list[ParentStack]] = {}
            for ps in build_parent_stacks(self):
                dlc: DLContent | None = ps[-1]
                if dlc is None: continue
                elif dlc not in dlc_mapping: dlc_mapping[dlc] = [ps]
                else: dlc_mapping[dlc].append(ps)
            
            if Zotify.CONFIG.get_download_parent_album():
                tracks_with_albums: set[Track] = {t for t in dlc_mapping if isinstance(t, Track) and t.album}
                for t in tracks_with_albums: dlc_mapping[dlc].append(ParentStack([self, t.album, t]))
            
            downloadables = set()
            for dlc, pss in dlc_mapping.items():
                nonskipped = [ps for ps in pss if not ps.check_skippable()] # handles already downloaded
                if not nonskipped: continue
                downloadables.add(nonskipped.pop()) # prioritize parent album entry if present
                dlc._clone_to.update(nonskipped)
            
            downloadables = edge_zip(sorted(downloadables, key=lambda c: getattr(c[-1], DURATION_MS, 0)))
            if Zotify.CONFIG.get_download_parent_album():
                downloadables = sorted(downloadables, key=lambda c: getattr(getattr(c, ALBUM, Album("")), URI))
            self._main_items = downloadables
        
        if Zotify.CONFIG.get_standard_interface():
            Interface.reset(self.ALL_NODES)
        
        interrupt = None
        try: super().download(ParentStack([self]))
        except BaseException as e: interrupt = e
        
        while Printer.ACTIVE_LOADER:
            Printer.ACTIVE_LOADER.stop()
        n_pbars = len(Printer.ACTIVE_PBARS)
        while Printer.ACTIVE_PBARS:
            Printer.ACTIVE_PBARS.pop().close()
        if Zotify.CONFIG.get_show_any_progress() and n_pbars:
            Printer.back_up() # closing a visible pbar will print an extra newline
        
        if isinstance(interrupt, KeyboardInterrupt):
            Printer.hashtaged(PrintChannel.MANDATORY, "USER CANCELED DOWNLOADS EARLY\n"+
                                                      "ATTEMPTING TO CLEAN UP")
        elif interrupt is not None:
            Printer.hashtaged(PrintChannel.ERROR, "UNEXPECTED ERROR DURING DOWNLOADS\n"+
                                                  "ATTEMPTING TO CLEAN UP")
            Printer.traceback(interrupt)
        
        if Zotify.CONFIG.get_export_m3u8() and self.requested_objs and not Zotify.CONFIG.get_bypass_metadata():
            with Loader("Creating m3u8 files..."):
                self.create_m3u8_playlists()
        
        if interrupt is not None:
            Printer.hashtaged(PrintChannel.ERROR, "CLEAN UP COMPLETE\n"+
                                                  "LOGGING ERROR AND TRACEBACK")
            # Printer.traceback(interrupt)
            Printer.logger(interrupt, PrintChannel.ERROR)
            if not isinstance(interrupt, KeyboardInterrupt):
                Printer.logger(self.__dict__, PrintChannel.ERROR)
                raise interrupt
    
    def reset(self):
        HierarchicalNode.ALL_NODES = {}
        ParentStack.PBARS = []
    
    def execute(self):
        self.reset()
        self.parse_query_metadata(self.fetch_query_metadata())
        if not Zotify.CONFIG.get_bypass_metadata():
            self.fetch_extra_metadata()
        self.download()


class VerifyLibrary(Query):
    _contains = Track
    name = "Verifiable Tracks"
    
    def fetch_verifiable_metadata(self) -> tuple[dict[str, list[PurePath]], list[list[dict]]]:
        """ ONLY WORKS WITH ARCHIVED TRACKS (THEORETICALLY GUARANTEES BULK_URL TO WORK) """
        # prioritize most recent paths first
        archived_ids = get_archived_item_ids()[::-1]
        archived_filenames_or_paths = get_archived_item_paths()[::-1]
        
        paths_per_track: dict[str, list[PurePath]] = {}
        
        track_ids: set[str] = set()
        for filepath in walk_directory_for_tracks(Track._path_root):
            if filepath in archived_filenames_or_paths:
                uri = f"{TRACK}:{archived_ids[archived_filenames_or_paths.index(filepath)]}"
                if uri not in paths_per_track:
                    paths_per_track[uri] = []
                paths_per_track[uri].append(filepath)
                track_ids.add(uri)
        
        return paths_per_track, [self.fetch_uris_metadata(list(track_ids), Track)]
    
    def verify_metadata(self, path: PurePath, track: Track) -> None:
        """Overwrite metadata on file at path with fetched metadata if necessary"""
        mismatches = track.compare_metadata(path)
        if not mismatches:
            Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{track.rel_path(path)}"\n' +
                                                       '(NO UPDATES REQUIRED)')
            return
        
        try:
            Printer.debug(f'Metadata Mismatches:', mismatches)
            track.write_audio_tags(path)
            Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{track.rel_path(path)}"\n' +
                                                      f'(UPDATED TAGS TO MATCH CURRENT API METADATA)')
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, F'FAILED TO CORRECT METADATA FOR "{track.rel_path(path)}"')
            Printer.traceback(e)  
    
    def execute(self):
        self.reset()
        paths_per_track, track_resps = self.fetch_verifiable_metadata()
        self.parse_query_metadata(track_resps, [Track])
        self.fetch_extra_metadata()
        parent_stack = ParentStack([self])
        for track in self.pbar(self.requested_objs[0], parent_stack):
            for path in paths_per_track[track]:
                self.verify_metadata(path, track)
            Printer.refresh_all_pbars(parent_stack.PBARS)


class UserItem(Query):
    _contains = User
    interactive = True
    inner_stripper = None
    outer_stripper = None
    url = USER_URL
    
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self.name = self.clsn + "s"
    
    def fetch_user_items(self) -> list[dict]:
        with Loader(f"Fetching {self.name}...", disabled=self.interactive):
            user_item_resps = Zotify.invoke_url_nextable(f"{self.url}?{MARKET_APPEND}", stripper=self.outer_stripper)
        return user_item_resps
    
    def display_select_user_items(self, user_item_resps: list[dict]) -> list[dict]:
        display_list = [[i+1, str(resp.get(self.inner_stripper, resp)[NAME])] for i, resp in enumerate(user_item_resps)]
        Printer.table(self.uppers, ('ID', 'Name'), [[0, f"ALL {self.uppers}"]] + display_list)
        selected_item_resps: list[None | dict] = select([None] + user_item_resps, first_ID=0)
        
        if selected_item_resps[0] == None:
            # option 0 == get all choices
            selected_item_resps = user_item_resps[1:]
        return selected_item_resps
    
    def execute(self):
        self.reset()
        user_item_resps = self.fetch_user_items()
        if self.interactive:
            user_item_resps = self.display_select_user_items(user_item_resps)
        if self.inner_stripper and self._contains in {Track, Album}:
            for resp in user_item_resps:
                resp[self.inner_stripper][ADDED_AT] = resp[ADDED_AT]
            user_item_resps = [resp[self.inner_stripper] for resp in user_item_resps]
        self.parse_query_metadata([user_item_resps], [self._contains])
        self.fetch_extra_metadata()
        self.download()


class LikedSong(UserItem):
    _contains = Track
    interactive = False
    inner_stripper = TRACK
    url = USER_SAVED_TRACKS_URL
   
    def create_m3u8_playlists(self):
        archive_mode = Zotify.CONFIG.get_liked_songs_archive_m3u8()
        liked_tracks: list[Track] = self.requested_objs[0]
        filepaths = [t.real_filepaths.get(ParentStack([self, t])) for t in liked_tracks]
        
        m3u8_dir = self._path_root
        if archive_mode: # only work for non-dynamic paths
            for part in PurePath(Zotify.CONFIG.get_output(self.clsn)).parts:
                if "{" in part or "}" in part: break
                m3u8_dir = m3u8_dir / part
        if m3u8_dir == self._path_root:
            m3u8_dir = self.get_m3u8_dir(filepaths, self._path_root, force_common_dir=archive_mode)
        if not m3u8_dir:
            m3u8_dir = self._path_root
        m3u8_path = m3u8_dir / f"{self.name}.m3u8"
        
        def find_sync_point(m3u8_entry_path: str) -> int | None:
            for i, filepath in enumerate(filepaths):
                Printer.logger(f"{filepath} == {m3u8_entry_path}")
                if str(filepath) == m3u8_entry_path:
                    return i
                elif str(filepath) in m3u8_entry_path:
                    Printer.hashtaged(PrintChannel.WARNING, "TRACK FILEPATH WITHIN LIKED SONG M3U8 ENTRY\n" +
                                                            "M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n" +
                                                            "POSSIBLY FROM NON-UPDATED SONG ARCHIVE FILE\n" +
                                                            "(CONSIDER RUNNING '--update-archive')")
                    return i
                elif m3u8_entry_path in str(filepath):
                    Printer.hashtaged(PrintChannel.WARNING, "LIKED SONG M3U8 ENTRY WITHIN TRACK FILEPATH\n" +
                                                            "M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n" +
                                                            "POSSIBLY FROM M3U8 USING RELATIVE PATHS\n" +
                                                            "(CONSIDER USING FULL PATHS FOR LIKED SONGS M3U8)")
                    return i
            return None
        
        def handle_archive_mode() -> list[str]:
            if not archive_mode or not Path(m3u8_path).exists():
                return []
            
            raw_liked_archive = fetch_m3u8_songs(m3u8_path)
            if not raw_liked_archive:
                Printer.hashtaged(PrintChannel.WARNING, "FAILED Liked Songs ARCHIVE M3U8 UPDATE\n" +
                                                        "FAILED TO READ EXISTING M3U8\n" +
                                                        "FALLING BACK TO STANDARD M3U8 CREATION")
                return []
            
            for i, liked_archive_path in enumerate(raw_liked_archive[1::3]):
                sync_point = find_sync_point(liked_archive_path[:-1])
                if sync_point is not None:
                    self.requested_objs[0] = liked_tracks[:sync_point] # doesn't include matching Track obj
                    append = raw_liked_archive[3*i:] # includes matching track m3u8 entry
                    return append
                if i == 0:
                    Printer.hashtaged(PrintChannel.WARNING, "FIRST TRACK IN EXISTING M3U8 NOT FOUND IN CURRENT LIKED SONGS\n" +
                                                            "PERFORMING DEEP SEARCH FOR SYNC POINT")
            
            Printer.hashtaged(PrintChannel.WARNING, "FAILED Liked Songs ARCHIVE M3U8 UPDATE\n" +
                                                    "FAILED TO FIND SYNC POINT\n" +
                                                    "FALLING BACK TO STANDARD M3U8 CREATION")
            return []
        
        super().create_m3u8_playlists(force_path=m3u8_path, append=handle_archive_mode())


class SavedAlbum(UserItem):
    _contains = Album
    inner_stripper = ALBUM
    url = USER_SAVED_ALBUMS_URL


class UserPlaylist(UserItem):
    _contains = Playlist
    url = USER_PLAYLISTS_URL


class FollowedArtist(UserItem):
    _contains = Artist
    outer_stripper = ARTISTS
    url = USER_FOLLOWED_ARTISTS_URL
