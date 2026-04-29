from __future__ import annotations
import music_tag
import requests
from uuid import uuid4

from zotify.config import Zotify, Streamer
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
    
    @classmethod
    def get_if_exists(cls, node_comparable) -> HierarchicalNode | None:
        return cls.ALL_NODES.get(node_comparable)
    
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
    _to_str_attrs: list[str] = [URI, NAME]
    _to_db_attrs: list[str] = []
    _fetch_args = ""
    _url = ""
    
    def __init__(self, uri: str):
        self.uri = uri 
        # URIs are immutable, tied to hash and equality, and must exist before HierarchicalNode init
        # uri   == {type} : {id}
        # user  == user   : {user}:{type}:{id}
        # local == local  : {artist}:{album_title}:{track_title}:{duration_sec}
        
        super().__init__()
        self.id = self.uri.split(":", 1)[-1] # mutable, may be changed by automatic relinking in parse_metadata
        self.is_local = self.id.count(":") > 0
        
        self._downloaded = False
        self._hasMetadata = False
        
        self.name = ""
    
    def __eq__(self, other) -> bool:
        if isinstance(other, Content): return self.uri == other.uri
        elif isinstance(other, str): return self.uri == other
        return False
    
    def __hash__(self):
        return hash(self.uri)
    
    def __str__(self):
        default = fix_filename(f"({self.type_attr}){self.id}")
        vals = []
        for attr in self._to_str_attrs:
            val = getattr(self, attr, None)
            if isinstance(val, list):       val = val[0] if isinstance(val[0], Content) else ", ".join(str(v) for v in val)
            if isinstance(val, Content):    val = getattr(val, NAME, None)
            if val:                         vals.append(str(val))
        return fix_filename(" - ".join(vals)) if vals else default
    
    def full_metadata(self) -> bool:
        return bool(self.name)
    
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
    
    @classmethod
    def rel_path(cls, p: PurePath | ParentStack) -> PurePath:
        if isinstance(p, ParentStack):
            if not isinstance(p[-1], DLContent): # output_path requires instance
                raise ValueError("ParentStack must end with DLContent to get relative path")
            dlc: DLContent = p[-1]
            p = check_path_dupes(dlc.output_path(p))
        try:
            return p.relative_to(cls._path_root)
        except ValueError: # not relative, return absolute
            return p
    
    @classmethod
    def fetch_metadata(cls, uri: str, args: list[str] = []) -> dict[str]:
        resp = {}
        if Zotify.CONFIG.permit_legacy_api() or (Zotify.CONFIG.permit_client_api() and not cls is Playlist):
            argstr = arg_comb(cls._fetch_args, *args)
            resp = Zotify.invoke_url(f'{cls._url}/{uri.split(":")[-1]}?{MARKET_APPEND}{argstr}')
        else:
            resp = Zotify.invoke_libre_md(cls, uri)
            if cls is Track and resp.get(DURATION):
                resp[DURATION_MS] = resp.pop(DURATION)
                if resp[ALBUM]:
                    resp[ALBUM][ALBUM_TYPE] = str.lower(resp[ALBUM].pop(TYPE))
            elif cls is Album and resp.get(TYPE):
                resp[ALBUM_TYPE] = str.lower(resp.pop(TYPE))
            elif cls is Playlist and resp.get(ATTRIBUTES):
                resp.update(resp.pop(ATTRIBUTES))
            resp.update({URI: ":" + uri, TYPE: cls.type_attr})
        if resp: return resp
        else:    raise ValueError("No Metadata Fetched")
    
    @staticmethod
    def fetch_uris_metadata(uris: list[str], ContClass: type[Content],
                            loader_text: str = None, hide_loader: bool = False) -> list[dict]:
        if not uris: return []
        elif not loader_text: loader_text = ContClass.type_attr
        
        if Zotify.CONFIG.permit_legacy_api() and not ContClass is Playlist:
            with Loader(f"Fetching bulk {loader_text} information...", disabled=hide_loader):
                fetch_url = f"{ContClass._url}?{MARKET_APPEND}&{BULK_APPEND}"
                ids = [uri.split(":")[-1] for uri in uris]
                resps = Zotify.invoke_url_bulk(fetch_url, ids, ContClass.lowers, ITEM_BULK_FETCH[ContClass])
            if resps: return resps
            Printer.hashtaged(PrintChannel.WARNING, 'API BULK ENDPOINTS NOT ACCESSIBLE FOR THIS CLIENT_ID\n' +
                                                    'THIS WILL ALSO INHIBIT PLAYLIST ITEM FETCHING\n' +
                                                    'RECOMMENDED TO SET CONFIG "API_CLIENT_LEGACY = False"')
            Zotify.LEGACY_API_ENDOINTS = False
        
        suffix = "..." if Zotify.CONFIG.permit_client_api() else " (unsafe)..."
        with Loader(f"Fetching {loader_text} information{suffix}", disabled=hide_loader):
            return [ContClass.fetch_metadata(uri) for uri in uris]
    
    def make_or_link_relative(self, relative_uri: str, RelativeClass: type[Content], make_parent: bool = False) -> Content | Container:
        relative_to_be = self.get_if_exists(relative_uri)
        if relative_to_be is None:
            relative_to_be: Content | Container = RelativeClass(relative_uri)
        
        self.be_supervised(relative_to_be) if make_parent else self.adopt(relative_to_be)
        return relative_to_be
    
    def parse_metadata(self, relative: Content | None, resp: dict):
        from zotify.metadata import MetadataIO
        for k, v in MetadataIO().from_resp(self, relative, resp):
            if v is None: continue
            elif k == ID and self.id != v: # handle automatic relinking
                Printer.debug(f"Updated {self.clsn} {self.name} ({self.uri}) ID to {self.id}")
                setattr(self, ID, v)
            elif isinstance(self, Container) and k in self.__dict__ and getattr(self, k) == getattr(self, "_main_items"):
                self._main_items.extend(v)
            elif relative and k in {ADDED_AT, ADDED_BY, ALBUM_GROUP}:
                relational_attr: dict[Container, str | bool | User] = getattr(self, k)
                relational_attr.update({relative: v})
            elif not self._hasMetadata or getattr(self, k, None) is None:
                setattr(self, k, v)
        self._hasMetadata = self.full_metadata()
    
    def parse_relatives(self, resps: list[dict[str, str] | None], RelativeClasses: type[Content] | tuple[type[Content], ...],
                        make_parent: bool = False) -> list[Content | Container | None]:
        RelativeClasses = RelativeClasses if isinstance(RelativeClasses, tuple) else (RelativeClasses,)
        type_selector = tuple(cls.type_attr for cls in RelativeClasses)
        
        new_relatives: list[Content | Container] = []
        for i, resp in enumerate(resps):
            if not resp or not resp.get(TYPE) or not resp.get(URI):
                reason = "EXPECTED RESPONSE FOR" if not resp else (f"{TYPE} OF" if not resp.get(TYPE) else f"{URI} OF")
                relation = "PARENT" if make_parent else f"CHILD #{i+1 if not isinstance(self, Container) else self.ccount+i+1}"
                with Printer.pause_loader():
                    Printer.hashtaged(PrintChannel.WARNING, f'MISSING {reason} RELATED METADATA OBJECT\n' +
                                                            f'PARSING {relation} OF {self.clsn} ({self.id})\n' +
                                                            f'EXPECTED RELATIVE TYPES: {[c.clsn for c in RelativeClasses]}')
                    if resp: Printer.json_dump(PrintChannel.WARNING, resp)
                new_relatives.append(None)
                continue
            elif len(RelativeClasses) > 1 and resp[TYPE] not in type_selector:
                with Printer.pause_loader():
                    Printer.hashtaged(PrintChannel.WARNING, f'UNMAPPED CONTENT TYPE {resp[TYPE]}\n' +
                                                            f'EXPECTED RELATIVE TYPES: {[c.clsn for c in RelativeClasses]}')
                    if resp: Printer.json_dump(PrintChannel.WARNING, resp)
                new_relatives.append(None)
                continue
            elif len(RelativeClasses) > 1:  
                RelativeClass: type[Content | Container] = RelativeClasses[type_selector.index(resp[TYPE])]
            else:
                RelativeClass: type[Content | Container] = RelativeClasses[0]
            new_relative = self.make_or_link_relative(resp[URI].split(":", 1)[-1], RelativeClass, make_parent)
            new_relative.parse_metadata(self, resp)
            new_relatives.append(new_relative)
        
        return new_relatives
    
    def parse_uris_metadata(self, item_resps: list[dict], ContClass: type[Content],
                            loader_text: str = None, hide_loader: bool = False) -> list[Content | Container]:
        if not item_resps: return []
        elif not loader_text: loader_text = ContClass.type_attr
        with Loader(f"Parsing {loader_text} information...", disabled=hide_loader):
            objs: list[Content | Container] = self.parse_relatives(item_resps, ContClass)
            
            if not objs or not any(objs) or not isinstance(objs[0], Container):
                return objs
            
            # missing children, only findale with Developer Client
            if Zotify.CONFIG.permit_client_api():
                for obj in objs:
                    if obj._needs_expansion: obj.grab_more_children(hide_loader=True)
            
            # children missing metadata
            recurs_objs = [o for o in objs if isinstance(o, Container) and o._needs_recursion]
            if recurs_objs:
                recurs_children: list[Container] = []
                for recurs_obj in recurs_objs:
                    recurs_children.extend(recurs_obj._main_items)
                contains: tuple[type[Content], ...] = recurs_objs[0]._contains
                for recurse_type in contains if isinstance(contains, tuple) else (contains,):
                    recurse_uris = [item.uri for item in recurs_children if isinstance(item, recurse_type)]
                    recurs_item_resps = self.fetch_uris_metadata(recurse_uris, recurse_type, hide_loader=True)
                    _ = self.parse_uris_metadata(recurs_item_resps, recurse_type, hide_loader=True)
            return objs
    
    def check_skippable(self, parent_stack: ParentStack) -> bool:
        return False
    
    def mark_downloaded(self, ps: ParentStack | None = None, path: PurePath | None = None):
        if isinstance(self, Container) and not isinstance(self, Query):
            self._downloaded = all(c._downloaded for c in self._main_items)
        elif not isinstance(self, DLContent):
            self._downloaded = True
        elif path:
            self._downloaded = True
            parent_stack = ps if Zotify.CONFIG.get_optimized_dl() else ParentStack(ps.copy())
            self._real_filepaths[parent_stack] = path
            from zotify.metadata import SongArchive
            if not self._in_global_archive:
                SongArchive().add_obj(self, path)
            if isinstance(self, Track) and not self.id in SongArchive(path.parent).ids():
                SongArchive(path.parent).add_obj(self, path)


class DLContent(Content):
    _codec = ""
    _ext   = ""
    
    def __init__(self, uri: str):
        from zotify.metadata import SongArchive
        super().__init__(uri)
        self._dl_status = ""
        self._in_global_archive = self.id in SongArchive().ids()
        self._real_filepaths: dict[ParentStack, PurePath] = {}
        self._clone_to: set[ParentStack] = set()
        
        self.duration_ms    : int                   = None
        self.gid            : str                   = None
        self.is_playable    : bool                  = None
        
        self.file_ids       : list[dict[str, str]]  = None
    
    def set_dl_status(self, str_status) -> Loader:
        self._dl_status = str_status
        if Zotify.CONFIG.get_standard_interface():
            Interface.refresh()
        return Loader(str_status + "...")
    
    # placeholder func, overwrite in each child class
    def fill_output_template(self, parent_stack: ParentStack, output_template: str = ""):
        pass
    
    def output_path(self, parent_stack: ParentStack, output_template: str = "") -> PurePath:
        try: # metadata path using child class custom metadata
            return self.fill_output_template(parent_stack, output_template)
        except Exception as e:
            Printer.hashtaged(PrintChannel.WARNING, f'FAILED TO FILL {self.clsn} OUTPUT TEMPLATE\n' +
                                                    f'ERROR: {str(e)}\n' + 
                                                    f'FALLING BACK TO DEFAULT OUTPUT PATH')
            return self._path_root / f"{self.id}.{self._ext}"
    
    def check_skippable(self, parent_stack: ParentStack) -> bool:
        from zotify.metadata import SongArchive
        def handle_archive(dir_path: PurePath | None):
            archived_path = SongArchive(dir_path).id_path(self.id)
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} DOWNLOADED PREVIOUSLY)\n'
                                                     f'FILE: "{self.rel_path(archived_path)}"')
            self.mark_downloaded(parent_stack, archived_path)
        
        path = self.output_path(parent_stack)
        path_exists = Path(path).is_file() and Path(path).stat().st_size
        if isinstance(self, Episode) and path.suffix == ".copy":
            # file suffix agnostic check
            for file_match in Path(path.parent).glob(path.stem + ".*", case_sensitive=True):
                if file_match.stat().st_size:
                    path_exists = True
                    break
        in_dir_archive = self.id in SongArchive(path.parent).ids()
        if not Zotify.CONFIG.get_optimized_dl():
            Printer.debug(f'Duplicate Check @ "{path}"\n' +
                          f'File Already Exists: {path_exists}\n' +
                          f'id in Local Archive: {in_dir_archive}\n' +
                          f'id in Global Archive: {self._in_global_archive}')
        
        if path_exists and Zotify.CONFIG.get_skip_existing() and Zotify.CONFIG.get_no_dir_archives():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.rel_path(path)}" (FILE ALREADY EXISTS)')
            self.mark_downloaded(parent_stack, path)
            return True
        elif in_dir_archive and Zotify.CONFIG.get_skip_existing() and not Zotify.CONFIG.get_no_dir_archives():
            handle_archive(path.parent)
            return True
        elif self._in_global_archive and Zotify.CONFIG.get_skip_previously_downloaded():
            handle_archive(None)
            return True
        
        elif self.regex_check(skip_debug_print=Zotify.CONFIG.get_optimized_dl()):
            return True
        elif self.is_local:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} IS A LOCAL FILE)')
            return True
        elif not self.is_playable:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} IS UNAVAILABLE)')
            return True
        
        return False
    
    def fetch_content_stream(self, stream: Streamer, temppath: PurePath, parent_stack: ParentStack) -> str:
        disable = Zotify.CONFIG.get_standard_interface() or not Zotify.CONFIG.get_show_download_pbar()
        pbar = Printer.pbar(desc=str(self), total=stream.size, unit='B', unit_scale=True,
                            unit_divisor=1024, disable=disable, pbar_stack=parent_stack.PBARS)
        Path(temppath.parent).mkdir(parents=True, exist_ok=True)
        try:
            with open(temppath, 'wb') as file:
                no_responses = 0
                time_start = time.time()
                t_per_byte = (self.duration_ms / 1000. / stream.size * Zotify.CONFIG.get_dl_rate_limter()) if self.duration_ms else 0
                while no_responses < 5:
                    bytes_r = file.write(stream.stream().read(Zotify.CONFIG.get_chunk_size()))
                    if bytes_r:
                        pbar.update(bytes_r)
                        time.sleep(bytes_r * t_per_byte)
                    else:
                        no_responses += 1
                        time.sleep(0.05)
                # if Zotify.CONFIG.get_download_real_time():
                    #     elapsed_real = time.time() - time_start
                    #     elapsed_want = (pbar.n / stream.size) * (self.duration_ms/1000)
                    # if elapsed_want > elapsed_real:
                    #     time.sleep(elapsed_want - elapsed_real)
        finally:
            pbar.close(); pbar.clear()
        
        return fmt_duration(time.time() - time_start)
    
    def get_audio_duration(self, path: PurePath) -> float:
        stdout = run_ffm(path, ["-show_entries", "format=duration"])
        duration = re.search(r'[\D]=([\d\.]*)', stdout).groups()[0]
        return float(duration) # in seconds
    
    def get_audio_codec(self, path: PurePath) -> str:
        stdout = run_ffm(path, ["-show_entries", "stream=codec_name"])
        return stdout.split("=")[1].split("\r")[0].split("\n")[0]
    
    def convert_audio_format(self, temppath: PurePath, path: PurePath) -> str | None:
        output_params = ['-c:a', self._codec]
        if self._codec != 'copy':
            bitrate = Zotify.CONFIG.get_transcode_bitrate()
            if bitrate in {"auto", ""}:
                bitrate = Zotify.DOWNLOAD_BITRATE
            if bitrate:
                output_params += ['-b:a', bitrate]
        Printer.logger(f'Temp Path: "{temppath}"\n' + 
                       f'Output Path: "{path}"\n' +
                       f'Desired Codec: {self._codec.upper()}\n' +
                       f'Expected Log Level: {Zotify.CONFIG.get_ffmpeg_log_level().upper()}', PrintChannel.DEBUG)
        
        time_ffmpeg_start = time.time()
        try:
            run_ffm(temppath, None, path, output_params + Zotify.CONFIG.get_custom_ffmpeg_args())
            return fmt_duration(time.time() - time_ffmpeg_start)
        except ffmpy.FFExecutableNotFoundError:
            Printer.hashtaged(PrintChannel.WARNING, 'FFMPEG NOT FOUND\n' +
                                                   f'SKIPPING CONVERSION TO {self._codec.upper()}')
            return
        except Exception as e:
            if Zotify.CONFIG.get_custom_ffmpeg_args():
                Printer.hashtaged(PrintChannel.WARNING, str(e) + '\n' + 'CUSTOM FFMPEG ARGUMENTS FAILED')
        
        try:
            run_ffm(temppath, None, path, output_params)
            return fmt_duration(time.time() - time_ffmpeg_start)
        except Exception as e:
            Printer.hashtaged(PrintChannel.WARNING, str(e) + '\n' + f'SKIPPING CONVERSION TO {self._codec.upper()}')
            return
    
    # placeholder func, overwrite in each child class
    def download(self, parent_stack: ParentStack):
        pass
    
    def clone_file(self, parent_stack: ParentStack) -> bool:
        """ Attempt to clone and return if clone succeeded """
        if parent_stack.check_skippable():
            return False
        clone_path = check_path_dupes(self.output_path(parent_stack))
        if not self._real_filepaths:
            Printer.hashtaged(PrintChannel.WARNING, f'ATTEMPT TO CLONE {self.clsn.upper()} "{self}" FAILED\n' + 
                                                     'FILE NOT YET DOWNLOADED, THIS SHOULD NOT HAPPEN')
        for filepath in self._real_filepaths.values():
            if not Path(filepath).exists(): continue
            pathlike_move_safe(filepath, clone_path, copy=True)
            self.mark_downloaded(parent_stack, clone_path)
            return True
        Printer.hashtaged(PrintChannel.WARNING, f'ATTEMPT TO CLONE {self.clsn.upper()} "{self}" FAILED\n' + 
                                                f'FALLING BACK TO REDOWNLOAD\n' +
                                                f'EXPECTED SOURCE FILES THAT DO NOT EXIST:')
        Printer.json_dump(PrintChannel.WARNING, {str(k): str(v) for k, v in self._real_filepaths.items()})
        return False
    
    def clone_to_all(self) -> bool:
        """ Attempt to clone all and return if all clones succeeded """
        for ps in self._clone_to:
            if ps in self._real_filepaths: continue
            if not self.clone_file(ps):
                return False
        return True


class Track(DLContent):
    _regex_flag = Zotify.CONFIG.get_regex_track()
    _to_str_attrs = [ARTISTS, NAME]
    _to_db_attrs = [TRACK_NUMBER, ARTISTS, ALBUM]
    _codec = CODEC_MAP_TRACK.get(Zotify.CONFIG.get_download_format().lower(), "copy")
    _ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "ogg")
    _url = TRACK_URL
    
    def __init__(self, uri: str) -> None:
        super().__init__(uri)
        self.disc_number    : int                   = None
        self.ean            : str                   = None # European Article Number
        self.isrc           : str                   = None # International Standard Recording Code
        self.track_number   : str                   = None
        self.upc            : str                   = None # Universal Product Code (Type-A)
        self.album          : Album                 = None
        self.artists        : list[Artist]          = None
        
        # only fetched if config set
        self.genres         : list[str]             = None
        self.lyrics         : list[str]             = None
        
        # only set by Playlist API or UserItem API
        self.added_at       : dict[Container, str]  = {}
        # only set by Playlist API
        self.added_by       : dict[Playlist, User]  = {}
    
    def fill_output_template(self, parent_stack: ParentStack, output_template: str = "") -> PurePath:
        parent: Container = parent_stack[-2]
        if not output_template:
            try:
                output_template = Zotify.CONFIG.get_output(parent.clsn)
            except:
                Printer.debug(f"Unexpected Track Parent: {parent.clsn}")
                output_template = Zotify.CONFIG.get_output('Query')
        
        repl_dict: dict[str, str] = {}
        def update_repl(md_val, *replstrs: str):
            # Printer.debug(replstrs[0])
            repl_dict.update(zip(replstrs, [md_val]*len(replstrs)))
        
        update_repl(self.id,            "{id}", "{track_id}", "{song_id}")
        update_repl(self.name,          "{name}", "{song_name}", "{track_name}", "{song_title}", "{track_title}")
        update_repl(self.track_number,  "{track_number}", "{song_number}", "{track_num}", "{song_num}", "{album_number}", "{album_num}")
        update_repl(self.disc_number,   "{disc_number}", "{disc_num}")
        update_repl(self.ean,           "{ean}")
        update_repl(self.isrc,          "{isrc}")
        update_repl(self.upc,           "{upc}")
        
        if self.artists:
            artists_names = conv_artist_format(self.artists, FORCE_NO_LIST=True)
            update_repl(self.artists[0].name,   "{artist}", "{track_artist}", "{song_artist}", "{main_artist}", "{primary_artist}")
            update_repl(artists_names,          "{artists}", "{track_artists}", "{song_artists}")
        
        if self.album:
            update_repl(self.album.id,              "{album_id}")
            update_repl(self.album.name,            "{album}", "{album_name}")
            update_repl(self.album.release_date,    "{date}", "{release_date}")
            update_repl(self.album.year,            "{year}", "{release_year}")
            if self.album.artists:
                album_artists_names = conv_artist_format(self.album.artists, FORCE_NO_LIST=True)
                update_repl(self.album.artists[0].name, "{album_artist}")
                update_repl(album_artists_names,        "{album_artists}")
        
        if Zotify.CONFIG.get_disc_track_totals():
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
        if super().check_skippable(parent_stack): return True
        
        if self.album and self.album.check_skippable(parent_stack):
            return True
        
        return False
    
    def fetch_lyrics(self, parent_stack: ParentStack) -> None:
        if self.lyrics:
            return
        elif not Zotify.CONFIG.get_lyrics_to_file() and not Zotify.CONFIG.get_lyrics_to_metadata():
            return
        
        try:
            with Loader("Fetching lyrics..."):
                # expect failure here, lyrics are not guaranteed to be available
                lyrics_dict = Zotify.invoke_url(LYRICS_URL + self.id, expectFail=True, force_login5=True)
                if not lyrics_dict:
                    raise ValueError('FAILED TO FETCH')
                try:
                    formatted_lyrics = lyrics_dict[LYRICS][LINES]
                except KeyError:
                    raise ValueError('LYRICS NOT AVAILABLE') from None
                
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
                lyricdir = self.output_path(parent_stack).parent
            Path(lyricdir).mkdir(parents=True, exist_ok=True)
            
            lrc_filename = self.output_path(parent_stack, Zotify.CONFIG.get_lyrics_filename()).stem
            
            with open(lyricdir / f"{lrc_filename}.lrc", 'w', encoding='utf-8') as file:
                if Zotify.CONFIG.get_lyrics_header():
                    lrc_header = [f"[ti: {self.name}]\n",
                                  f"[ar: {conv_artist_format(self.artists, FORCE_NO_LIST=True)}]\n",
                                  f"[al: {self.album.name}]\n",
                                  f"[length: {self.duration_ms // 60000}:{(self.duration_ms % 60000) // 1000}]\n",
                                  f"[by: Zotify v{Zotify.VERSION}]\n",
                                  "\n"]
                    file.writelines(lrc_header)
                file.writelines(self.lyrics)
    
    def write_audio_tags(self, filepath: PurePath, parent_stack: ParentStack | None = None) -> None:
        file_tags: music_tag.AudioFile = music_tag.load_file(filepath)
        img = None # expect jpeg
        
        def set_tag_safe(FILETAG, tag_value):
            if not tag_value: return
            try: file_tags[FILETAG] = tag_value
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
            if self._ext == "mp3":   custom_mp3_tag(tag, val)
            elif self._ext == "m4a": custom_m4a_tag(tag, val)
            else:                    custom_ogg_tag(tag, val)
        
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
            img = requests.get(self.album.image_url).content if self.album.image_url else None
            set_tag_safe(   ARTWORK,        img)
        
        # Unreliable Tags
        custom_tag(         TRACKID,        self.id)
        custom_tag(         URI,            self.uri)
        custom_tag(         EAN,            self.ean)
        custom_tag(         ISRC,           self.isrc)
        custom_tag(         UPC,            self.upc)
        
        if self.album and Zotify.CONFIG.get_disc_track_totals():
            set_tag_safe(TOTALTRACKS,       self.album.total_tracks)
            set_tag_safe(TOTALDISCS,        self.album.total_discs)
        
        if self.album and self.album.compilation:
            set_tag_safe(COMPILATION,       self.album.compilation)
        
        if self.lyrics and Zotify.CONFIG.get_lyrics_to_metadata():
            set_tag_safe(LYRICS,            "".join(self.lyrics))
        
        if self._ext == "mp3" and not Zotify.CONFIG.get_disc_track_totals() and self.disc_number and self.track_number:
            # music_tag python library writes DISCNUMBER and TRACKNUMBER as X/Y instead of X for mp3
            # this method bypasses all internal formatting, probably not resilient against arbitrary inputs
            file_tags.set_raw("mp3", "TPOS", str(self.disc_number))
            file_tags.set_raw("mp3", "TRCK", str(self.track_number))
        
        file_tags.save()
        
        # save track image art to file
        if not Zotify.CONFIG.get_album_art_jpg_file() or img is None or not parent_stack:
            return
        jpg_album_cover_path = filepath.with_name('cover.jpg')
        jpg_single_path = filepath.with_suffix('.jpg')
        Printer.logger(f"Album Art Detected: {Path(jpg_album_cover_path).exists()}\n" +
                       f"Single Art Detected: {Path(jpg_single_path).exists()}", PrintChannel.DEBUG)
        if Path(jpg_album_cover_path).exists() or Path(jpg_single_path).exists():
            return
        jpg_path = jpg_album_cover_path if len(parent_stack) > 1 and isinstance(parent_stack[-2], Album) else jpg_single_path
        with open(jpg_path, 'wb') as f: f.write(img)
    
    def download(self, parent_stack: ParentStack) -> None:
        if not Zotify.CONFIG.get_optimized_dl():
            if Zotify.CONFIG.get_download_parent_album():
                with Zotify.CONFIG.temporary_config(DOWNLOAD_PARENT_ALBUM, False):
                    self.album.download(ParentStack([parent_stack[0], self.album]))
                return
            elif self._downloaded and self.clone_file(parent_stack):
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} ALREADY DOWNLOADED THIS SESSION)\n' + 
                                                         f'FILE COPIED TO NEW DESTINATION "{self.rel_path(parent_stack)}"')
                return
        elif Zotify.CONFIG.get_optimized_dl() and self._downloaded:
            if self.clone_to_all(): return
        
        if Zotify.CONFIG.get_always_check_lyrics():
            self.fetch_lyrics(parent_stack)
        
        if parent_stack.check_skippable():
            return
        
        Interface.bind(parent_stack)
        with self.set_dl_status("Preparing Download"):
            path = check_path_dupes(self.output_path(parent_stack))
            if path != self.output_path(parent_stack): # path exists but id isn't archived OR skipping disabled
                Printer.debug('Path Duplicate Not Being Skipped:\n' +
                              'ID not Archived' if Zotify.CONFIG.get_skip_existing() else 'Skipping Disabled')
            temppath = path.with_suffix(".tmp")
            if Zotify.CONFIG.get_temp_download_dir():
                temppath = Zotify.CONFIG.get_temp_download_dir() / f'zotify_{str(uuid4())}_{self.id}.tmp'
        
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
        
        try: self.write_audio_tags(path, parent_stack)
        except NotImplementedError as e:
            if not "Mutagen type" in e.args[0]: raise
            err_codec = e.args[0].removeprefix("Mutagen type ").removesuffix(" not implemented")
            Printer.hashtaged(PrintChannel.ERROR,  'FAILED TO WRITE METADATA\n' +
                                                  f'FILE "{self.rel_path(path)}" OF MEDIA TYPE {err_codec}\n' +
                                                  f'INSTEAD OF EXPECTED MEDIA TYPE {self._codec}')
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO WRITE METADATA\n')
            Printer.traceback(e)
        
        Interface.dl_complete(self, path, time_elapsed_dl, time_elapsed_ffmpeg)
        
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
    _to_db_attrs = [SHOW]
    _codec = CODEC_MAP_EPISODE.get(Zotify.CONFIG.get_download_format().lower(), "copy")
    _ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "copy")
    _url = EPISODE_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.description            : str       = None
        self.explicit               : bool      = None
        self.external_url           : str       = None
        self.is_externally_hosted   : bool      = None
        self.partner_url            : str       = None
        self.publish_time           : str       = None
        self.release_date           : str       = None
        self.show                   : Show      = None
        
        # only set by Playlist API
        self.added_at       : dict[Playlist, str]   = {}
        self.added_by       : dict[Playlist, User]  = {}
    
    def fill_output_template(self, parent_stack: list[Container], output_template: str = "") -> PurePath:
        return self._path_root / fix_filename(self.show.name) / f"{self}.{self._ext}"
    
    def fetch_partner_url(self) -> str | None:
        resp = Zotify.invoke_url(PARTNER_URL + self.id + '"}&extensions=' + PERSISTED_QUERY, force_login5=True)
        if not resp.get(DATA):
            raise ValueError( 'NO DATA IN PARTNER RESPONSE\n' +
                             f'Episode_ID: {self.id}')
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
        with Printer.pbar_stream(r.raw, desc=desc, total=file_size) as f_stream:
            pathlike_move_safe(f_stream, path)
        
        time_dl_end = time.time()
        return fmt_duration(time_dl_end - time_start)
    
    def download(self, parent_stack: ParentStack):
        if not Zotify.CONFIG.get_optimized_dl() and self._downloaded and self.clone_file(parent_stack):
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" ({self.clsn.upper()} ALREADY DOWNLOADED THIS SESSION)\n' +
                                                     f'FILE COPIED TO NEW DESTINATION "{self.rel_path(parent_stack)}"')
            return
        elif Zotify.CONFIG.get_optimized_dl() and self._downloaded:
            if self.clone_to_all(): return
        elif parent_stack.check_skippable():
            return
        
        Interface.bind(parent_stack)
        with self.set_dl_status("Preparing Download"):
            path = check_path_dupes(self.output_path(parent_stack))
            if path != self.output_path(parent_stack): # path exists but id isn't archived OR skipping disabled
                Printer.debug('Path Duplicate Not Being Skipped:\n' +
                              'ID not Archived' if Zotify.CONFIG.get_skip_existing() else 'Skipping Disabled')
            temppath = path.with_suffix(".tmp")
            if Zotify.CONFIG.get_temp_download_dir():
                temppath = Zotify.CONFIG.get_temp_download_dir() / f'zotify_{str(uuid4())}_{self.id}.tmp'
        
        self.set_dl_status("Downloading Stream")
        if not self.fetch_partner_url():
            stream = Zotify.get_content_stream(self)
            if stream is None:
                Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING EPISODE - FAILED TO GET CONTENT STREAM\n' +
                                                     f'Episode_ID: {self.id}')
                return
            time_elapsed_dl = self.fetch_content_stream(stream, temppath, parent_stack)
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
        
        Interface.dl_complete(self, path, time_elapsed_dl, time_elapsed_ffmpeg)
        
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
        self._main_items: list[DLContent | Container | None] = []
        self._needs_expansion = False
        self._needs_recursion = False
    
    @property
    def ccount(self):
        return len(self._main_items)
    
    def fetch_items(self, args: list[str] = [], hide_loader: bool = False) -> list[dict]:
        item_key = ITEMS if isinstance(self, Playlist) else self._contains.lowers
        with Loader(f'Fetching {self.type_attr} {item_key}...', disabled=hide_loader):
            argstr = arg_comb(self._fetch_args, *args)
            if self._nextable:
                resp = Zotify.invoke_url_nextable(f'{self._url}/{self.id}/{item_key.replace(" ", "-")}?{MARKET_APPEND}{argstr}',
                                                  params={LIMIT: self._fetch_q, OFFSET: self.ccount})
            else:
                resp = Zotify.invoke_url(f'{self._url}/{self.id}/{item_key.replace(" ", "-")}?{MARKET_APPEND}{argstr}')
                _, resp = resp.popitem()
            return resp
    
    def recurse_DLC(self) -> list[DLContent | None]:
        dlc = []
        for c in self._main_items:
            if isinstance(c, DLContent):    dlc.append(c)
            elif isinstance(c, Container):  dlc.extend(c.recurse_DLC())
            else:                           dlc.append(None)
        return dlc
    
    def grab_more_children(self, hide_loader: bool = False) -> list[dict]:
        item_resps = self.fetch_items(hide_loader=hide_loader)
        item_objs = self.parse_relatives(item_resps, self._contains)
        self._main_items.extend(item_objs)
        self._needs_expansion = False
    
    def pbar(self, items: list[DLContent | Container | None], ps: ParentStack) -> list[DLContent | Container]:
        real_items: list[DLContent | Container] = [c for c in items if c is not None]
        if not any(real_items): return []
        parent: DLContent | Container = ps[-1]
        unit = "Content" if isinstance(parent._contains, tuple) else parent._contains.__name__
        if isinstance(parent, Query) and not isinstance(parent, UserItem): # avoid overruling UserItem._contains
            unit = "Content" if Zotify.CONFIG.get_optimized_dl() else "URL"
        pbar: list[DLContent | Container] = Printer.pbar(real_items, parent.name, unit=unit, default_pos=7,
                                                         disable=not parent._show_pbar, pbar_stack=ps.PBARS)
        ps.PBARS.append(pbar)
        return pbar
    
    def download(self, parent_stack: ParentStack):      
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
    _url = PLAYLIST_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.collaborative      : bool                      = None
        self.description        : str                       = None
        self.deleted_by_owner   : bool                      = None
        self.length             : int                       = None
        self.image_url          : str                       = None
        self.public             : bool                      = None
        self.revision           : str                       = None
        self.snapshot_id        : str                       = None
        self.timestamp          : str                       = None
        
        self.owner              : User                      = None
        self.tracks_or_eps      : list[Track | Episode]     = self._main_items
    
    def unwrap(self, playlist_items: list[dict[str, dict]]) -> list[dict | None]:
        tracks_eps_empty: list[dict] = [item.get(ITEM) for item in playlist_items]
        for i, track_or_ep, item in zip(range(len(playlist_items)), tracks_eps_empty, playlist_items):
            if track_or_ep is None:
                # Printer.debug(f'Playlist Item {self.ccount+i+1} ({IS_LOCAL} == {item.get(IS_LOCAL)})\n' +
                #                 'Has Playlist Entry but no Metadata:', item)
                continue
            track_or_ep[ADDED_AT] = item.get(ADDED_AT)
            track_or_ep[ADDED_BY] = item.get(ADDED_BY)
            track_or_ep[IS_LOCAL] = item.get(IS_LOCAL)
        return tracks_eps_empty
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        return self.unwrap( super().fetch_items(hide_loader=hide_loader) )


class User(Container):
    _contains = Playlist
    _display_name_map = {}
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.display_name   : str   = None
        self.external_urls  : dict  = None
    
    @classmethod
    def fetch_display_name(cls, username: str) -> str:
        display_name = cls._display_name_map.get(username)
        if display_name: return display_name
        
        user_profile = Zotify.get_user_profile(username)
        cls._display_name_map[username] = user_profile.get(NAME, username)
        return cls._display_name_map[username]


class Album(Container):
    _regex_flag = Zotify.CONFIG.get_regex_album()
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _to_str_attrs = [ARTISTS, NAME]
    _to_db_attrs = [TOTAL_TRACKS, ARTISTS]
    _contains = Track
    _preloaded = 50
    _url = ALBUM_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.album_type     : str                   = None
        self.compilation    : bool                  = None
        self.duration_ms    : int                   = None
        self.ean            : str                   = None # European Article Number
        self.image_url      : str                   = None
        self.isrc           : str                   = None # International Standard Recording Code
        self.label          : str                   = None
        self.release_date   : str                   = None
        self.total_discs    : int                   = None
        self.total_tracks   : str                   = None
        self.upc            : str                   = None # Universal Product Code (Type-A)
        self.year           : str                   = None
        self.artists        : list[Artist]          = None
        self.tracks         : list[Track]           = self._main_items
        
        # only set by Artist Albums API
        self.album_group    : dict[Container, str]  = {}
        # only set by UserItem API
        self.added_at       : dict[Container, str]  = {}
    
    def full_metadata(self) -> bool:
        return bool(self.tracks)
    
    def grab_more_children(self, hide_loader: bool = False) -> list[dict]:
        super().grab_more_children(hide_loader=hide_loader)
        self.total_discs = str(self.tracks[-1].disc_number)
        self.duration_ms = sum((int(t.duration_ms) for t in self.tracks))
    
    def check_skippable(self, parent_stack: ParentStack) -> bool:
        discog_artist = next((p for p in parent_stack if isinstance(p, Artist)), None)
        album_group = self.album_group.get(discog_artist, getattr(discog_artist, APPEARS_ON, None))
        if album_group:
            if Zotify.CONFIG.get_skip_comp_albums() and album_group == COMPILATION:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ARTIST ONLY COMPILED INTO ALBUM)')
                return True
            elif Zotify.CONFIG.get_skip_appears_on_album() and (album_group == APPEARS_ON or self in discog_artist.appears_on):
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ARTIST ONLY APPEARS ON ALBUM)')
                return True
            elif Zotify.CONFIG.get_discog_by_album_artist() and self.artists[0].name != discog_artist.name:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ARTIST NOT ALBUM ARTIST)')
                return True
        
        if Zotify.CONFIG.get_skip_comp_albums() and self.compilation:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (COMPILATION ALBUM)')
            return True
        elif Zotify.CONFIG.get_skip_various_artists() and "".join(self.artists[0].name.lower().split()) == "variousartists":
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self}" (ALBUM OF VARIOUS ARTISTS)')
            return True
        
        return False


class Artist(Container):
    _show_pbar = Zotify.CONFIG.get_show_artist_pbar()
    _to_str_attrs = [NAME, FOLLOWERS, GENRES]
    _to_db_attrs = [GENRES]
    _toptrackmode: bool = False # Zotify.get_artist_fetch_top_tracks(), not implemented
    _contains = Album if not _toptrackmode else TopTrack
    _fetch_q = (20 if Zotify.CONFIG.permit_legacy_api() else 10) if not _toptrackmode else 100
    _nextable = not _toptrackmode
    _url = ARTIST_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self._needs_expansion = True
        self._needs_recursion = True
        
        self.biography      : str               = None
        self.end_year       : str               = None
        self.followers      : int               = None
        self.start_year     : str               = None
        self.albums         : list[Album]       = None
        self.all_albums     : list[Album]       = self._main_items if not self._toptrackmode else None
        self.appears_on     : list[Album]       = None
        self.genres         : list[str]         = None
        self.singles        : list[Album]       = None
        self.top_tracks     : list[TopTrack]    = self._main_items if self._toptrackmode else None
    
    def full_metadata(self) -> bool:
        return bool(self.genres)


class Show(Container):
    _path_root: PurePath = Zotify.CONFIG.get_root_podcast_path()
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _to_str_attrs = [PUBLISHER, NAME]
    _to_str_attrs = [TOTAL_EPISODES]
    _contains = Episode
    _preloaded = 50
    _url = SHOW_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.description            : str               = None
        self.explicit               : bool              = None
        self.is_externally_hosted   : bool              = None
        self.image_url              : str               = None
        self.publisher              : str               = None
        self.total_episodes         : str               = None
        self.episodes               : list[Episode]     = self._main_items


# start not implemented
class TopTrack(Track):
    pass


class Chapter(DLContent):
    _url = CHAPTER_URL


class Audiobook(Container):
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _contains = Chapter
    _preloaded = 50
    _url = AUDIOBOOK_URL
    
    def __init__(self, uri: str):
        super().__init__(uri)
        self.chapters       : list[Chapter]         = self._main_items
# end not implemented


# sets Query fetch order and bulk quantity by type
ITEM_BULK_FETCH: dict[type[DLContent] | type[Container], int] = {
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
    """ Will contain DLContent as last item in self if complete.
        Possible to include a NoneType if metadata fails to fetch,
        where None will always be the last item if present """
    PBARS                               = []
    skippable: dict[str, bool | None]   = {}
    
    def __hash__(self: ParentStack | list[Content | None]):
        # this means Container._main_items with no metadata are indistinguishable,
        # which is *probably* fine since they should all be skipped anyway
        return hash("&".join(c.uri if isinstance(c, Content) else "None" for c in self))
    
    def __eq__(self: ParentStack | list[Content | None], other: ParentStack | list[Content | None]):
        return len(self) == len(other) and all(a == b for a, b in zip(self, other))
    
    def __str__(self: ParentStack | list[Content | None]) -> str:
        return "[" + ' -> '.join([c.clsn if isinstance(c, Content) else "None" for c in self]) + "]"
    
    def check_skippable(self: ParentStack | list[Content | None]) -> bool:
        h = hash(self)
        if h in self.skippable: return self.skippable[h]
        skip = self[-1] is None or any(c.check_skippable(self) for c in self[::-1])
        self.skippable[h] = skip
        return skip
    
    def download(self: ParentStack | list[DLContent | Container | None], _: ParentStack):
        if self[-1] is None: 
            Printer.hashtaged(PrintChannel.WARNING, f'ATTEMPTING TO DOWNLOAD A STACK THAT FAILED TO FETCH METADATA')
            return
        self[-1].download(self)


class Query(Container):
    _root_node = True
    _show_pbar = Zotify.CONFIG.get_show_url_pbar()
    name = "Total Progress"
    
    def __init__(self, timestamp: str):
        HierarchicalNode.ALL_NODES = {}
        ParentStack.PBARS = []
        super().__init__(f"{self.type_attr}:{timestamp}" )
        del self.name
        
        self.parsed_request  : list[list[str]]                                        = []
        self.prefetched_map  : list[dict[int, Content]]                               = []
        self.requested_objs  : list[list[DLContent | Container | None]]               = []
        self._main_items     : list[DLContent | Container | None] | list[ParentStack] = []
    
    def request(self, requested_urls: str) -> Query:
        self.parsed_request = bulk_regex_urls(requested_urls)
        n_urls = len(set.union(*[set(l) for l in self.parsed_request]))
        [c for c in self.parsed_request if any(c)]
        Printer.debug(f'Requested URL String: {requested_urls}\n' + 
                      f'Parsed URI List ({n_urls}): {self.parsed_request}\n')
        return self
    
    def handle_zmd_prefetch(self):
        if not Zotify.CONFIG.get_import_zmd(): return
        
        from zotify.metadata import MetadataIO
        MetadataIO.from_zmd()
        
        def skip_fetch(uri: str) -> bool:
            zmd_cont: Content | None = self.get_if_exists(uri)
            return zmd_cont and zmd_cont._hasMetadata
        
        self.prefetched_map = [{i: self.get_if_exists(uri) for i, uri in enumerate(uris) if skip_fetch(uri)}
                              for uris in self.parsed_request]
        self.parsed_request = [[uri for uri in uris if uri not in prefet.values()]
                              for uris, prefet in zip(self.parsed_request, self.prefetched_map)]
    
    def fetch_query_metadata(self) -> list[list[dict]]:
        item_resps_by_type: list[list[dict]] = []
        for uris, cont_type in zip(self.parsed_request, ITEM_BULK_FETCH):
            item_resps_by_type.append(self.fetch_uris_metadata(uris, cont_type))
        return item_resps_by_type
    
    def parse_query_metadata(self, item_resps_by_type: list[list[dict]], item_types: list[type[Content]] = ITEM_BULK_FETCH) -> None:
        """ Writes list[list[Content]] to self.requested_objs """
        for item_resps, item_type in zip(item_resps_by_type, item_types):
            self.requested_objs.append(self.parse_uris_metadata(item_resps, item_type))
        return self.requested_objs
    
    def conditional_metadata(self):
        alltracks = {t for t in self.ALL_NODES if isinstance(t, Track) and not t.is_local}
        
        artists = set().union(*(set(track.artists) for track in alltracks))
        artist_uris: dict[str, Artist] = {a.uri: a for a in artists if not a.is_local and not a._hasMetadata
                                          and not "".join(a.name.lower().split()) == "variousartists"}
        if Zotify.CONFIG.get_save_genres() and artist_uris:
            artist_resps = self.fetch_uris_metadata(artist_uris.keys(), Artist, loader_text=GENRE)
            for artist, artist_resp in zip(artist_uris.values(), artist_resps):
                artist.parse_metadata(None, artist_resp)
                artist._needs_expansion = False
            for track in alltracks:
                genres: list[str] = [*set().union(*[set(artist.genres) for artist in track.artists if artist.genres])]
                genres.sort()
                track.genres = genres
        
        albums = {track.album for track in alltracks if track.album and not track.album.is_local}
        album_uris: dict[str, Album] = {a.uri: a for a in albums if not a._hasMetadata}
        if (Zotify.CONFIG.get_disc_track_totals() or Zotify.CONFIG.get_download_parent_album()) and albums:
            loader_text = "parent album" if Zotify.CONFIG.get_download_parent_album() else "track/disc total"
            album_resps = self.fetch_uris_metadata(album_uris.keys(), Album, loader_text=loader_text)
            for album, album_resp in zip(album_uris.values(), album_resps):
                album.parse_metadata(None, album_resp)
                if album._needs_expansion:
                    album.grab_more_children(hide_loader=True)
                if album._needs_recursion:
                    track_resps = self.fetch_uris_metadata([t.uri for t in album.tracks], Track, loader_text=loader_text)
                    album.parse_uris_metadata(track_resps, Track, loader_text=loader_text)
        
        if Zotify.CONFIG.get_export_zmd():
            from zotify.metadata import MetadataIO
            MetadataIO.to_zmd(self.id, self.ALL_NODES)
        
        if not self.prefetched_map: return
        self.requested_objs = [[prefet.get(i) or newfet.pop() for i in reversed(range(len(newfet)+len(prefet)))][::-1]
                              for newfet, prefet in zip(self.requested_objs, self.prefetched_map)] # efficient
    
    def create_m3u8_playlists(self) -> None:
        from zotify.metadata import M3U8
        for obj_list, cont_type in zip(self.requested_objs, ITEM_BULK_FETCH):
            if not any(obj_list): continue
            
            if issubclass(cont_type, DLContent): 
                filepaths: list[PurePath | None] = [dlc._real_filepaths.get(ParentStack([self, dlc])) for dlc in obj_list]
                M3U8(filepaths, cont_type, self).write(obj_list, filepaths)
                continue
            
            for obj in obj_list:
                if not any(obj._main_items):
                    Printer.hashtaged(PrintChannel.WARNING, f'SKIPPING M3U8 CREATION FOR "{obj.name}"\n' +
                                                            f'{obj.clsn.upper()} CONTAINS NO CONTENT')
                    continue
                
                dlcs: list[DLContent | None] = obj.recurse_DLC()
                filepaths: list[PurePath | None] = []
                for dlc in dlcs:
                    if dlc is None: filepaths.append(None)
                    else: # at most one ParentStack where the target obj was the Query's requested_obj
                        ps: ParentStack | None = next((ps for ps in dlc._real_filepaths if ps[1] == obj), None)
                        filepaths.append(dlc._real_filepaths.get(ps))
                M3U8(filepaths, cont_type, obj).write(dlcs, filepaths)
    
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
            Interface.ALL_DLCONTENT = {n for n in self.ALL_NODES if isinstance(n, DLContent)}
            Interface.refresh()
        
        interrupt = None
        try: super().download(ParentStack([self]))
        except BaseException as e:
            interrupt = e
            traceback = e.__traceback__
        
        while Printer.ACTIVE_LOADER:
            Printer.ACTIVE_LOADER.stop()
        n_pbars = len(Printer.ACTIVE_PBARS)
        while Printer.ACTIVE_PBARS:
            Printer.ACTIVE_PBARS.pop().close()
        if Zotify.CONFIG.get_show_any_progress() and n_pbars:
            Printer.back_up() # closing a visible pbar will print an extra newline
        
        if isinstance(interrupt, KeyboardInterrupt):
            Printer.hashtaged(PrintChannel.MANDATORY, 'USER CANCELED DOWNLOADS EARLY\n'+
                                                      'ATTEMPTING TO CLEAN UP')
        elif interrupt is not None:
            Printer.hashtaged(PrintChannel.ERROR, 'UNEXPECTED ERROR DURING DOWNLOADS\n'+
                                                  'ATTEMPTING TO CLEAN UP')
            Printer.hashtaged(PrintChannel.ERROR, str(interrupt))
        
        if Zotify.CONFIG.get_export_m3u8() and self.requested_objs:
            with Loader("Creating m3u8 files..."):
                self.create_m3u8_playlists()
        
        if interrupt is not None:
            Printer.hashtaged(PrintChannel.MANDATORY, 'CLEAN UP COMPLETE')
            Printer.logger(interrupt, PrintChannel.ERROR)
            if not isinstance(interrupt, KeyboardInterrupt):
                Printer.hashtaged(PrintChannel.ERROR, 'LOGGING ERROR AND TRACEBACK')
                Printer.logger(self.__dict__, PrintChannel.ERROR)
                raise interrupt.with_traceback(traceback)
    
    def execute(self):
        self.handle_zmd_prefetch()
        self.parse_query_metadata(self.fetch_query_metadata())
        self.conditional_metadata()
        self.download()


class VerifyLibrary(Query):
    _contains = Track
    name = "Verifiable Tracks"
    
    def fetch_verifiable_metadata(self) -> tuple[dict[str, list[PurePath]], list[list[dict]]]:
        """ ONLY WORKS WITH ARCHIVED TRACKS (THEORETICALLY GUARANTEES METADATA FETCH) """
        # prioritize most recent paths first
        from zotify.metadata import SongArchive
        archived_ids = SongArchive().ids()[::-1]
        archived_filenames_or_paths = SongArchive().paths()[::-1]
        
        paths_per_track: dict[str, list[PurePath]] = {}
        
        track_ids: set[str] = set()
        for filepath in walk_directory_for_tracks(Track._path_root):
            if filepath in archived_filenames_or_paths:
                uri = f"{TRACK}:{archived_ids[archived_filenames_or_paths.index(filepath)]}"
                if uri not in paths_per_track:
                    paths_per_track[uri] = []
                paths_per_track[uri].append(filepath)
                track_ids.add(uri)
        
        return paths_per_track, [self.fetch_uris_metadata(track_ids, Track)]
    
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
                                                       '(UPDATED TAGS TO MATCH CURRENT API METADATA)')
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, f'FAILED TO CORRECT METADATA FOR "{track.rel_path(path)}"')
            Printer.traceback(e)  
    
    def execute(self):
        # no zmd prefetch, meant to update entries
        paths_per_track, track_resps = self.fetch_verifiable_metadata()
        self.parse_query_metadata(track_resps, [Track])
        self.conditional_metadata()
        parent_stack = ParentStack([self])
        for track in self.pbar(self.requested_objs[0], parent_stack):
            for path in paths_per_track[track]:
                self.verify_metadata(path, track)
            Printer.refresh_all_pbars(parent_stack.PBARS)


class UserItem(Query):
    _contains = User
    _interactive = True
    _inner_stripper = None
    _outer_stripper = None
    _url = USER_URL
    
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self.name = self.clsn + "s"
    
    def fetch_user_items(self) -> list[dict]:
        with Loader(f"Fetching {self.name}...", disabled=self._interactive):
            user_item_resps = Zotify.invoke_url_nextable(f"{self._url}?{MARKET_APPEND}", stripper=self._outer_stripper)
        return user_item_resps
    
    def display_select_user_items(self, user_item_resps: list[dict]) -> list[dict]:
        display_list = [[i+1, str(resp.get(self._inner_stripper, resp)[NAME])] for i, resp in enumerate(user_item_resps)]
        Printer.table(self.uppers, ('ID', 'Name'), [[0, f"ALL {self.uppers}"]] + display_list)
        selected_item_resps: list[None | dict] = select([None] + user_item_resps, first_ID=0)
        
        if selected_item_resps[0] == None:
            # option 0 == get all choices
            selected_item_resps = user_item_resps[1:]
        return selected_item_resps
    
    def execute(self):
        user_item_resps = self.fetch_user_items()
        if not user_item_resps: return
        if self._interactive:
            user_item_resps = self.display_select_user_items(user_item_resps)
        if self._inner_stripper and self._contains in {Track, Album}:
            for resp in user_item_resps:
                resp[self._inner_stripper][ADDED_AT] = resp.get(ADDED_AT)
            user_item_resps = [resp[self._inner_stripper] for resp in user_item_resps]
        if self._contains is Playlist:
            user_item_resps = self.fetch_uris_metadata([resp[URI] for resp in user_item_resps], Playlist)
        self.parse_query_metadata([user_item_resps], [self._contains])
        self.conditional_metadata()
        self.download()


class LikedSong(UserItem):
    _contains = Track
    _interactive = False
    _inner_stripper = TRACK
    _url = USER_SAVED_TRACKS_URL
    
    # use static portion of OUTPUT_LIKED_SONGS
    def dynamic_path_root(self) -> PurePath:
        m3u8_dir = self._path_root
        if Zotify.CONFIG.get_liked_songs_archive_m3u8(): 
            for part in PurePath(Zotify.CONFIG.get_output(self.clsn)).parts:
                if "{" in part or "}" in part: break
                m3u8_dir = m3u8_dir / part
        return m3u8_dir
    
    def create_m3u8_playlists(self):
        from zotify.metadata import M3U8
        liked_tracks: list[Track] = self.requested_objs[0]
        filepaths = [t._real_filepaths.get(ParentStack([self, t])) for t in liked_tracks]
        m3u8 = M3U8(filepaths, Track, self)
        
        archive_mode = Zotify.CONFIG.get_liked_songs_archive_m3u8()
        archive_dir = self.dynamic_path_root()
        if archive_dir == self._path_root: # fallback to common dir
            archive_dir = m3u8.dynamic_dir(filepaths) if archive_mode else m3u8.path.parent
        if not archive_dir:
            archive_dir = self._path_root
        m3u8.path = archive_dir / f"{self.name}.m3u8"
        
        append_strs = []
        if archive_mode and Path(m3u8.path).exists():
            raw_liked_archive = M3U8.fetch_songs(m3u8.path)
            for i, liked_archive_path in enumerate(raw_liked_archive[1::3]):
                sync_point = M3U8.find_sync_point(filepaths, liked_archive_path[:-1])
                if sync_point is not None:
                    liked_tracks = liked_tracks[:sync_point] # doesn't include matching Track obj
                    append_strs = raw_liked_archive[3*i:] # includes matching track m3u8 entry
                    break
                if i == 0:
                    Printer.hashtaged(PrintChannel.WARNING, 'FIRST TRACK IN EXISTING M3U8 NOT FOUND IN CURRENT LIKED SONGS\n' +
                                                            'PERFORMING DEEP SEARCH FOR SYNC POINT')
            if not append_strs:
                reason = 'READ EXISTING M3U8' if not raw_liked_archive else 'FIND SYNC POINT'
                Printer.hashtaged(PrintChannel.WARNING, 'FAILED Liked Songs ARCHIVE M3U8 UPDATE\n' +
                                                        'FAILED TO ' + reason + '\n' +
                                                        'FALLING BACK TO STANDARD M3U8 CREATION')
        
        m3u8.write(liked_tracks, filepaths)
        m3u8.append(append_strs)


class SavedAlbum(UserItem):
    _contains = Album
    _inner_stripper = ALBUM
    _url = USER_SAVED_ALBUMS_URL


class UserPlaylist(UserItem):
    _contains = Playlist
    _url = USER_PLAYLISTS_URL


class FollowedArtist(UserItem):
    _contains = Artist
    _outer_stripper = ARTISTS
    _url = USER_FOLLOWED_ARTISTS_URL
