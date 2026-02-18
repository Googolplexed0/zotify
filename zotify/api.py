from __future__ import annotations
import ffmpy
import functools
import music_tag
import requests
import subprocess
import uuid
from typing import Type

from zotify import __version__
from zotify.const import *
from zotify.termoutput import PrintChannel, Printer, Loader, Interface
from zotify.utils import *


def filter_search_query(search_query: str, item_types: tuple[str]) -> dict[str, str]:
    
    search_filters: dict[str, list[set | str]] = {
        TYPE:               [{'/t',  '/type',},                  ','.join(item_types[:4])             ],
        SEARCH_QUERY_SIZE:  [{'/l',  '/limit', '/s', '/size',},  Zotify.CONFIG.get_search_query_size()],
        OFFSET:             [{'/o',  '/offset',},                "0"                                  ],
        INCLUDE_EXTERNAL:   [{'/ie', '/include-external',},      "False"                              ],
        'q':                [{},                                 search_query                         ],
    }
    for k, v in search_filters.items():
        search_filters[k][0] = {" " + flag + " " for flag in v[0]}
    
    if "/" not in search_query:
        return {k: v[-1] for k, v in search_filters.items() if v[-1]}
    
    Printer.debug(f"Filtering Search Query: {search_query}")
    parsed_query = [search_query]
    for filter_param in search_filters:
        filter_flags = search_filters[filter_param][0]
        for filter_flag in filter_flags:
            val_and_suffix = None
            for i, part in enumerate(parsed_query):
                if filter_flag not in part:
                    continue
                parsed_query.remove(part)
                prefix, val_and_suffix = part.split(filter_flag, 1)
                parsed_query.insert(i, val_and_suffix)
                parsed_query.insert(i, prefix)
                for k, v in search_filters.items():
                    search_filters[k][-1] = val_and_suffix if k == filter_param \
                                      else v[-1].replace(filter_flag + val_and_suffix, "").strip()
                break
            if val_and_suffix:
                break
    
    # type / value validation
    max_offset = 1000
    max_limit = 50
    for k, v in list(search_filters.items()):
        if   k == TYPE:              fv = ",".join([t for t in v[-1].split() if t in item_types])
        elif k == SEARCH_QUERY_SIZE: fv = str(clamp(0, int(v[-1]), max_offset + max_limit))
        elif k == OFFSET:            fv = str(clamp(0, int(v[-1]), max_offset            ))
        elif k == INCLUDE_EXTERNAL:  fv = "audio" if v[-1].lower() == "true" else ""
        else:                        fv = v[-1]
        if fv:
            search_filters[k] = fv
        else:
            del search_filters[k]
    
    Printer.debug(search_filters)
    return search_filters


def fetch_search_display(search_query: str) -> list[str]:
    # example search query: working in a coal mine /l 5 /type track album
    
    table_headers = {
        TRACKS:     ('ID', 'Name', 'Artists'    ),
        ALBUMS:     ('ID', 'Name', 'Artists'    ),
        ARTISTS:    ('ID', 'Name'               ),
        PLAYLISTS:  ('ID', 'Name', 'Owner'      ),
        EPISODES:   ('ID', 'Name', 'Show'       ),
        SHOWS:      ('ID', 'Name', 'Publisher'  ),
    }
    
    params = filter_search_query(search_query, tuple(t[:-1] for t in table_headers))
    stop = int(params.pop(SEARCH_QUERY_SIZE))
    url = f"{SEARCH_URL}?{MARKET_APPEND}"
    items = Zotify.invoke_url_nextable(url, stop=stop, stripper=tuple(t for t in table_headers if t[:-1] in params[TYPE]), params=params)
    
    search_result_uris = []
    for item_type, headers in table_headers.items():
        if item_type not in items or not len(items[item_type]):
            continue
        
        resps: list[dict] = [i for i in items[item_type] if i is not None]
        counter = len(search_result_uris) + 1
        if item_type == TRACKS:
            data = [ [resps.index(t) + counter,
                      str(t[NAME]) + (" [E]" if t[EXPLICIT] else ""),
                      ','.join([artist[NAME] for artist in t[ARTISTS]]) ] for t in resps]
        elif item_type == ALBUMS:
            data = [ [resps.index(m) + counter,
                      str(m[NAME]),
                      ','.join([artist[NAME] for artist in m[ARTISTS]]) ] for m in resps]
        elif item_type == ARTISTS:
            data = [ [resps.index(a) + counter,
                      str(a[NAME])                                      ] for a in resps]
        elif item_type == PLAYLISTS:
            data = [ [resps.index(p) + counter,
                      str(p[NAME]),
                      str(p[OWNER][DISPLAY_NAME])                       ] for p in resps]
        if item_type == EPISODES:
            data = [ [resps.index(e) + counter,
                      str(e[NAME]) + (" [E]" if e[EXPLICIT] else ""),
                      str(e[SHOW][NAME])                                ] for e in resps]
        elif item_type == SHOWS:
            data = [ [resps.index(s) + counter,
                      str(s[NAME]) + (" [E]" if s[EXPLICIT] else ""),
                      str(s[PUBLISHER])                                 ] for s in resps]
        
        search_result_uris.extend([i[URI] for i in resps])
        Printer.table(item_type.capitalize(), headers, data)
    
    return search_result_uris


class Tree():
    query: Query = None
    allNodes:   set[Content] = set()
    subContent: set[Content] = set()
    
    def __init__(self, parent: Content | Container | None, node: Content | Container):
        self.node = node
        self.parent = parent
        self.branch: list[Content | Container] = (self.parent.tree.branch if self.parent else []) + [self.node,]
        self.children: set[Content | Container] = set()
        self.siblings: set[Content | Container] = set()
    
    def __contains__(self, c: Content | Container) -> bool:
        return c in self.branch
    
    def __iter__(self):
        return iter(self.branch)
    
    def __str__(self) -> str:
        return f"[{' -> '.join([c.clsn for c in self.branch])}]"
    
    def createChild(self, rawobj: Content | Container) -> Content | Container:
        """ Returns matching obj if found, else passed obj after adopting """
        
        # same track, same parent container
        equal = {cont for cont in self.allNodes if rawobj == cont}
        obj = equal.pop() if equal else rawobj
        
        # same track, different parent container
        obj.tree.siblings = {cont for cont in self.allNodes if obj % cont}
        for sib in obj.tree.siblings:
            sib.tree.siblings.add(obj)
        
        self.allNodes.add(obj)
        if not isinstance(obj, Container) and all([isinstance(o, Container) for o in self]):
            self.subContent.add(obj)
        self.children.add(obj)
        return obj


class DynamicClassNameAttrs(type):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        cls.clsn = "".join([" " + c if c.isupper() else c for c in cls.__name__])[1:]
        cls.lowers = cls.clsn.lower().removesuffix("y") +\
                     ("ie" if cls.clsn.endswith("y") else "") +\
                     ("s" if not cls.clsn.endswith("s") else "")
        cls.uppers = cls.lowers.upper()


class Content(metaclass=DynamicClassNameAttrs):
    # CONFIG must be loaded with args before any Content classes are instantiated/imported
    _path_root: PurePath = Zotify.CONFIG.get_root_path()
    _regex_flag: re.Pattern | None = None
    url = ""
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        if ":" in id_or_uri:
            self.uri = id_or_uri.split(":", 1)[-1]
            self.id = self.uri.split(":", 1)[-1] # local file URIs will have more than 2 colons
            if self.id.count(":"):
                self.id = None
        else:
            self.id = id_or_uri
            self.uri = f"{self.clsn.lower()}:{self.id}"
        self.parent = parent
        self.tree = Tree(self.parent, self)
        
        self.downloaded = False # self / all child DLContent must have valid Path if True
        self.hasMetadata = False
        self.printing_label = ""
        self.skippable = None
        
        self.name = ""
    
    def __mod__(self, other) -> bool:
        # used for evaluating api equality
        if isinstance(other, Content):
            return self.uri == other.uri
        return False
    
    def __eq__(self, other) -> bool:
        # used for evaluating object tree equality
        if isinstance(other, Content):
            parent_match = self.parent.uri == other.parent.uri
            both_child_of_content = not (isinstance(self.parent, Container) or isinstance(other.parent, Container))
            return self % other and (parent_match or both_child_of_content)
        return False
    
    def __hash__(self):
        return hash((self.uri, self.parent.uri if self.parent else None))
    
    def update_id(self, new_id: str):
        if self.id != new_id:
            self.id = new_id
            Printer.debug(f"Updated {self.name} ({self.uri}) ID to {self.id}")
    
    def rel_path(self, path: PurePath = None) -> PurePath | None:
        if path is None: path = self.filepath
        # if path is None and isinstance(self, DLContent):
        #     path = self.fill_output_template()
        if path is None:
            Printer.debug(f'NO PATH TO RELATIVIZE FOR "{self.printing_label}"')
            return
        return path.relative_to(self._path_root)
    
    def regex_check(self, skip_debug_print: bool = False) -> bool:
        if self._regex_flag is None:
            return False
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
    
    def fetch_metadata(self) -> dict[str]:
        with Loader(f"Fetching {self.clsn.lower()} information..."):
            _, resp = Zotify.invoke_url(f'{self.url}/{self.id}?{MARKET_APPEND}')
        if resp:
            return resp
        else:
            raise ValueError("No Metadata Fetched")
    
    # placeholder func, overwrite in each child class
    def parse_metadata(self, resp: dict):
        pass
    
    def parse_linked_objs(self, resps: list[dict], obj: Content | Container | tuple[Content | Container]) -> list[Content | Container]:
        if isinstance(obj, tuple):
            type_select = tuple(cls.__name__.lower() for cls in obj)
            rawobjs: list[Content | Container] = [obj[type_select.index(resp[TYPE])](resp[URI], self) for resp in resps]
        else:
            rawobjs: list[Content | Container] = [obj(resp[URI], self) for resp in resps]
        
        objs = []
        for rawobj, resp in zip(rawobjs, resps):
            obj = self.tree.createChild(rawobj)
            if not obj.hasMetadata: # overly cautious
                obj.parse_metadata(resp) # theoretically shouldn't lose metadata by re-parsing if obj was parsed prev
            objs.append(obj)
        return objs
    
    def mark_downloaded(self, path: PurePath | None = None): 
        # Best practice / convention is to only call this on self
        self.downloaded = True
        if isinstance(self, DLContent) and path is not None:
            self.filepath = path
        
        # copy downloaded file to all siblings' paths
        for sib in self.tree.siblings:
            if sib.downloaded:
                continue
            
            if not (isinstance(self, DLContent) and isinstance(sib, DLContent)):
                pass
            elif Zotify.CONFIG.get_download_parent_album() and isinstance(sib, Track) and sib.album == self.parent:
                # sibling spawned this object as part of its parent album download
                # do not copy file to sibling's path
                sib.mark_downloaded(self.filepath)
            elif sib not in self.tree.query.downloadables:
                pass
            else:
                Printer.debug(f"{self.tree} has Sibling at {sib.tree}")
                sib.filepath = check_path_dupes(sib.fill_output_template())
                if Path(self.filepath).exists(): # SHOULD always be true
                    pathlike_move_safe(self.filepath, sib.filepath, copy=True)
                    if isinstance(sib, Track):
                        sib.set_audio_tags(sib.filepath)
                    Printer.debug(f'"{self.rel_path()}" Copied to Sibling at "{sib.rel_path()}"')
                else:
                    Printer.hashtaged(PrintChannel.WARNING, f"SIBLING FAILED TO COPY METADATA\n" +
                                                            f'MISSING FILE EXPECTED AT PATH "{self.filepath}"')
                sib.mark_downloaded()
        
        if self.parent and all({i.downloaded for i in self.parent.tree.children}):
            self.parent.mark_downloaded()
    
    def dashboard(self, extra_attrs: list[str] = [], suppress_id: bool = False, force_clsn: bool = False) -> str:
        db = ""
        attrs = (["id", "name"] if not suppress_id else ["name",]) + extra_attrs
        for attr in attrs:
            adds = Interface.parse_dbs(self, attr)
            if force_clsn:
               adds = "\n".join([f"{self.clsn} {line}" if self.clsn not in line else line for line in adds.split("\n")])
            db += adds
            if attr != attrs[-1]:
                db += "\n"
        return db


class User(Content):
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self.display_name = ""
        self.external_urls: dict = {}
    
    def parse_metadata(self, user_resp: dict):
        self.update_id(user_resp[ID])
        self.display_name: str = user_resp[DISPLAY_NAME]
        self.external_urls: dict = user_resp[EXTERNAL_URLS]
        self.hasMetadata = True


class DLContent(Content):
    _codecs: dict[str, str] = {}
    _ext = ""
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self.dl_status = ""
        self.filepath: PurePath | None = None
        self.in_dir_archive = False
        self.in_global_archive = False
        
        self.duration_ms = 0
        self.is_playable = False
    
    def set_dl_status(self, str_status) -> Loader:
        self.dl_status = str_status
        if Zotify.CONFIG.get_standard_interface():
            Interface.refresh()
        return Loader(str_status + "...")
    
    # placeholder func, overwrite in each child class
    def fill_output_template(self) -> PurePath:
        pass
    
    def check_skippable(self, check_path_glob: bool = False) -> bool:
        if self.skippable is not None:
            return self.skippable
        self.skippable = False
        
        if self.downloaded:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" ({self.clsn.upper()} ALREADY DOWNLOADED THIS SESSION)')
            self.skippable = True
        elif self.regex_check(skip_debug_print=Zotify.CONFIG.get_optimized_dl_order()):
            self.skippable = True
        
        if self.skippable:
            return self.skippable
        
        path = self.fill_output_template()
        path_exists = Path(path).is_file() and Path(path).stat().st_size
        if check_path_glob and path.suffix == ".copy":
            # file suffix agnostic check
            for file_match in Path(path.parent).glob(path.stem + ".*", case_sensitive=True):
                if file_match.stat().st_size:
                    path_exists = True
                    break
        
        self.in_dir_archive = self.id in get_archived_item_ids(path.parent)
        self.in_global_archive = self.id in get_archived_item_ids()
        if not Zotify.CONFIG.get_optimized_dl_order():
            Printer.debug("Duplicate Check\n" +
                         f"File Already Exists: {path_exists}\n" +
                         f"id in Local Archive: {self.in_dir_archive}\n" +
                         f"id in Global Archive: {self.in_global_archive}")
        
        if not self.is_playable and not Zotify.CONFIG.get_bypass_metadata():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" ({self.clsn.upper()} IS UNAVAILABLE)')
            self.skippable = True
        elif path_exists and Zotify.CONFIG.get_skip_existing() and Zotify.CONFIG.get_disable_directory_archives():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.rel_path(path)}" (FILE ALREADY EXISTS)')
            self.mark_downloaded(path)
            self.skippable = True
        elif self.in_dir_archive and Zotify.CONFIG.get_skip_existing() and not Zotify.CONFIG.get_disable_directory_archives():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" ({self.clsn.upper()} ALREADY EXISTS)')
            path = get_archived_item_paths(path.parent)[get_archived_item_ids(path.parent).index(self.id)]
            self.mark_downloaded(path)
            self.skippable = True
        elif self.in_global_archive and Zotify.CONFIG.get_skip_previously_downloaded():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" ({self.clsn.upper()} DOWNLOADED PREVIOUSLY)')
            path = get_archived_item_paths()[get_archived_item_ids().index(self.id)]
            self.mark_downloaded(path)
            self.skippable = True
        
        return self.skippable
    
    def fetch_content_stream(self, stream, temppath: PurePath, pbar_stack: list) -> str:
        time_start = time.time()
        total_size = stream.input_stream.size
        downloaded = 0
        disable = Zotify.CONFIG.get_standard_interface() or not Zotify.CONFIG.get_show_download_pbar()
        pos, pbar_stack = Printer.pbar_position_handler(1, pbar_stack)
        pbar = Printer.pbar(desc=self.printing_label, total=total_size, unit='B', unit_scale=True,
                            unit_divisor=1024, disable=disable, pos=pos)
        
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
    
    # placeholder func, overwrite in each child class
    def download(self, pbar_stack: list):
        pass
    
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


class Track(DLContent):
    _regex_flag = Zotify.CONFIG.get_regex_track()
    _codecs = CODEC_MAP_TRACK
    _ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "ogg")
    url = TRACK_URL
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None) -> None:
        super().__init__(id_or_uri, parent)
        self.disc_number = ""
        self.track_number = ""
        self.year = ""
        self.album: Album = None
        self.artists: list[Artist] = []
        
        # only fetched if config set
        self.genres: list[str] = []
        self.lyrics: list[str] = []
        
        # only set by Playlist API or UserItem API
        self.added_at = ""
        # only set by Playlist API
        self.added_by = ""
        self.is_local = ""
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["track_number", "artists", "album"], suppress_id=suppress_id)
    
    def parse_metadata(self, track_resp: dict[str, str | int | bool]) -> None:
        self.update_id(track_resp[ID])
        self.name: str = track_resp[NAME]
        self.disc_number = str(track_resp[DISC_NUMBER])
        self.duration_ms: int = track_resp[DURATION_MS]
        self.is_playable: bool = track_resp[IS_PLAYABLE] if IS_PLAYABLE in track_resp else False
        self.track_number = str(track_resp[TRACK_NUMBER]).zfill(2)
        
        if ALBUM in track_resp:
            if not track_resp[ALBUM][URI]:
                track_resp[ALBUM][URI] = f":local:{track_resp[ALBUM][NAME]}:::" # fallback for local tracks
            self.album: Album = self.parse_linked_objs([track_resp[ALBUM]], Album)[0]
        elif isinstance(self.parent, Album):
            self.album = self.parent
       
        if ARTISTS in track_resp:
            for artist in track_resp[ARTISTS]:
                if not artist[URI]:
                    artist[URI] = f":local:{artist[NAME]}:::" # fallback for local tracks
            self.artists = self.parse_linked_objs(track_resp[ARTISTS], Artist)
            self.printing_label = fix_filename(self.artists[0].name) + ' - ' + fix_filename(self.name)
        
        if isinstance(self.parent, Playlist):
            self.added_at = track_resp[ADDED_AT]
            self.added_by = track_resp[ADDED_BY]
            self.is_local = track_resp[IS_LOCAL]
        
        self.hasMetadata = True
    
    def compare_metadata(self):
        # Compares metadata in self (just fetched) against metadata on file (at self.filepath),
        # returns Truthy value if discrepancy is found
        
        reliable_tags = (
            conv_artist_format(self.artists), conv_genre_format(self.genres), self.name, self.album.name, 
            conv_artist_format(self.album.artists), self.album.year, self.disc_number, self.track_number
            )
        unreliable_tags = {
            TOTALTRACKS: self.album.total_tracks if Zotify.CONFIG.get_disc_track_totals() else None,
            TOTALDISCS: self.album.total_discs if Zotify.CONFIG.get_disc_track_totals() else None, 
            COMPILATION: self.album.compilation,
            LYRICS: self.lyrics,
            "trackid": self.id,
            }
        reliable_tags_onfile, unreliable_tags_onfile = self.get_audio_tags()
        
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
    
    def verify_metadata(self) -> None:
        """Overwrite metadata on file (at self.filepath) with current metadata if necessary"""
        
        mismatches = self.compare_metadata()
        if not mismatches:
            Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{self.rel_path()}"\n' +
                                                       '(NO UPDATES REQUIRED)')
            return
        
        try:
            Printer.debug(f'Metadata Mismatches:', mismatches)
            self.set_audio_tags(self.filepath)
            Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{self.rel_path()}"\n' +
                                                      f'(UPDATED TAGS TO MATCH CURRENT API METADATA)')
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, F'FAILED TO CORRECT METADATA FOR "{self.rel_path()}"')
            Printer.traceback(e) 
    
    def fill_output_template(self, output_template: str = "") -> PurePath:
        if Zotify.CONFIG.get_bypass_metadata():
            return Zotify.CONFIG.get_root_path() / f"{self.id}.{self._ext}"
        
        if not output_template:
            try:
                output_template = Zotify.CONFIG.get_output(self.parent.clsn)
            except:
                Printer.debug(f"Unexpected Track Parent: {self.parent.clsn}")
                output_template = Zotify.CONFIG.get_output('Query')
        
        replstrset = [
            {"{id}", "{track_id}", "{song_id}"},
            {"{name}", "{song_name}", "{track_name}", "{song_title}", "{track_title}",},
            {"{track_number}", "{song_number}", "{track_num}", "{song_num}", "{album_number}", "{album_num}",},
            {"{disc_number}", "{disc_num}",},
            
            {"{artist}", "{track_artist}", "{song_artist}", "{main_artist}", "{primary_artist}",},
            {"{artists}", "{track_artists}", "{song_artists}",},
            
            {"{album_id}",},
            {"{album}", "{album_name}",},
            {"{album_artist}",},
            {"{album_artists}",},
            {"{date}", "{release_date}",},
            {"{year}", "{release_year}",},
        ]
        
        repl_mds = [
            self.id,
            self.name,
            self.track_number,
            self.disc_number,
            
            self.artists[0].name,
            conv_artist_format(self.artists),
            
            self.album.id,
            self.album.name,
            self.album.artists[0].name,
            conv_artist_format(self.album.artists),
            self.album.release_date,
            self.album.year,
        ]
        
        if Zotify.CONFIG.get_disc_track_totals():
            if self.album.needs_expansion: self.album.grab_more_children(hide_loader=True) # moved from Query.fetch_extra_metadata()
            replstrset += [{"{total_tracks}",}, {"{total_discs}",},] 
            repl_mds += [self.album.total_tracks, self.album.total_discs]
        
        if isinstance(self.parent, Playlist):
            replstrset += [{"{playlist}",}, {"{playlist_id}",}, {"{playlist_number}", "{playlist_num}",},]
            playlist_number = str(self.parent.tracks_or_eps.index(self) + 1).zfill(2)
            repl_mds += [self.parent.name, self.parent.id, playlist_number]
        
        for replstrs, repl_md in zip(replstrset, repl_mds):
            for replstr in replstrs:
                output_template = output_template.replace(replstr, fix_filename(repl_md))
        
        return Zotify.CONFIG.get_root_path() / f"{output_template}.{self._ext}"
    
    def fetch_lyrics(self) -> None:
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
            Printer.hashtaged(PrintChannel.SKIPPING, f'LYRICS FOR "{self.printing_label}" ({e.args[0]})')
            return
        
        if Zotify.CONFIG.get_lyrics_to_file():
            lyricdir = Zotify.CONFIG.get_lyrics_location()
            if lyricdir is None:
                lyricdir = self.fill_output_template().parent
            Path(lyricdir).mkdir(parents=True, exist_ok=True)
            
            lrc_filename = self.fill_output_template(Zotify.CONFIG.get_lyrics_filename()).stem
            
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
    
    @staticmethod
    def parse_audio_tags(filepath: PurePath) -> tuple[tuple, dict]:
        tags = music_tag.load_file(filepath)
        
        artists = conv_artist_format(tags[ARTIST].values)
        genres = conv_genre_format(tags[GENRE].values)
        track_name = tags[TRACKTITLE].val
        album_name = tags[ALBUM].val
        album_artist = conv_artist_format(tags[ALBUMARTIST].values)
        release_year = str(tags[YEAR].val)
        disc_number = str(tags[DISCNUMBER].val)
        track_number = str(tags[TRACKNUMBER].val).zfill(2)
        
        unreliable_tags = [TOTALTRACKS, TOTALDISCS, COMPILATION, LYRICS]
        custom_tags = ["trackid"]
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
            fetch_method = "legit"
            try:
                val = tags[utag].val
            except:
                fetch_method = "hacky"
                if utag in tag_dict:
                    val = tag_dict[utag]
            
            if val is None:
                pass
            elif utag == LYRICS:
                val = [line + "\n" for line in val.splitlines()]
            elif utag == COMPILATION:
                val = int(val)
            elif MP3_CUSTOM_TAG_PREFIX in utag:
                val = val.text
                if len(val) == 1:
                    val = val[0]
            elif M4A_CUSTOM_TAG_PREFIX in utag:
                if len(val) == 1:
                    val = val[0].decode()
                else:
                    val = [v.decode() for v in val]
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
    
    def get_audio_tags(self) -> tuple[tuple, dict]:
        return self.parse_audio_tags(self.filepath)
    
    def set_audio_tags(self, path: PurePath) -> None:
        file_tags: music_tag.AudioFile = music_tag.load_file(path)
        img = None
        
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
        set_tag_safe(       ARTIST,      conv_artist_format(self.artists))
        set_tag_safe(       ARTIST,      conv_artist_format(self.artists))
        set_tag_safe(       GENRE,       conv_genre_format(self.genres))
        set_tag_safe(       TRACKTITLE,  self.name)
        set_tag_safe(       DISCNUMBER,  self.disc_number)
        set_tag_safe(       TRACKNUMBER, self.track_number)
        if self.album:
            set_tag_safe(   ALBUM,       self.album.name)
            set_tag_safe(   ALBUMARTIST, conv_artist_format(self.album.artists))
            set_tag_safe(   YEAR,        self.album.year)
            img =                        requests.get(self.album.image_url).content # expect jpeg
            set_tag_safe(   ARTWORK,     img)
        
        # Unreliable Tags
        custom_tag(         "trackid",   self.id)
        custom_tag(         "uri",       self.uri)
        
        if self.album and Zotify.CONFIG.get_disc_track_totals():
            set_tag_safe(TOTALTRACKS,    self.album.total_tracks)
            set_tag_safe(TOTALDISCS,     self.album.total_discs)
        
        if self.album and self.album.compilation:
            set_tag_safe(COMPILATION,    self.album.compilation)
        
        if Zotify.CONFIG.get_lyrics_to_metadata():
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
        jpg_path = path.parent / ('cover.jpg' if isinstance(self.parent, Album) else path.stem + '.jpg')
        if not Path(jpg_path).exists():
            with open(jpg_path, 'wb') as jpg_file:
                jpg_file.write(img)
    
    def check_skippable(self) -> bool:
        if super().check_skippable():
            return self.skippable
        elif self.album:
            self.skippable = self.album.check_skippable()
        
        if self.skippable:
            return self.skippable
        
        return self.parent.check_skippable()
    
    def download(self, pbar_stack: list) -> None:
        if self.downloaded:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" ({self.clsn.upper()} ALREADY DOWNLOADED THIS SESSION)')
            return
        
        if Zotify.CONFIG.get_download_parent_album() and not isinstance(self.parent, Album):
            # only called when not get_optimized_dl() since CONFIG.download_parent_album() replaces Tracks with optimized Albums
            self.album.download(pbar_stack)
            return
        
        if Zotify.CONFIG.get_always_check_lyrics():
            self.fetch_lyrics()
        
        if self.check_skippable():
            return
        
        Interface.bind(self)
        with self.set_dl_status("Preparing Download"):
            path = check_path_dupes(self.fill_output_template())
            if path != self.fill_output_template():
                # path exists but id isn't archived OR skipping disabled
                Printer.debug(f"Path Duplicate Not Being Skipped:\n" +
                               "ID not Archived" if Zotify.CONFIG.get_skip_existing() else "Skipping Disabled")
            
            temppath = path.with_suffix(".tmp")
            if Zotify.CONFIG.get_temp_download_dir():
                temppath = Zotify.CONFIG.get_temp_download_dir() / f'zotify_{str(uuid.uuid4())}_{self.id}.tmp'
        
        stream = Zotify.get_content_stream(self)
        if stream is None:
            Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING TRACK - FAILED TO GET CONTENT STREAM\n' +
                                                 f'Track_ID: {self.id}')
            return
        
        self.set_dl_status("Downloading Stream")
        time_elapsed_dl = self.fetch_content_stream(stream, temppath, pbar_stack)
        
        if not Zotify.CONFIG.get_always_check_lyrics():
            self.fetch_lyrics()
        
        with self.set_dl_status("Converting File"):
            # convert temppath -> path here
            create_download_directory(path.parent)
            time_elapsed_ffmpeg = self.convert_audio_format(temppath, path)
            if time_elapsed_ffmpeg is None:
                path = pathlike_move_safe(temppath, path.with_suffix(".ogg"))
            self.mark_downloaded(path)
        
        try:
            self.set_audio_tags(path)
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO WRITE METADATA\n' +
                                                  'Ensure FFMPEG is installed and added to your PATH')
            Printer.traceback(e)
        
        Printer.dl_complete(self, time_elapsed_dl, time_elapsed_ffmpeg)
        if not Zotify.CONFIG.get_bypass_metadata():
            if not self.in_dir_archive:
                add_obj_to_song_archive(self, self.filepath.parent)
            if not self.in_global_archive:
                add_obj_to_song_archive(self)


class Episode(DLContent):
    _path_root: PurePath = Zotify.CONFIG.get_root_podcast_path()
    _regex_flag = Zotify.CONFIG.get_regex_episode()
    _codecs = CODEC_MAP_EPISODE
    _ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "copy")
    url = EPISODE_URL
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self.desc = ""
        self.explicit = False
        self.external = False
        self.partner_url = ""
        self.release_date = ""
        self.show: Show = None
        
        # only set by Playlist API
        self.added_at = ""
        self.added_by = ""
        self.is_local = ""
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["show",], suppress_id=suppress_id)
    
    def parse_metadata(self, episode_resp: dict[str, str | int | bool]):
        self.update_id(episode_resp[ID])
        self.name: str = episode_resp[NAME]
        self.desc: str = episode_resp[DESCRIPTION]
        self.duration_ms: int = episode_resp[DURATION_MS]
        self.explicit: bool = episode_resp[EXPLICIT]
        self.external: bool = episode_resp[IS_EXTERNALLY_HOSTED]
        self.release_date: str = episode_resp[RELEASE_DATE]
        self.is_playable: bool = episode_resp[IS_PLAYABLE]
        
        if SHOW in episode_resp:
            self.show = self.parse_linked_objs([episode_resp[SHOW]], Show)[0]
            self.printing_label = fix_filename(self.show.name) + ' - ' + fix_filename(self.name)
        elif isinstance(self.parent, Show):
            self.show = self.parent
            self.printing_label = fix_filename(self.show.name) + ' - ' + fix_filename(self.name)
        
        elif isinstance(self.parent, Playlist):
            self.added_at = episode_resp[ADDED_AT]
            self.added_by = episode_resp[ADDED_BY]
            self.is_local = episode_resp[IS_LOCAL]
        
        self.hasMetadata = True
    
    def fill_output_template(self) -> PurePath:
        if Zotify.CONFIG.get_bypass_metadata():
            return Zotify.CONFIG.get_root_podcast_path() / f"{self.id}.{self._ext}"
        
        return Zotify.CONFIG.get_root_podcast_path() / fix_filename(f"{self.show.name}/{self.printing_label}.{self._ext}")
    
    def check_skippable(self) -> bool:
        if super().check_skippable(check_path_glob=True):
            return self.skippable
        
        return self.parent.check_skippable()
    
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
    
    def download(self, pbar_stack: list | None):
        if self.check_skippable():
            return
        
        Interface.bind(self)
        with self.set_dl_status("Preparing Download"):
            path = self.fill_output_template()
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
            time_elapsed_dl = self.fetch_content_stream(stream, temppath, pbar_stack)
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
            self.mark_downloaded(path)
        
        Printer.dl_complete(self, time_elapsed_dl, time_elapsed_ffmpeg)
        if not Zotify.CONFIG.get_bypass_metadata() and not self.in_dir_archive:
            add_obj_to_song_archive(self, self.filepath.parent)


class Container(Content):
    _show_pbar = not Zotify.CONFIG.get_standard_interface()
    _contains = Content
    _preloaded = 0
    _fetch_q = 50
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self._main_items: list[DLContent | Container] = []
        self.needs_expansion = False
        self.needs_recursion = False
    
    @property
    def len(self):
        return len(self._main_items)
    
    def fetch_items(self, item_key: str, args: str = "", hide_loader: bool = False) -> list[dict]:
        with Loader(f'Fetching {self.clsn.lower()} {item_key}...', disabled=hide_loader):
            if args: args = "&" + args
            return Zotify.invoke_url_nextable(f'{self.url}/{self.id}/{item_key}?{MARKET_APPEND}{args}',
                                              limit=self._fetch_q, offset=self.len)
    
    def recurse_DLC(self) -> list[DLContent]:
        children = []
        for c in self._main_items:
            children.append(c) if isinstance(c, DLContent) else children.extend(c.recurse_DLC())
        return children
    
    def grab_more_children(self, hide_loader: bool = False) -> list[dict]:
        item_resps = self.fetch_items(hide_loader=hide_loader)
        # assumes all items inside objs are the same class
        self._main_items.extend(self.parse_linked_objs(item_resps, self._contains))
    
    def create_pbar(self, pbar_stack: list | None = None) -> tuple[list[DLContent | Container], list]:
        pos, pbar_stack = Printer.pbar_position_handler(7, pbar_stack)
        unit = "Content" if isinstance(self._contains, tuple) else self._contains.__name__
        if isinstance(self, Query) and not isinstance(self, UserItem): # avoid overwriting UserItem._contains
            unit = "Content" if Zotify.CONFIG.get_optimized_dl_order() else "URL"
        pbar: list[Content] = Printer.pbar(self._main_items, self.name, pos=pos,
                                           unit=unit, disable=not self._show_pbar)
        pbar_stack.append(pbar)
        return pbar, pbar_stack
    
    def check_skippable(self, check_parent: bool = True) -> bool:
        self.skippable = self.parent.check_skippable() if self.parent and check_parent else False
        return self.skippable
    
    def download(self, pbar_stack: list | None):
        "Downloads whatever list[DLContent | Container] self._main_items points to"
        if self.downloaded:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" ({self.clsn.upper()} ALREADY DOWNLOADED THIS SESSION)')
            return
        
        pbar, pbar_stack = self.create_pbar(pbar_stack)
        for child in pbar:
            child.download(pbar_stack)
            Printer.refresh_all_pbars(pbar_stack)
            if isinstance(child, DLContent):
                wait_between_downloads(child.check_skippable())
        self.mark_downloaded() # technically should have no effect, as last child marks parent downloaded


class Playlist(Container):
    _show_pbar = Zotify.CONFIG.get_show_playlist_pbar()
    _contains = (Track, Episode)
    _preloaded = 100
    _fetch_q = 100
    url = PLAYLIST_URL
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self.collaborative = False
        self.desc = ""
        self.image_url = ""
        self.owner = ""
        self.public = False
        self.snapshot_id = ""
        self.tracks_or_eps: list[Track | Episode] = self._main_items
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(suppress_id=suppress_id)
    
    def parse_metadata(self, playlist_resp: dict[str, str | bool]):
        self.update_id(playlist_resp[ID])
        self.name: str = playlist_resp[NAME]
        self.collaborative: bool = playlist_resp[COLLABORATIVE]
        self.desc: str = playlist_resp[DESCRIPTION]
        largest_image = max(playlist_resp[IMAGES], key=lambda img: img[WIDTH], default={URL: ""})
        self.image_url: str = largest_image[URL]
        self.public: bool = playlist_resp[PUBLIC]
        self.snapshot_id: str = playlist_resp[SNAPSHOT_ID]
        
        self.owner: User = self.parse_linked_objs([playlist_resp[OWNER]], User)[0]
        self.printing_label = fix_filename(self.owner.name) + ' - ' + fix_filename(self.name)
        
        if TRACKS in playlist_resp and ITEMS in playlist_resp[TRACKS]:
            items = [item for item in playlist_resp[TRACKS][ITEMS] if item[TRACK] is not None]
            tracks_or_eps: list[dict] = [item[TRACK] for item in items]
            for track_or_ep, item in zip(tracks_or_eps, items):
                track_or_ep[ADDED_AT] = item[ADDED_AT]
                track_or_ep[ADDED_BY] = item[ADDED_BY]
                track_or_ep[IS_LOCAL] = item[IS_LOCAL]
            self.tracks_or_eps = self.parse_linked_objs(tracks_or_eps, (Track, Episode)) # possible underflow if len(items) > 100
            # self.tracks_or_eps.sort(key=lambda s: strptime_utc(s[ADDED_AT]))
        self.needs_expansion = NEXT not in playlist_resp[TRACKS] or playlist_resp[TRACKS][NEXT] is not None
        
        self.hasMetadata = True
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        playlist_items = super().fetch_items(TRACKS, "additional_types=track%2Cepisode", hide_loader)
        for item in playlist_items:
            item[TRACK][ADDED_AT] = item[ADDED_AT]
            item[TRACK][ADDED_BY] = item[ADDED_BY]
            item[TRACK][IS_LOCAL] = item[IS_LOCAL]
        track_or_episode_resps = [item[TRACK] if item[TRACK] is not None and item[TRACK][URI] else None for item in playlist_items]
        # playlist_items.sort(key=lambda s: strptime_utc(s[ADDED_AT]))
        return track_or_episode_resps


class Album(Container):
    _regex_flag = Zotify.CONFIG.get_regex_album()
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _contains = Track
    _preloaded = 50
    url = ALBUM_URL
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self.compilation = 0
        self.duration_ms = 0
        self.image_url = ""
        self.label = ""
        self.release_date = ""
        self.total_discs = ""
        self.total_tracks = ""
        self.type = ""
        self.artists: list[Artist] = []
        self.tracks: list[Track] = self._main_items
        
        # only set by Artist Albums API
        self.album_group = ""
        # only set by UserItem API
        self.added_at = ""
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["total_tracks", "artists"], suppress_id=suppress_id, force_clsn=True)
    
    def parse_metadata(self, album_resp: dict[str, str | bool]):
        self.update_id(album_resp[ID])
        self.name: str = album_resp[NAME]
        
        # Local File == None or Key May Not Exist
        if album_resp.get(ALBUM_TYPE): self.type: str = album_resp[ALBUM_TYPE]
        if album_resp.get(RELEASE_DATE): self.release_date: str = album_resp[RELEASE_DATE]
        # Local File == Key May Not Exist
        if album_resp.get(TOTAL_TRACKS): self.total_tracks = str(album_resp[TOTAL_TRACKS]).zfill(2)
        
        # Local File == Key May Not Exist
        if album_resp.get(IMAGES):
            largest_image = max(album_resp[IMAGES], key=lambda img: img.get(WIDTH, 0), default={URL: ""})
            self.image_url: str = largest_image.get(URL, "")
        else:
            self.image_url: str = ""
        self.compilation: int = 1 if COMPILATION == self.type else 0
        self.year: str = self.release_date.split('-')[0]
        
        if ARTISTS in album_resp and album_resp[ARTISTS]:
            self.artists = self.parse_linked_objs(album_resp[ARTISTS], Artist)
            self.printing_label = fix_filename(self.artists[0].name) + ' - ' + fix_filename(self.name)
        
        if TRACKS in album_resp and album_resp[TRACKS]:
            self.label: str = album_resp[LABEL]
            self.tracks = self.parse_linked_objs(album_resp[TRACKS][ITEMS], Track) # possible underflow if len(items) > 100
            self.needs_expansion = album_resp[TRACKS][NEXT] is not None
            if not self.needs_expansion:
                # set in self.grab_more_children() if album incomplete
                self.total_discs = str(album_resp[TRACKS][ITEMS][-1][DISC_NUMBER])
                self.duration_ms = sum((int(t.duration_ms) for t in self.tracks))
            self.hasMetadata = True
        elif isinstance(self.parent, Artist):
            self.album_group = album_resp[ALBUM_GROUP]
            self.needs_expansion = True
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        return super().fetch_items(TRACKS, hide_loader=hide_loader)
    
    def grab_more_children(self, hide_loader: bool = False) -> list[dict]:
        super().grab_more_children(hide_loader=hide_loader)
        self.needs_expansion = False
        self.total_discs = str(self.tracks[-1].disc_number)
        self.duration_ms = sum((int(t.duration_ms) for t in self.tracks))
    
    def check_skippable(self) -> bool:
        if super().check_skippable(check_parent=False):
            return self.skippable
        
        if isinstance(self.parent, Artist) and self.album_group:
            if Zotify.CONFIG.get_skip_comp_albums() and self.album_group == COMPILATION:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (ARTIST ONLY COMPILED INTO ALBUM)')
                self.skippable = True
            elif Zotify.CONFIG.get_skip_appears_on_album() and self.album_group == APPEARS_ON:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (ARTIST ONLY APPEARS ON ALBUM)')
                self.skippable = True
            elif Zotify.CONFIG.get_discog_by_album_artist() and self.artists[0].name == self.parent.name:
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (ARTIST NOT ALBUM ARTIST)')
                self.skippable = True
        
        if self.skippable:
            return self.skippable
        
        if Zotify.CONFIG.get_skip_comp_albums() and self.compilation:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (COMPILATION ALBUM)')
            self.skippable = True
        elif Zotify.CONFIG.get_skip_various_artists() and "".join(self.artists[0].name.lower().split()) == "variousartists":
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (ALBUM OF VARIOUS ARTISTS)')
            self.skippable = True
        
        if self.skippable:
            return self.skippable
        
        return self.skippable if isinstance(self.parent, Track) else self.parent.check_skippable()
    
    def download(self, pbar_stack):
        if Zotify.CONFIG.get_optimized_dl_order():
            # only called when get_download_parent_album() since get_optimized_dl() typically bypasses Containers
            tracks = self.tracks
            downloadables = [c for c in self.tracks if c.id and not c.check_skippable()]
            downloadables.sort(key=lambda x: x.duration_ms); edge_zip(downloadables)
            self.tracks = downloadables
        
        super().download(pbar_stack)
        
        if Zotify.CONFIG.get_optimized_dl_order():
            self.tracks = tracks


class Artist(Container):
    _show_pbar = Zotify.CONFIG.get_show_artist_pbar()
    _toptrackmode: bool = False # Zotify.get_artist_fetch_top_tracks()
    _contains = Track if _toptrackmode else Album
    _fetch_q = 20 if not _toptrackmode else 100
    url = ARTIST_URL
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self.needs_recursion = not self._toptrackmode
        
        self.total_followers = 0
        self.albums: list[Album] = self._main_items if not self._toptrackmode else []
        self.genres: list[str] = []
        self.top_songs: list[Track] = self._main_items if self._toptrackmode else []
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["genres"], suppress_id=suppress_id)
    
    def parse_metadata(self, artist_resp: dict[str, str | int | list[str]]):
        self.update_id(artist_resp[ID])
        self.name: str = artist_resp[NAME]
        
        if GENRES in artist_resp:
            self.total_followers: int = artist_resp[FOLLOWERS][TOTAL]
            self.genres: list[str] = artist_resp[GENRES]
            self.printing_label = fix_filename(f"{self.genres} {self.total_followers}") + ' - ' + fix_filename(self.name)
            self.hasMetadata = True
        
        self.needs_expansion = True
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        if self._toptrackmode:
            with Loader(f"Fetching {self.clsn.lower()} top tracks...", disabled=hide_loader):
                _, resp = Zotify.invoke_url(f'{self.url}/{self.id}/top-tracks?{MARKET_APPEND}')
                artist_items = resp[TRACKS]
        else:
            artist_items = super().fetch_items(ALBUMS, hide_loader=hide_loader)
        return artist_items


class Show(Container):
    _path_root: PurePath = Zotify.CONFIG.get_root_podcast_path()
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _contains = Episode
    _preloaded = 50
    url = SHOW_URL
    
    def __init__(self, id_or_uri: str, parent: Content | Container = None):
        super().__init__(id_or_uri, parent)
        self.desc = ""
        self.explicit = False
        self.external = False
        self.image_url = ""
        self.publisher = ""
        self.total_episodes = ""
        self.episodes: list[Episode] = self._main_items
    
    def dashboard(self, suppress_id: bool = False) -> str:
        return super().dashboard(["total_episodes",], suppress_id=suppress_id)
    
    def parse_metadata(self, show_resp: dict[str, str | bool]):
        self.update_id(show_resp[ID])
        self.name: str = show_resp[NAME]
        self.desc: str = show_resp[DESCRIPTION]
        self.explicit: bool = show_resp[EXPLICIT]
        self.external: bool = show_resp[IS_EXTERNALLY_HOSTED]
        largest_image = max(show_resp[IMAGES], key=lambda img: img[WIDTH], default={URL: ""})
        self.image_url: str = largest_image[URL]
        self.publisher: str = show_resp[PUBLISHER]
        self.total_episodes = str(show_resp[TOTAL_EPISODES]).zfill(2)
        self.printing_label = fix_filename(self.publisher) + ' - ' + fix_filename(self.name)
        
        if EPISODES in show_resp:
            self.episodes = self.parse_linked_objs(show_resp[EPISODES][ITEMS], Episode)
            self.needs_expansion = show_resp[EPISODES][NEXT] is not None
        else:
            self.needs_expansion = True
        
        self.hasMetadata = True
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        return super().fetch_items(EPISODES, hide_loader=hide_loader)


# start not implemented
class Chapter(DLContent):
    url = CHAPTER_URL
    
    def __init__(self, id_or_uri: str, parent: Content = None):
        super().__init__(id_or_uri, parent)


class Audiobook(Container):
    _show_pbar = Zotify.CONFIG.get_show_album_pbar()
    _contains = Chapter
    _preloaded = 50
    url = AUDIOBOOK_URL
    
    def __init__(self, id_or_uri: str, parent: Content = None):
        super().__init__(id_or_uri, parent)
        self.chapters: list[Audiobook] = self._main_items
# end not implemented


ITEM_FETCH: dict[Type[DLContent] | Type[Container], int] = {
    Playlist:   0,
    Artist:    50,
    Album:     20,
    Audiobook: 50,
    Show:      50,
    Chapter:   50,
    Episode:   50,
    Track:    100
}
ITEM_NAMES = tuple(cls.__name__.lower() for cls in ITEM_FETCH)


class Query(Container):
    _show_pbar = Zotify.CONFIG.get_show_url_pbar()
    
    def __init__(self, timestamp: str):
        Tree.query = self
        super().__init__(timestamp, None)
        self.name = "Total Progress"
        self.pbar_stack: list = []
        
        self.requested_urls = "" # for debug only
        self.parsed_request: list[list[str]] = []
        self.requested_objs: list[list[DLContent | Container]] = self._main_items
        self.downloadables: set[DLContent | Container] | list[DLContent | Container] = []
    
    def request(self, requested_urls: str) -> Query:
        self.requested_urls = requested_urls # only used here, can remove later
        self.parsed_request = bulk_regex_urls(self.requested_urls)
        n_urls = len(set.union(*[set(l) for l in self.parsed_request]))
        Printer.debug(f'Request Parsed as {n_urls} URL' + ("s" if n_urls > 1 else ""))
        return self
    
    def create_linked_obj(self, cls: Content | Container, id_or_uri: str) -> Content | Container:
        return self.tree.createChild(cls(id_or_uri, self))
    
    def create_direct_objs(self, clss: dict[Type[DLContent] | Type[Container], int] = ITEM_FETCH) -> list[list[DLContent | Container]]:
        direct_reqs_objs = []
        for cls, id_list in zip(clss, self.parsed_request):
            objs: list[Content | Container] = [None]*len(id_list)
            for i, id in enumerate(id_list):
                objs[i] = self.create_linked_obj(cls, id)
            direct_reqs_objs.append(objs)
        return direct_reqs_objs
    
    def fetch_direct_metadata(self, direct_reqs_objs: list[list[DLContent | Container]],
                              qs: tuple[int] = ITEM_FETCH.values()) -> tuple[list[list[DLContent | Container]], list[list[dict]]]:
        direct_req_item_resps = []
        for objs, q in zip(direct_reqs_objs, qs):
            if not objs:
                direct_req_item_resps.append([])
                continue
            elif isinstance(objs[0], Playlist) or len(objs) == 1:
                item_resps = [obj.fetch_metadata() for obj in objs]
            else:
                with Loader(f"Fetching bulk {objs[0].clsn.lower()} information..."):
                    url = f"{objs[0].url}?{MARKET_APPEND}&{BULK_APPEND}"
                    item_resps = Zotify.invoke_url_bulk(url, [obj.id for obj in objs], objs[0].lowers, q)
            direct_req_item_resps.append(item_resps)
        return direct_reqs_objs, direct_req_item_resps
    
    def parse_direct_metadata(self, direct_reqs_objs: list[list[DLContent | Container]], direct_req_item_resps: list[list[dict]]) -> None:
        """ This sets self.requested_objs (Query's name for self._main_items) """
        for objs, item_resps in zip(direct_reqs_objs, direct_req_item_resps):
            if not objs:
                self.requested_objs.append([])
                continue
            
            with Loader(f"Parsing {objs[0].clsn.lower()} information..."):
                for obj, item_resp in zip(objs, item_resps):
                    obj.parse_metadata(item_resp)
                    if isinstance(obj, Container) and obj.needs_expansion:
                        obj.grab_more_children(hide_loader=True)
            
            while any({isinstance(obj, Container) and obj.needs_recursion for obj in objs}):
                recurs = [o for o in objs if isinstance(o, Container) and o.needs_recursion]
                children: list[Container] = []
                for r in recurs: 
                    children.extend(r._main_items)
                # assumes all Containers inside objs are the same class
                url = f"{children[0].url}?{MARKET_APPEND}&{BULK_APPEND}"
                item_resps = Zotify.invoke_url_bulk(url, [c.id for c in children], children[0].lowers, ITEM_FETCH[children[0].__class__])
                for child, resp in zip(children, item_resps):
                    child.parse_metadata(resp)
                    if isinstance(child, Container) and child.needs_expansion:
                        child.grab_more_children()
                objs = children
            
            self.requested_objs.append(objs) # basic metadata complete objs
    
    def fetch_extra_metadata(self):
        alltracks = {t for t in self.tree.subContent if isinstance(t, Track) and t.id}
        
        if Zotify.CONFIG.get_save_genres():
            artists = set.union(set(), *(set(track.artists) for track in alltracks))
            artist_ids: dict[str, Artist] = {artist.id: artist for artist in artists if artist.id and not artist.hasMetadata}
            if not artist_ids:
                return
            
            with Loader(f"Fetching bulk genre information..."):
                url = f"{ARTIST_URL}?{MARKET_APPEND}&{BULK_APPEND}"
                artist_resps = Zotify.invoke_url_bulk(url, list(artist_ids.keys()), ARTISTS, ITEM_FETCH[Artist])
                for artist_resp in artist_resps:
                    artist = artist_ids[artist_resp[ID]]
                    artist.parse_metadata(artist_resp); artist.needs_expansion = False
                    for sib in artist.tree.siblings:
                        if not sib.hasMetadata:
                            sib.parse_metadata(artist_resp); sib.needs_expansion = False
            for track in alltracks:
                genres: list[str] = [*set.union(*[set(artist.genres) for artist in track.artists])]
                genres.sort()
                track.genres = genres
        
        if Zotify.CONFIG.get_disc_track_totals() or Zotify.CONFIG.get_download_parent_album():
            albums = {track.album for track in alltracks}
            album_ids: dict[str, Album] = {album.id: album for album in albums if album.id and not album.hasMetadata}
            if not album_ids:
                return
            
            loader_text = "parent album" if Zotify.CONFIG.get_download_parent_album() else "track/disc total"
            with Loader(f"Fetching bulk {loader_text} information..."):
                url = f"{ALBUM_URL}?{MARKET_APPEND}&{BULK_APPEND}"
                album_resps = Zotify.invoke_url_bulk(url, list(album_ids.keys()), ALBUMS, ITEM_FETCH[Album])
                for album_resp in album_resps:
                    a = album_ids[album_resp[ID]]
                    a.parse_metadata(album_resp)
                    if a.needs_expansion: 
                        a.grab_more_children(hide_loader=True)
    
    def get_m3u8_dir(self, content_list: list[DLContent], force_common_dir: bool = False) -> PurePath | None:
        m3u8_dir = Zotify.CONFIG.get_m3u8_location()
        if m3u8_dir and not force_common_dir:
            return m3u8_dir
        
        allpaths = {dlc.filepath for dlc in content_list if dlc.filepath and dlc.filepath.is_relative_to(dlc._path_root)}
        if allpaths:
            return get_common_dir(allpaths)
    
    def create_m3u8_playlists(self, force_path: PurePath | None = None, force_name: str = "", append: list[str] | None = None) -> None:        
        for obj_list in self.requested_objs:
            if not obj_list:
                continue
            
            if isinstance(obj_list[0], Container):
                content_lists: list[list[DLContent]] = [obj.recurse_DLC() for obj in obj_list]
                def get_m3u8_filename(content_list: list[DLContent]) -> str:
                    return fix_filename(content_list[0].parent.name)
            else:
                # zip will only have one iteration
                content_lists: list[list[DLContent]] = [obj_list]
                def get_m3u8_filename(content_list: list[DLContent]) -> str:
                    return fix_filename(f"{self.id}_{content_list[0].lowers}")
            if force_name:
                def get_m3u8_filename(content_list: list[DLContent]) -> str:
                    return fix_filename(force_name)
            
            for obj, content_list in zip(obj_list, content_lists):
                name = f'"{obj.name}"' if isinstance(obj, Container) else obj.uppers
                
                if not content_list:
                    # only possible for childless Container 
                    Printer.hashtaged(PrintChannel.WARNING, f'SKIPPING M3U8 CREATION FOR {name}\n' +
                                                            f"{obj.clsn.upper()} CONTAINS NO CONTENT")
                    continue
                
                # content_list Paths -> already fix_filename/fix_filepath -> m3u8_dirs safe
                m3u8_dir = self.get_m3u8_dir(content_list)
                m3u8_filename = get_m3u8_filename(content_list)
                if m3u8_dir is None:
                    Printer.hashtaged(PrintChannel.WARNING, f'SKIPPING M3U8 CREATION FOR "{m3u8_filename}"\n' +
                                                             "NO CONTENT WITH VALID FILEPATHS FOUND")
                    continue
                
                m3u8_path = m3u8_dir / (m3u8_filename + ".m3u8")
                m3u8_path = fix_filepath(force_path, content_list[0]._path_root) if force_path else m3u8_path
                Path(m3u8_path).unlink(missing_ok=True)
                add_to_m3u8(m3u8_path, content_list, append)
                Printer.hashtaged(PrintChannel.MANDATORY, f'M3U8 CREATED FOR {name}\n' +
                                                          f"SAVED TO: {content_list[0].rel_path(m3u8_path)}")
    
    def download(self):
        if Zotify.CONFIG.get_optimized_dl_order():
            self.downloadables = {c for c in self.tree.subContent if isinstance(c, DLContent) and c.id}
            
            if Zotify.CONFIG.get_download_parent_album():
                tracks = {t for t in self.downloadables if isinstance(t, Track) and t.album is not None}
                albums = {t.album for t in tracks}
                for album in albums:
                    if not album.duration_ms: # do not overwrite fully-fetched albums' true duration
                        album.duration_ms = int(album.total_tracks) * 195000 # assumes 3:15 average track duration
                self.downloadables = {i for i in self.downloadables if not i in tracks}.union(albums)
            
            # self.check_skippable()
            skipped = {d for d in self.downloadables if d.check_skippable()}
            self.downloadables = [d for d in self.downloadables if d not in skipped]
            if not self.downloadables: self.skippable = True
            
            if Zotify.CONFIG.get_always_check_lyrics() and False:
                # this will probably spam API calls
                def get_lyr(item: DLContent | Container):
                    if isinstance(item, Track):
                        item.fetch_lyrics() 
                    else:
                        for i in item._main_items:
                            get_lyr(i)
                for d in skipped:
                    get_lyr(d)
            
            def sort_by_dur(item: DLContent | Album) -> int:
                return item.duration_ms
            self.downloadables.sort(key=sort_by_dur)
            edge_zip(self.downloadables)
        else:
            for cats in self.requested_objs:
                self.downloadables.extend(cats)
        
        if Zotify.CONFIG.get_standard_interface():
            Interface.refresh()
        
        interrupt = None
        self._main_items = self.downloadables
        try:
            super().download(pbar_stack=None)
        except BaseException as e:
            interrupt = e
        
        while Printer.ACTIVE_LOADER: # catch and close any/all active loaders
            Printer.ACTIVE_LOADER.stop()
        n_pbars = len(Printer.ACTIVE_PBARS)
        while Printer.ACTIVE_PBARS: # catch and close any/all active pbars
            Printer.ACTIVE_PBARS.pop().close()
        if Zotify.CONFIG.get_show_any_progress() and n_pbars: # closing any visible pbars will print an extra newline
            Printer.back_up()
        
        if isinstance(interrupt, KeyboardInterrupt):
            Printer.hashtaged(PrintChannel.MANDATORY, "USER CANCELED DOWNLOADS EARLY\n"+
                                                      "ATTEMPTING TO CLEAN UP")
        elif interrupt is not None:
            Printer.hashtaged(PrintChannel.ERROR, "UNEXPECTED ERROR DURING DOWNLOADS\n"+
                                                  "ATTEMPTING TO CLEAN UP")
        
        if Zotify.CONFIG.get_export_m3u8() and self.requested_objs:
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
    
    def execute(self):
        direct_reqs_objs = self.create_direct_objs()
        if not Zotify.CONFIG.get_bypass_metadata():
            self.parse_direct_metadata(*self.fetch_direct_metadata(direct_reqs_objs))
            self.fetch_extra_metadata()
        self.download()


class VerifyLibrary(Query):
    _contains = Track
    url = f"{TRACK_URL}?{MARKET_APPEND}&{BULK_APPEND}"
    
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self.name = "Verifiable Tracks"
    
    def create_fetch_verifiable_tracks(self) -> tuple[list[list[Track]], list[list[dict]]]:
        # ONLY WORKS WITH ARCHIVED TRACKS (THEORETICALLY GUARANTEES BULK_URL TO WORK)
        archived_tracks = get_archived_entries()
        archived_ids = [entry.strip().split('\t')[0] for entry in archived_tracks]
        archived_filenames_or_paths = [PurePath(entry.strip().split('\t')[4]).stem for entry in archived_tracks]
        
        verifiable_tracks: list[Track] = []
        library = walk_directory_for_tracks(Zotify.CONFIG.get_root_path())
        for entry in library:
            if entry.stem in archived_filenames_or_paths:
                track: Track = self.create_linked_obj(Track, archived_ids[archived_filenames_or_paths.index(entry.stem)])
                track.filepath = PurePath(entry)
                verifiable_tracks.append(track)
        
        track_resps = Zotify.invoke_url_bulk(self.url, [t.id for t in verifiable_tracks], TRACKS)
        
        return [verifiable_tracks], [track_resps]
    
    def execute(self):
        self.parse_direct_metadata(*self.create_fetch_verifiable_tracks())
        self.fetch_extra_metadata()
        self.requested_objs = self.requested_objs[0] # remove outer layer used in Querey
        pbar: list[Track]
        pbar, pbar_stack = self.create_pbar()
        for child in pbar:
            child.verify_metadata()
            Printer.refresh_all_pbars(pbar_stack)


class UserItem(Query):
    _contains = User
    interactive = True
    inner_stripper = None
    outer_stripper = None
    url = USER_URL
    
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self.name = self.clsn + "s"
    
    def fetch_user_items(self) -> list[None | dict]:
        with Loader(f"Fetching {self.name}...", disabled=self.interactive):
            user_item_resps = Zotify.invoke_url_nextable(f"{self.url}?{MARKET_APPEND}", stripper=self.outer_stripper)
        return user_item_resps
    
    def display_select_user_items(self, user_item_resps: list[None | dict]) -> list[dict]:
        display_list = [[i+1, str(resp.get(self.inner_stripper, resp)[NAME])] for i, resp in enumerate(user_item_resps)]
        Printer.table(self.uppers, ('ID', 'Name'), [[0, f"ALL {self.uppers}"]] + display_list)
        selected_item_resps: list[None | dict] = select([None] + user_item_resps, first_ID=0)
        
        if selected_item_resps[0] == None:
            # option 0 == get all choices
            selected_item_resps = user_item_resps[1:]
        return selected_item_resps
    
    def create_user_items(self, user_item_resps: list[dict]) -> list[list[Content]]:
        self.parsed_request = [[resp.get(self.inner_stripper, resp)[URI] for resp in user_item_resps]]
        return self.create_direct_objs((self._contains,))
    
    def execute(self):
        user_item_resps = self.fetch_user_items()
        if self.interactive:
            user_item_resps = self.display_select_user_items(user_item_resps)
        
        wrapped_objs = self.create_user_items(user_item_resps)
        if self.inner_stripper and self._contains in {Track, Album}:
            added_dates = [resp[ADDED_AT] for resp in user_item_resps]
            for obj, added_date in zip(wrapped_objs[0], added_dates):
                obj: Track | Album
                obj.added_at = added_date
            user_item_resps = [resp[self.inner_stripper] for resp in user_item_resps]
        self.parse_direct_metadata(wrapped_objs, [user_item_resps])
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
        m3u8_dir = self._path_root
        if archive_mode:
            for part in PurePath(Zotify.CONFIG.get_output(self.clsn)).parts:
                if "{" in part or "}" in part: break
                m3u8_dir = m3u8_dir / part
        if m3u8_dir == self._path_root:
            m3u8_dir = self.get_m3u8_dir(liked_tracks, force_common_dir=archive_mode)
        m3u8_path = m3u8_dir / f"{self.name}.m3u8"
        
        def find_sync_point(liked_tracks: list[Track], m3u8_entry_path: str) -> int | None:
            for i, liked_track in enumerate(liked_tracks):
                Printer.logger(f"{liked_track.filepath} == {m3u8_entry_path}")
                if str(liked_track.filepath) == m3u8_entry_path:
                    return i
                elif str(liked_track.filepath) in m3u8_entry_path:
                    Printer.hashtaged(PrintChannel.WARNING, "TRACK FILEPATH WITHIN LIKED SONG M3U8 ENTRY\n" +
                                                            "M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n" +
                                                            "POSSIBLY FROM NON-UPDATED SONG ARCHIVE FILE\n" +
                                                            "(CONSIDER RUNNING '--update-archive')")
                    return i
                elif m3u8_entry_path in str(liked_track.filepath):
                    Printer.hashtaged(PrintChannel.WARNING, "LIKED SONG M3U8 ENTRY WITHIN TRACK FILEPATH\n" +
                                                            "M3U8 MAY NOT PLAY/LINK TO FILES CORRECTLY\n" +
                                                            "POSSIBLY FROM M3U8 USING RELATIVE PATHS\n" +
                                                            "(CONSIDER USING FULL PATHS FOR LIKED SONGS M3U8)")
                    return i
            return None
        
        def handle_archive_mode() -> None | list[str]:
            if not archive_mode or not Path(m3u8_path).exists():
                return
            
            raw_liked_archive = fetch_m3u8_songs(m3u8_path)
            if not raw_liked_archive:
                Printer.hashtaged(PrintChannel.WARNING, "FAILED Liked Songs ARCHIVE M3U8 UPDATE\n" +
                                                        "FAILED TO READ EXISTING M3U8\n" +
                                                        "FALLING BACK TO STANDARD M3U8 CREATION")
                return
            
            for i, liked_archive_path in enumerate(raw_liked_archive[1::3]):
                sync_point = find_sync_point(liked_tracks, liked_archive_path[:-1])
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
