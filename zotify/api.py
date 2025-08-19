from __future__ import annotations

import ffmpy
import functools
import music_tag
import requests
import shutil
import subprocess
import uuid
from librespot.metadata import TrackId, EpisodeId
from tqdm.auto import tqdm

from zotify import __version__
from zotify.const import *
from zotify.utils import *


def fetch_search_display(search_term: str) -> list[str]:
    params = {LIMIT: '10',
              OFFSET: '0',
              'q': search_term,
              TYPE: 'track,album,artist,playlist'}
    
    # Parse args
    splits = search_term.split()
    for split in splits:
        index = splits.index(split)
        
        if split[0] == '-' and len(split) > 1:
            if len(splits)-1 == index:
                raise IndexError(f'No parameters passed after option: {split}')
        
        if split == '-l' or split == '-limit':
            try:
                int(splits[index+1])
            except ValueError:
                raise ValueError(f'Parameter passed after {split} option must be an integer')
            if int(splits[index+1]) > 50:
                raise ValueError('Invalid limit passed. Max is 50')
            params['limit'] = splits[index+1]
        
        if split == '-t' or split == '-type':
            allowed_types = ['track', 'playlist', 'album', 'artist']
            passed_types = []
            for i in range(index+1, len(splits)):
                if splits[i][0] == '-':
                    break
                if splits[i] not in allowed_types:
                    raise ValueError(f'Parameters passed after {split} option must be from this list:\n' +\
                                     f'{'\n'.join(allowed_types)}')
                passed_types.append(splits[i])
            params[TYPE] = ','.join(passed_types)
    
    if len(params[TYPE]) == 0:
        params[TYPE] = 'track,album,artist,playlist'
    
    # Clean search term
    search_term_list = []
    for split in splits:
        if split[0] == "-":
            break
        search_term_list.append(split)
    if not search_term_list:
        raise ValueError("Invalid query")
    params["q"] = ' '.join(search_term_list)
    
    resp = Zotify.invoke_url_with_params(SEARCH_URL, **params)
    search_result_uris = []
    counter = 1
    
    if TRACK in params[TYPE].split(',') and len(resp[TRACKS][ITEMS]):
        track_resps: list[dict] = resp[TRACKS][ITEMS]
        track_data = [ [track_resps.index(t) + counter,
                        str(t[NAME]) + (" [E]" if t[EXPLICIT] else ""),
                        ','.join([artist[NAME] for artist in t[ARTISTS]])   ] for t in track_resps]
        search_result_uris.extend([t[URI] for t in track_resps])
        counter += len(track_resps)
        Printer.table("Tracks", ('ID', 'Name', 'Artists'), track_data)
    
    if ALBUM in params[TYPE].split(',') and len(resp[ALBUMS][ITEMS]):
        album_resps: list[dict] = resp[ALBUMS][ITEMS]
        album_data = [ [album_resps.index(a) + counter,
                        str(a[NAME]),
                        ','.join([artist[NAME] for artist in a[ARTISTS]])   ] for a in album_resps]
        search_result_uris.extend([a[URI] for a in album_resps])
        counter += len(album_resps)
        Printer.table("Albums", ('ID', 'Name', 'Artists'), album_data)
    
    if ARTIST in params[TYPE].split(',') and len(resp[ARTISTS][ITEMS]):
        artist_resps: list[dict] = resp[ARTISTS][ITEMS]
        artist_data = [ [artist_resps.index(a) + counter,
                         str(a[NAME])                                       ] for a in artist_resps]
        search_result_uris.extend([a[URI] for a in artist_resps])
        counter += len(artist_resps)
        Printer.table("Artists", ('ID', 'Name'), artist_data)
    
    if PLAYLIST in params[TYPE].split(',') and len(resp[PLAYLISTS][ITEMS]):
        playlist_resps: list[dict] = resp[PLAYLISTS][ITEMS]
        playlist_data = [ [playlist_resps.index(p) + counter,
                           str(p[NAME]),
                           str(p[OWNER][DISPLAY_NAME])                      ] for p in playlist_resps]
        search_result_uris.extend([p[URI] for p in playlist_resps])
        counter += len(playlist_resps)
        Printer.table("Playlists", ('ID', 'Name', 'Owner'), playlist_data)
    
    return search_result_uris


class Content():
    def __mod__(self, other) -> bool:
        # used for evaluating api track equality
        if isinstance(other, Content):
            return self.uri == other.uri
        return False
    
    def __eq__(self, other) -> bool:
        # used for evaluating track object equality
        if isinstance(other, Content):
            return self.uri == other.uri and self._parent.uri == other._parent.uri
        return False
    
    def __hash__(self):
        return hash((self.uri, self._parent.uri if self._parent else None))
    
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        self._clsn = self.__class__.__name__
        self._plural = self._clsn.lower() + "s"
        self._regex_flag: re.Pattern | None = None
        if ":" in id_or_uri:
            self.uri = id_or_uri.split(":", 1)[-1]
            self.id = self.uri.split(":", 1)[-1] #local file URIs will have more than 2 commas
            if self.id.count(":"):
                self.id = None
        else:
            self.id = id_or_uri
            self.uri = f"{self._clsn.lower()}:{self.id}"
        self._parent: Content | Container = _parent
        self._children: set[Content | Container] = set()
        self._siblings: set[Content | Container] = set()
        self._accepting_children = True
        self.downloaded = False # self / all child DLContent must have valid Path if True
        self.hasMetadata = False
        self.url = ""
        self.name = ""
    
    @property
    def subContent(self) -> set[Content]:
        childContent = {child for child in self._children if not isinstance(child, Container)}
        childContainers = {child for child in self._children if isinstance(child, Container)}
        return childContent.union(*(child.subContent for child in childContainers))
    
    @property
    def _allContent(self) -> set[Container]:
        def allSubContent(cont: Content) -> set[Container]:
            childContent = {child for child in cont._children if not isinstance(child, Container)}
            return childContent.union(*(allSubContent(child) for child in cont._children))
        return self._parent._allContent if self._parent else allSubContent(self)
    
    @property
    def _allContainers(self) -> set[Container]:
        def allSubContainers(cont: Content) -> set[Container]:
            childContainers = {child for child in cont._children if isinstance(child, Container)}
            return childContainers.union(*(allSubContainers(child) for child in cont._children))
        return self._parent._allContainers if self._parent else allSubContainers(self)
    
    def add_children(self, obj_or_objs: Content | Container | list[Content | Container]):
        if isinstance(obj_or_objs, (tuple, list, set)):
            self._children.update(obj_or_objs)
        else:
            self._children.add(obj_or_objs)
    
    def findChild(self, obj: Content | Container) -> Content | Container:
        """Returns matching obj if found, else passed obj after adopting"""
        
        # same track, same container
        allcont = self._allContainers if isinstance(obj, Container) else self._allContent
        if obj in allcont:
            return {cont for cont in allcont if obj == cont}.pop()
        
        if not self._accepting_children:
            return obj
        
        # same track, different container
        obj._siblings = {cont for cont in allcont if obj % cont}
        for sib in obj._siblings:
            sib._siblings.add(obj)
        
        self.add_children(obj)
        return obj
    
    def regex_check(self) -> bool:
        if self._regex_flag is None:
            return False
        regex_match = self._regex_flag.search(self.name)
        Printer.debug("Regex Check\n" +\
                     f"Pattern: {self._regex_flag.pattern}\n" +\
                     f"{self._clsn} Name: {self.name}\n" +\
                     f"Match Object: {regex_match}")
        if regex_match:
            Printer.hashtaged(PrintChannel.SKIPPING, f'{self._clsn.upper()} MATCHES REGEX FILTER\n' +\
                                                     f'{self._clsn}_Name: {self.name} - {self._clsn}_ID: {self.id}' +\
                                                    (f'\nRegex Groups: {regex_match.groupdict()}' if regex_match.groups() else ""))
        return regex_match
    
    def fetch_metadata(self) -> dict[str]:
        with Loader(PrintChannel.PROGRESS_INFO, f"Fetching {self._clsn.lower()} information..."):
            (raw, info) = Zotify.invoke_url(f'{self.url}/{self.id}?{MARKET_APPEND}')
        if info:
            return info
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
            obj = self.findChild(rawobj)
            if not obj.hasMetadata: # overly cautious
                obj.parse_metadata(resp) # theoretically shouldn't lose metadata by re-parsing if obj was parsed prev
            objs.append(obj)
        return objs
    
    # placeholder func, overwrite in each child class
    def download(self, pbar_stack: list):
        pass
    
    def mark_downloaded(self): 
        # Best practice / convention is to only call this on self
        self.downloaded = True
        
        # copy downloaded file to all siblings' paths
        for sib in self._siblings:
            if sib.downloaded:
                continue
            if isinstance(self, DLContent) and isinstance(sib, DLContent):
                sib.filepath = check_path_dupes(sib.fill_output_template())
                Path(sib.filepath.parent).mkdir(parents=True, exist_ok=True)
                shutil.copyfile(self.filepath, sib.filepath)
                if isinstance(sib, Track):
                    sib.set_audio_tags(sib.filepath)
                    sib.set_music_thumbnail(sib.filepath)
            sib.mark_downloaded()
        
        if self._parent and all({i.downloaded for i in self._parent._children}):
            self._parent.mark_downloaded()


class Owner(Content):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self.display_name = ""
        self.external_urls: dict = {}
    
    def parse_metadata(self, owner_resp):
        self.display_name: str = owner_resp[DISPLAY_NAME]
        self.external_urls: dict = owner_resp[EXTERNAL_URLS]
        self.hasMetadata = True


class DLContent(Content):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self._codecs: dict[str, str] = {}
        self._ext = ""
        self.printing_label = ""
        self.filepath: PurePath | None = None
        self.duration_ms = 0
    
    def fill_output_template(self) -> PurePath:
        pass
    
    def fetch_content_stream(self, stream, temppath: PurePath, pbar_stack: list) -> str:
        time_start = time.time()
        total_size = stream.input_stream.size
        downloaded = 0
        pos, pbar_stack = Printer.pbar_position_handler(1, pbar_stack)
        pbar = Printer.pbar(desc=self.printing_label, total=total_size, unit='B', unit_scale=True,
                            unit_divisor=1024, disable=not Zotify.CONFIG.get_show_download_pbar(), pos=pos)
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
        pbar.close(); pbar.clear()
        time_dl_end = time.time()
        
        return fmt_duration(time_dl_end - time_start)
    
    def get_audio_duration(self, path: PurePath) -> float:
        """ Returns the downloaded file's duration in seconds """
        
        command = ['ffprobe', '-show_entries', 'format=duration', '-i', str(path)]
        output = subprocess.run(command, capture_output=True)
        
        duration = re.search(r'[\D]=([\d\.]*)', str(output.stdout)).groups()[0]
        duration = float(duration)
        
        return duration
    
    def convert_audio_format(self, temppath: PurePath, path: PurePath) -> str | None:
        file_codec = self._codecs.get(Zotify.CONFIG.get_download_format().lower(), 'copy')
        output_params = ['-c:a', file_codec]
        
        if file_codec != 'copy':
            bitrate = Zotify.CONFIG.get_transcode_bitrate()
            if bitrate in {"auto", ""}:
                bitrate_presets = {
                    'auto': '320k' if Zotify.check_premium() else '160k',
                    'normal': '96k',
                    'high': '160k',
                    'very_high': '320k'
                    }
                bitrate = bitrate_presets.get(Zotify.CONFIG.get_download_quality(), bitrate_presets["auto"])
            output_params += ['-b:a', bitrate]
        
        time_ffmpeg_start = time.time()
        try:
            ff_m = ffmpy.FFmpeg(
                global_options=['-y', '-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
                inputs={temppath: None},
                outputs={path: output_params}
            )
            ff_m.run()
            
            if Path(temppath).exists():
                Path(temppath).unlink()
            
            time_ffmpeg_end = time.time()
            time_elapsed_ffmpeg = fmt_duration(time_ffmpeg_end - time_ffmpeg_start)
        except Exception as e:
            if isinstance(e, ffmpy.FFExecutableNotFoundError):
                reason = 'FFMPEG NOT FOUND\n'
            else:
                reason = str(e) + "\n"
            Printer.hashtaged(PrintChannel.WARNING, reason + f'SKIPPING CONVERSION TO {file_codec.upper()}')
            time_elapsed_ffmpeg = None
        
        return time_elapsed_ffmpeg


class Track(DLContent):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self._regex_flag = Zotify.CONFIG.get_regex_track()
        self._codecs = CODEC_MAP_TRACK
        self._ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "ogg")
        self.url = TRACK_URL
        
        self.disc_number = ""
        self.is_playable = False
        self.track_number = ""
        self.year = ""
        self.album: Album = None
        self.artists: list[Artist] = []
        
        # only fetched if config set
        self.genres: list[str] = []
        self.lyrics: list[str] = []
        
        # only set by Playlist API
        self.added_at = ""
        self.added_by = ""
        self.is_local = ""
  
    def parse_metadata(self, track_resp: dict[str, str | int | bool]):
        if isinstance(self._parent, LikedSongs):
            self.added_at = track_resp[ADDED_AT]
            track_resp = track_resp[TRACK]
        
        self.name: str = track_resp[NAME]
        self.disc_number = str(track_resp[DISC_NUMBER])
        self.duration_ms: int = track_resp[DURATION_MS]
        self.is_playable: bool = track_resp[IS_PLAYABLE] if IS_PLAYABLE in track_resp else False
        self.track_number = str(track_resp[TRACK_NUMBER]).zfill(2)
        
        if ALBUM in track_resp:
            if not track_resp[ALBUM][URI]:
                track_resp[ALBUM][URI] = f":local:{track_resp[ALBUM][NAME]}:::" # fallback for local tracks
            self.album: Album = self.parse_linked_objs([track_resp[ALBUM]], Album)[0]
        elif isinstance(self._parent, Album):
            self.album = self._parent
       
        if ARTISTS in track_resp:
            for artist in track_resp[ARTISTS]:
                if not artist[URI]:
                    artist[URI] = f":local:{artist[NAME]}:::" # fallback for local tracks
            self.artists = self.parse_linked_objs(track_resp[ARTISTS], Artist)
            self.printing_label = fix_filename(self.artists[0].name) + ' - ' + fix_filename(self.name)
        
        if isinstance(self._parent, Playlist):
            self.added_at = track_resp[ADDED_AT]
            self.added_by = track_resp[ADDED_BY]
            self.is_local = track_resp[IS_LOCAL]
        
        self.hasMetadata = True
    
    def compare_metadata(self):
        """ Compares metadata in self (just fetched) against metadata on file (at self.filepath),
        returns Truthy value if discrepancy is found """
        
        reliable_tags = (conv_artist_format(self.artists), conv_genre_format(self.genres), self.name, self.album, 
                         conv_artist_format(self.album.artists), self.album.year, self.disc_number, self.track_number)
        unreliable_tags = (self.id,
                           self.album.total_tracks if Zotify.CONFIG.get_disc_track_totals() else None,
                           self.album.total_discs if Zotify.CONFIG.get_disc_track_totals() else None, 
                           self.album.compilation, self.lyrics)
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
        for i in range(len(unreliable_tags)):
            if isinstance(unreliable_tags[i], list) and isinstance(unreliable_tags_onfile[i], list):
                # do not sort lyrics, since order matters
                if unreliable_tags[i] != unreliable_tags_onfile[i]:
                    mismatches.append( (unreliable_tags[i], unreliable_tags_onfile[i]) )
            else:
                if str(unreliable_tags[i]) != str(unreliable_tags_onfile[i]):
                    mismatches.append( (unreliable_tags[i], unreliable_tags_onfile[i]) )
        
        return mismatches
    
    def verify_metadata(self):
        """Overwrite metadata on file (at self.filepath) with current metadata if necessary"""
        
        mismatches = self.compare_metadata()
        relpath = self.filepath.relative_to(Zotify.CONFIG.get_root_path())
        if not mismatches:
            Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{relpath}"\n' +\
                                                       '(NO UPDATES REQUIRED)')
            return
        
        try:
            Printer.debug(f'Metadata Mismatches:', mismatches)
            self.set_audio_tags(self.filepath)
            self.set_music_thumbnail(self.filepath)
            Printer.hashtaged(PrintChannel.DOWNLOADS, f'VERIFIED:  METADATA FOR "{relpath}"\n' +\
                                                      f'(UPDATED TAGS TO MATCH CURRENT API METADATA)')
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, F'FAILED TO CORRECT METADATA FOR "{relpath}"')
            Printer.traceback(e) 
    
    def fill_output_template(self) -> PurePath:
        
        try:
            output_template = Zotify.CONFIG.get_output(self._parent._clsn)
        except:
            Printer.debug(f"Unexpected Track Parent: {self._parent._clsn}")
            output_template = Zotify.CONFIG.get_output('Query')
        
        replstrset = [
            {"{id}", "{track_id}", "{song_id}"},
            {"{name}", "{song_name}", "{track_name}", "{song_title}", "{track_title}",},
            {"{artist}", "{track_artist}", "{song_artist}", "{main_artist}",},
            {"{artists}", "{track_artists}", "{song_artists}",},
            {"{track_number}", "{song_number}", "{track_num}", "{song_num}", "{album_number}", "{album_num}",},
            {"{disc_number}", "{disc_num}",},
            {"{album_id}",},
            {"{album}", "{album_name}",},
            {"{album_artist}",},
            {"{album_artists}",},
            {"{year}", "{release_year}",},
        ]
        
        repl_mds = [
            self.id,
            self.name,
            self.artists[0].name,
            conv_artist_format(self.artists),
            self.track_number,
            self.disc_number,
            self.album.id,
            self.album.name,
            self.album.artists[0].name,
            conv_artist_format(self.album.artists),
            self.album.year,
        ]
        
        if Zotify.CONFIG.get_disc_track_totals():
            replstrset += [{"{total_tracks}",}, {"{total_discs}",},] 
            repl_mds += [self.album.total_tracks, self.album.total_discs]
        
        if isinstance(self._parent, Playlist):
            replstrset += [{"{playlist}",}, {"{playlist_id}",}, {"{playlist_number}", "{playlist_num}",},]
            playlist_number = str(self._parent.tracks_or_eps.index(self) + 1).zfill(2)
            repl_mds += [self._parent.name, self._parent.id, playlist_number]
        
        for replstrs, repl_md in zip(replstrset, repl_mds):
            for replstr in replstrs:
                output_template = output_template.replace(replstr, fix_filename(repl_md))
        
        return Zotify.CONFIG.get_root_path() / f"{output_template}.{self._ext}"
    
    def fetch_lyrics(self, filedir: PurePath) -> list[str]:
        
        if not Zotify.CONFIG.get_download_lyrics() and not Zotify.CONFIG.get_always_check_lyrics():
            return
        
        try:
            with Loader(PrintChannel.PROGRESS_INFO, "Fetching lyrics..."):
                
                lyricdir = Zotify.CONFIG.get_lyrics_location()
                if lyricdir is None:
                    lyricdir = filedir
                
                Path(lyricdir).mkdir(parents=True, exist_ok=True)
                
                # expect failure here, lyrics are not guaranteed to be available
                (raw, lyrics_dict) = Zotify.invoke_url(LYRICS_URL + self.id, expectFail=True)
                if not lyrics_dict:
                    raise ValueError(f'Failed to fetch lyrics: {self.id}')
                try:
                    formatted_lyrics = lyrics_dict[LYRICS][LINES]
                except KeyError:
                    raise ValueError(f'Failed to fetch lyrics: {self.id}')
                
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
                    
                    lrc_header = [f"[ti: {self.name}]\n",
                                f"[ar: {conv_artist_format(self.artists, FORCE_NO_LIST=True)}]\n",
                                f"[al: {self.album.name}]\n",
                                f"[length: {self.duration_ms // 60000}:{(self.duration_ms % 60000) // 1000}]\n",
                                f"[by: Zotify v{__version__}]\n",
                                "\n"]
                
                self.lyrics = lyrics
                with open(lyricdir / f"{self.printing_label}.lrc", 'w', encoding='utf-8') as file:
                    if Zotify.CONFIG.get_lyrics_header():
                        file.writelines(lrc_header)
                    file.writelines(lyrics)
            
        except ValueError:
            Printer.hashtaged(PrintChannel.SKIPPING, f'LYRICS FOR "{self.printing_label}" (LYRICS NOT AVAILABLE)')
    
    def get_audio_tags(self):
        tags = music_tag.load_file(self.filepath)
        
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
        if self.filepath.suffix.lower() == ".mp3":
            custom_tags = [MP3_CUSTOM_TAG_PREFIX + tag.upper() for tag in custom_tags]
        elif self.filepath.suffix.lower() == ".m4a":
            custom_tags = [M4A_CUSTOM_TAG_PREFIX + tag for tag in custom_tags]
        unreliable_tags.extend(custom_tags)
        
        # Printer.debug(tags.mfile.tags.__dict__)
        tag_dict = dict(tags.mfile.tags)
        utag_vals = []
        for utag in unreliable_tags:
            val = None
            fetch_method = "legit"
            try:
                val = tags[utag].val
            except:
                fetch_method = "hacky"
                if utag in tag_dict:
                    val = tag_dict[utag]
            
            if utag == LYRICS:
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
            # Printer.debug(f"{fetch_method} {utag}", val)
            utag_vals.append(val)
            return (artists, genres, track_name, album_name, album_artist, release_year, disc_number, track_number), \
                   tuple(utag_vals)
    
    def set_audio_tags(self, path: PurePath):
        
        def custom_mp3_tag(audio_file: music_tag.AudioFile, tag: str, val: str):
            from mutagen.id3 import TXXX
            audio_file.mfile.tags.add(TXXX(encoding=3, desc=tag.upper(), text=[val]))
        
        def custom_m4a_tag(audio_file: music_tag.AudioFile, tag: str, val: str):
            from music_tag.mp4 import freeform_set
            atomic_tag = M4A_CUSTOM_TAG_PREFIX + tag
            freeform_set(audio_file, atomic_tag, type('tag', (object,), {'values': [val]})())
        
        def custom_ogg_tag(audio_file: music_tag.AudioFile, tag: str, val: str):
            from music_tag.file import TAG_MAP_ENTRY
            audio_file.tag_map[tag] = TAG_MAP_ENTRY(getter=tag, setter=tag, type=type(val))
            audio_file[tag] = val
        
        def custom_tag(audio_file: music_tag.AudioFile, tag: str, val: str):
            if self._ext == "mp3":
                custom_mp3_tag(audio_file, tag, val)
            elif self._ext == "m4a":
                custom_m4a_tag(audio_file, tag, val)
            else:
                custom_ogg_tag(audio_file, tag, val)
        
        tags: music_tag.AudioFile = music_tag.load_file(path)
        
        # Reliable Tags
        tags[ARTIST] = conv_artist_format(self.artists)
        tags[GENRE] = conv_genre_format(self.genres)
        tags[TRACKTITLE] = self.name
        tags[ALBUM] = self.album.name
        tags[ALBUMARTIST] = conv_artist_format(self.album.artists)
        tags[YEAR] = self.album.year
        tags[DISCNUMBER] = self.disc_number
        tags[TRACKNUMBER] = self.track_number
        
        # Unreliable Tags
        custom_tag(tags, "trackid", self.id)
        custom_tag(tags, "uri", self.uri)
        
        if Zotify.CONFIG.get_disc_track_totals():
            tags[TOTALTRACKS] = self.album.total_tracks
            if self.album.total_discs is not None:
                tags[TOTALDISCS] = self.album.total_discs
        
        if self.album.compilation:
            tags[COMPILATION] = self.album.compilation
        
        if self.lyrics and Zotify.CONFIG.get_save_lyrics_tags():
            tags[LYRICS] = "".join(self.lyrics)
        
        if self._ext == "mp3" and not Zotify.CONFIG.get_disc_track_totals():
            # music_tag python library writes DISCNUMBER and TRACKNUMBER as X/Y instead of X for mp3
            # this method bypasses all internal formatting, probably not resilient against arbitrary inputs
            tags.set_raw("mp3", "TPOS", str(self.disc_number))
            tags.set_raw("mp3", "TRCK", str(self.track_number))
        
        tags.save()
    
    def set_music_thumbnail(self, path: PurePath):
        # jpeg format expected from request
        img = requests.get(self.album.image_url).content
        tags: music_tag.AudioFile = music_tag.load_file(path)
        tags[ARTWORK] = img
        tags.save()
        
        if not Zotify.CONFIG.get_album_art_jpg_file():
            return
        
        jpg_filename = 'cover.jpg' if isinstance(self._parent, Album) else path.stem + '.jpg'
        jpg_path = Path(path.parent / jpg_filename)
        
        if not jpg_path.exists():
            with open(jpg_path, 'wb') as jpg_file:
                jpg_file.write(img)
    
    def download(self, pbar_stack: list):
        if self.downloaded:
            # okay to skip get_always_check_lyrics, since it was already checked this session
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (TRACK ALREADY DOWNLOADED THIS SESSION)')
            return
        
        path = self.fill_output_template()
        
        if Zotify.CONFIG.get_always_check_lyrics():
            self.fetch_lyrics(path.parent)
        
        if Zotify.CONFIG.get_skip_comp_albums() and self.album and self.album.compilation:
            return
        elif Zotify.CONFIG.get_download_parent_album() and not Zotify.CONFIG.get_optimized_dl_order():
            # if Zotify.CONFIG.get_optimized_dl_order(), then album's children have already been included
            # see Query.fetch_extra_metadata() and Query.download()
            self.album.download(pbar_stack)
            return
        elif self.regex_check():
            return
        
        with Loader(PrintChannel.PROGRESS_INFO, "Preparing download..."):
            temppath = path
            if Zotify.CONFIG.get_temp_download_dir() != '':
                rando = str(uuid.uuid4())
                temppath = Zotify.CONFIG.get_temp_download_dir() / f'zotify_{rando}_{self.id}.tmp'
            
            path_exists = Path(path).is_file() and Path(path).stat().st_size
            in_dir_songids = self.id in get_directory_song_ids(path.parent)
            in_global_songids = self.id in get_archived_song_ids()
            Printer.debug("Duplicate Check\n" +\
                         f"File Already Exists: {path_exists}\n" +\
                         f"song_id in Local Archive: {in_dir_songids}\n" +\
                         f"song_id in Global Archive: {in_global_songids}")
            
            # same track_path, not same song_id, rename the newcomer
            if not in_dir_songids and not Zotify.CONFIG.get_disable_directory_archives():
                path = check_path_dupes(path)
                path_exists = False # new track_path guaranteed to be unique
        
        if not self.is_playable:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (TRACK IS UNAVAILABLE)')
            return
        if path_exists and Zotify.CONFIG.get_skip_existing() and Zotify.CONFIG.get_disable_directory_archives():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{path.relative_to(Zotify.CONFIG.get_root_path())}" (FILE ALREADY EXISTS)')
            self.filepath = path
            return
        if in_dir_songids and Zotify.CONFIG.get_skip_existing() and not Zotify.CONFIG.get_disable_directory_archives():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (TRACK ALREADY EXISTS)')
            self.filepath = path
            return
        if in_global_songids and Zotify.CONFIG.get_skip_previously_downloaded():
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (TRACK DOWNLOADED PREVIOUSLY)')
            self.filepath = path
            return
        
        stream = Zotify.get_content_stream(TrackId.from_base62(self.id), Zotify.DOWNLOAD_QUALITY)
        if stream is None:
            Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING SONG - FAILED TO GET CONTENT STREAM\n' +\
                                                 f'Track_ID: {self.id}')
            return
        create_download_directory(temppath.parent)
        time_elapsed_dl = self.fetch_content_stream(stream, temppath, pbar_stack)
        
        if not Zotify.CONFIG.get_always_check_lyrics():
            self.fetch_lyrics(path.parent)
        
        with Loader(PrintChannel.PROGRESS_INFO, "Converting file..."):
            # convert temppath -> path here
            create_download_directory(path.parent)
            time_elapsed_ffmpeg = self.convert_audio_format(temppath, path)
            if time_elapsed_ffmpeg is None:
                path = PurePath(Path(temppath).rename(path.with_suffix(".ogg")))
            self.filepath = path
            self.mark_downloaded()
        
        try:
            self.set_audio_tags(path)
            self.set_music_thumbnail(path)
        except Exception as e:
            Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO WRITE METADATA\n' +\
                                                  'Ensure FFMPEG is installed and added to your PATH')
            Printer.traceback(e)
        
        Printer.hashtaged(PrintChannel.DOWNLOADS, f'DOWNLOADED: "{path.relative_to(Zotify.CONFIG.get_root_path())}"\n' +\
                                                  f'DOWNLOAD TOOK {time_elapsed_dl}' + \
                                                  f' (PLUS {time_elapsed_ffmpeg} CONVERTING)' if time_elapsed_ffmpeg else '')
        if not in_global_songids:
            add_to_song_archive(self.id, path.name, self.artists[0].name, self.name)
        if not in_dir_songids:
            add_to_directory_song_archive(path, self.id, self.artists[0].name, self.name)


class Episode(DLContent):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self._regex_flag = Zotify.CONFIG.get_regex_episode()
        self._codecs = CODEC_MAP_EPISODE
        self._ext = EXT_MAP.get(Zotify.CONFIG.get_download_format().lower(), "copy")
        self.url = EPISODE_URL
        
        self.desc = ""
        self.explicit = False
        self.external = False
        self.release_date = ""
        self.is_playable = False
        self.show: Show = None
        
        # only set by Playlist API
        self.added_at = ""
        self.added_by = ""
        self.is_local = ""
    
    def parse_metadata(self, episode_resp: dict[str, str | int | bool]):
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
        elif isinstance(self._parent, Show):
            self.show = self._parent
            self.printing_label = fix_filename(self.show.name) + ' - ' + fix_filename(self.name)
        
        elif isinstance(self._parent, Playlist):
            self.added_at = episode_resp[ADDED_AT]
            self.added_by = episode_resp[ADDED_BY]
            self.is_local = episode_resp[IS_LOCAL]
        
        self.hasMetadata = True
    
    def fill_output_template(self) -> PurePath:
        return PurePath(Zotify.CONFIG.get_root_podcast_path()) / f"{self.show.name}/{self.printing_label}.{self._ext}"
    
    def download_directly(direct_download_url: str, path: PurePath) -> str:
        time_start = time.time()
        r = requests.get(direct_download_url, stream=True, allow_redirects=True)
        if r.status_code != 200:
            r.raise_for_status()  # Will only raise for 4xx codes, so...
            raise RuntimeError(f"Request to {direct_download_url} returned status code {r.status_code}")
        file_size = int(r.headers.get('Content-Length', 0))
        if not file_size:
            file_size = int(r.headers.get('content-length', 0))
        
        path = Path(path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        
        desc = "(Unknown total file size)" if file_size == 0 else ""
        r.raw.read = functools.partial(
            r.raw.read, decode_content=True)  # Decompress if needed
        with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
            with path.open("wb") as f:
                shutil.copyfileobj(r_raw, f)
        
        time_dl_end = time.time()
        return fmt_duration(time_dl_end - time_start)
    
    def download(self, pbar_stack: list | None):
        if self.downloaded:
            Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (EPISODE ALREADY DOWNLOADED THIS SESSION)')
            return
        
        if not all(self.name, self.duration_ms, self.show):
            Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING EPISODE - FAILED TO QUERY METADATA\n' +\
                                                 f'Episode_ID: {self.id}')
            return
        
        if self.regex_check():
            return
        
        with Loader(PrintChannel.PROGRESS_INFO, "Preparing download..."):
            requested_path = self.fill_output_template()
            path = requested_path.with_suffix(".tmp")
            
            path_exists = False # file suffix agnostic
            for file_match in Path(path.parent).glob(path.stem + ".*", case_sensitive=True):
                if file_match.stat().st_size:
                    path_exists = True
                    break
            if path_exists and Zotify.CONFIG.get_skip_existing():
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{self.printing_label}" (EPISODE ALREADY EXISTS)')
                return
            
            (raw, resp) = Zotify.invoke_url(PARTNER_URL + self.id + '"}&extensions=' + PERSISTED_QUERY)
            direct_download_url = resp[DATA][EPISODE][AUDIO][ITEMS][-1][URL]
        
        if "anon-podcast.scdn.co" in direct_download_url or "audio_preview_url" not in resp:
            stream = Zotify.get_content_stream(EpisodeId.from_base62(self.id), Zotify.DOWNLOAD_QUALITY)
            if stream is None:
                Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING EPISODE - FAILED TO GET CONTENT STREAM\n' +\
                                                     f'Episode_ID: {self.id}')
                return
            
            create_download_directory(path.parent)
            time_elapsed_dl = self.fetch_content_stream(stream, path, pbar_stack)
        else:
            create_download_directory(path.parent)
            try:
                time_elapsed_dl = self.download_directly(direct_download_url, path)
            except RuntimeError as e:
                Printer.hashtaged(PrintChannel.ERROR, 'FAILED TO DOWNLOAD EPISODE DIRECTLY')
                Printer.traceback(e)
                return
        
        Printer.hashtaged(PrintChannel.DOWNLOADS, f'DOWNLOADED: "{path}"\n' +\
                                                  f'DOWNLOAD TOOK {time_elapsed_dl}')
        
        try:
            with Loader(PrintChannel.PROGRESS_INFO, "Identifying episode audio codec..."):
                ff_m = ffmpy.FFprobe(
                    global_options=['-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
                    inputs={path: ["-show_entries", "stream=codec_name"]},
                )
                stdout, _ = ff_m.run(stdout=subprocess.PIPE)
                codec = stdout.decode().strip().split("=")[1].split("\r")[0].split("\n")[0]
                
                path_codec = path.with_suffix("." + {EXT_MAP.get(codec, codec)})
                if Path(path_codec).exists():
                    Path(path_codec).unlink()
                path = Path(path).rename(path_codec)
                self.filepath = path
            
            Printer.debug(f"Detected Codec: {codec}\n" +\
                            f"File Renamed: {path.name}")
        
        except ffmpy.FFExecutableNotFoundError:
            path = Path(path).rename(path.with_suffix(".mp3"))
            Printer.hashtaged(PrintChannel.WARNING, 'FFMPEG NOT FOUND\n' +\
                                                    'SKIPPING CODEC ANALYSIS - OUTPUT ASSUMED MP3')
        
        self.mark_downloaded()
        self.filepath = path
        if requested_path.suffix == ".copy":
            return
        
        elif path.suffix != requested_path.suffix:
            with Loader(PrintChannel.PROGRESS_INFO, "Converting file..."):
                time_elapsed_ffmpeg = self.convert_audio_format(path, requested_path)
                if time_elapsed_ffmpeg is not None:
                    self.filepath = requested_path


class Container(Content):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self._contains = Content, Container
        self._preloaded = 0
        self._fetch_q = 50
        self._unit = "Content" if isinstance(self._contains, tuple) else self._contains.__name__ + "s"
        self._disable_flag = Zotify.CONFIG.get_show_url_pbar()
        self.needs_expansion = False
    
    # supersede in each child class
    def extChildren(self, _extensibleChildren: list[Content | Container],
                    objs: list[Content | Container] = []) -> list[Content | Container]:
        _extensibleChildren.extend(objs)
        return _extensibleChildren
    
    @property
    def len(self):
        return len(self.extChildren())
    
    def fetch_items(self, item_key: str, args: str = "", hide_loader: bool = False) -> list[dict]:
        with Loader(PrintChannel.PROGRESS_INFO, f"Fetching {self._clsn.lower()} {item_key}...", disabled=hide_loader):
            if args: args = "&" + args
            return Zotify.invoke_url_nextable(f'{self.url}/{self.id}/{item_key}?{MARKET_APPEND}{args}',
                                              ITEMS, self._fetch_q, offset=self._preloaded)
    
    def recurse_children(self) -> list[Content]:
        children = []
        for c in self.extChildren():
            if isinstance(c, DLContent): children.append(c)
            else: children.extend(c.recurse_children())
        return children
    
    def grab_more_children(self, hide_loader: bool = False):
        items = self.fetch_items(hide_loader=hide_loader)
        self.extChildren(self.parse_linked_objs(items, self._contains))
    
    def create_pbar(self, pbar_stack: list | None = None) -> tuple[list[Content], list]:
        pos, pbar_stack = Printer.pbar_position_handler(7, pbar_stack)
        pbar: list[Content] = Printer.pbar(self.extChildren(), self.name, pos=pos,
                                           unit=self._unit, disable=not self._disable_flag)
        pbar_stack.append(pbar)
        return pbar, pbar_stack
    
    def download(self, pbar_stack: list | None):
        if self.downloaded:
            return
        
        pbar, pbar_stack = self.create_pbar(pbar_stack)
        for child in pbar:
            predownloaded = child.downloaded
            child.download(pbar_stack)
            Printer.refresh_all_pbars(pbar_stack)
            if not predownloaded: wait_between_downloads()
        self.mark_downloaded()


class Playlist(Container):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self._contains = Track, Episode
        self._preloaded = 100
        self._fetch_q = 100
        self._disable_flag = Zotify.CONFIG.get_show_playlist_pbar()
        
        self.url = PLAYLIST_URL
        self.collaborative = False
        self.desc = ""
        self.image_url = ""
        self.owner = ""
        self.public = False
        self.snapshot_id = ""
        self.tracks_or_eps: list[Track | Episode] = []
    
    def extChildren(self, objs: list[Content | Container] = []):
        return super().extChildren(self.tracks_or_eps, objs)
    
    def parse_metadata(self, playlist_resp: dict[str, str | bool]):
        self.name: str = playlist_resp[NAME]
        self.collaborative: bool = playlist_resp[COLLABORATIVE]
        self.desc: str = playlist_resp[DESCRIPTION]
        largest_image = max(playlist_resp[IMAGES], key=lambda img: img[WIDTH], default={URL: ""})
        self.image_url: str = largest_image[URL]
        self.public: bool = playlist_resp[PUBLIC]
        self.snapshot_id: str = playlist_resp[SNAPSHOT_ID]
        
        self.owner: Owner = self.parse_linked_objs([playlist_resp[OWNER]], Owner)[0]
        
        tracks_or_eps: list[dict] = [item[TRACK] for item in playlist_resp[TRACKS][ITEMS]]
        for track_or_ep, item in zip(tracks_or_eps, playlist_resp[TRACKS][ITEMS]):
            track_or_ep[ADDED_AT] = item[ADDED_AT]
            track_or_ep[ADDED_BY] = item[ADDED_BY]
            track_or_ep[IS_LOCAL] = item[IS_LOCAL]
        self.tracks_or_eps = self.parse_linked_objs(tracks_or_eps, (Track, Episode)) # possible underflow if len(items) > 100
        # self.tracks_or_eps.sort(key=lambda s: strptime_utc(s[ADDED_AT]))
        self.needs_expansion = playlist_resp[TRACKS][NEXT] is not None
        
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
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self._contains = Track
        self._preloaded = 50
        self._disable_flag = Zotify.CONFIG.get_show_album_pbar()
        self._regex_flag = Zotify.CONFIG.get_regex_album()
        
        self.url = ALBUM_URL
        self.compilation = 0
        self.image_url = ""
        self.label = ""
        self.release_date = ""
        self.total_discs = ""
        self.total_tracks = ""
        self.type = ""
        self.artists: list[Artist] = []
        self.tracks: list[Track] = []
    
    def extChildren(self, objs: list[Content | Container] = []):
        return super().extChildren(self.tracks, objs)
    
    def parse_metadata(self, album_resp: dict[str, str | bool]):
        self.name: str = album_resp[NAME]
        self.compilation: int = 1 if COMPILATION == album_resp[ALBUM_TYPE] else 0
        largest_image = max(album_resp[IMAGES], key=lambda img: img[WIDTH], default={URL: ""})
        self.image_url: str = largest_image[URL]
        self.release_date: str = album_resp[RELEASE_DATE]
        self.year: str = self.release_date.split('-')[0] if self.release_date else ""
        self.total_tracks = str(album_resp[TOTAL_TRACKS]).zfill(2)
        self.type: str = album_resp[ALBUM_TYPE]
        
        if ARTISTS in album_resp:
            self.artists = self.parse_linked_objs(album_resp[ARTISTS], Artist)
        
        if TRACKS in album_resp:
            self.label: str = album_resp[LABEL]
            self.tracks = self.parse_linked_objs(album_resp[TRACKS][ITEMS], Track) # possible underflow if len(items) > 100
            self.needs_expansion = album_resp[TRACKS][NEXT] is not None
            if not self.needs_expansion:
                # set in self.grab_more_children() if album incomplete
                self.total_discs = str(album_resp[TRACKS][ITEMS][-1][DISC_NUMBER])
            self.hasMetadata = True
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        return super().fetch_items(TRACKS, hide_loader=hide_loader)
    
    def grab_more_children(self, hide_loader: bool = False):
        super().grab_more_children(hide_loader=hide_loader)
        self.total_discs = str(self.tracks[-1].disc_number)
    
    def download(self, pbar_stack):
        if Zotify.CONFIG.get_skip_comp_albums() and self.compilation:
            return
        elif self.regex_check():
            return
        super().download(pbar_stack)


class Artist(Container):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        # self.toptrackmode: bool = Zotify.get_artist_fetch_top_tracks()
        self.toptrackmode: bool = False
        self._contains = Album if not self.toptrackmode else Track
        self._fetch_q = 20 if not self.toptrackmode else 100
        self._disable_flag = Zotify.CONFIG.get_show_artist_pbar()
        
        self.url = ARTIST_URL
        self.genres: list[str] = []
        self.total_followers = 0
        self.albums: list[Album] = []
        self.top_songs: list[Track] = []
    
    def extChildren(self, objs: list[Content | Container] = []):
        return super().extChildren(self.albums if not self.toptrackmode else self.top_songs, objs)
    
    def parse_metadata(self, artist_resp: dict[str, str | int | list[str]]):
        self.name: str = artist_resp[NAME]
        
        if GENRES in artist_resp:
            self.total_followers: int = artist_resp[FOLLOWERS][TOTAL]
            self.genres: list[str] = artist_resp[GENRES]
            self.hasMetadata = True
        
        self.needs_expansion = True
    
    def fetch_items(self, hide_loader: bool = False) -> list[dict | None]:
        if self.toptrackmode:
            with Loader(PrintChannel.PROGRESS_INFO, f"Fetching {self._clsn.lower()} top tracks...", disabled=hide_loader):
                top_track_url = f'{self.url}/{self.id}/top-tracks&{MARKET_APPEND}'
                artist_items = Zotify.invoke_url(top_track_url, None, {"limit": self._fetch_q})
        else:
            artist_items = super().fetch_items(ALBUMS, hide_loader=hide_loader)
        return artist_items


class Show(Container):
    def __init__(self, id_or_uri: str, _parent: Content | Container = None):
        super().__init__(id_or_uri, _parent)
        self._contains = Episode
        self._preloaded = 50
        self._disable_flag = Zotify.CONFIG.get_show_album_pbar()
        
        self.url = SHOW_URL
        self.desc = ""
        self.explicit = False
        self.external = False
        self.image_url = ""
        self.publisher = ""
        self.total_episodes = ""
        self.episodes: list[Episode] = []
    
    def extChildren(self, objs: list[Content | Container] = []):
        return super().extChildren(self.episodes, objs)
    
    def parse_metadata(self, show_resp: dict[str, str | bool]):
        self.name: str = show_resp[NAME]
        self.desc: str = show_resp[DESCRIPTION]
        self.explicit: bool = show_resp[EXPLICIT]
        self.external: bool = show_resp[IS_EXTERNALLY_HOSTED]
        largest_image = max(show_resp[IMAGES], key=lambda img: img[WIDTH], default={URL: ""})
        self.image_url: str = largest_image[URL]
        self.publisher: str = show_resp[PUBLISHER]
        self.total_episodes = str(show_resp[TOTAL_EPISODES]).zfill(2)
        
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
    def __init__(self, id_or_uri: str, _parent: Content = None):
        super().__init__(id_or_uri, _parent)
        self.url = CHAPTER_URL


class Audiobook(Container):
    def __init__(self, id_or_uri: str, _parent: Content = None):
        super().__init__(id_or_uri, _parent)
        self._contains = Chapter
        self._preloaded = 50
        self._disable_flag = Zotify.CONFIG.get_show_album_pbar()
        
        self.url = AUDIOBOOK_URL
# end not implemented


ITEM_FETCH = {
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
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self._contains = Content, Container
        self._unit = "Content" if Zotify.CONFIG.get_optimized_dl_order() else "URL"
        self.name = "Total Progress"
        self.pbar_stack: list = []
        
        self.requested_urls = ""
        self.parsed_request: list[list[str]] = []
        self.requested_objs: list[list[DLContent | Container]] = []
    
    def extChildren(self, objs: list[Content | Container] = []):
        return super().extChildren(self.requested_objs, objs)
    
    def request(self, requested_urls: str) -> Query:
        self.requested_urls = requested_urls # only used here, can remove later
        self.parsed_request = bulk_regex_urls(self.requested_urls)
        Printer.debug(f'Starting Download of {len(self.parsed_request)} {self._unit}')
        return self
    
    def create_linked_obj(self, cls: Content | Container, id_or_uri: str) -> Content | Container:
        return self.findChild(cls(id_or_uri, self))
    
    def create_direct_objs(self, clss: tuple[DLContent | Container] = ITEM_FETCH) -> list[list[DLContent | Container]]:
        direct_reqs_objs = []
        for cls, id_list in zip(clss, self.parsed_request):
            objs: list[Content | Container] = [None]*len(id_list)
            for i, id in enumerate(id_list):
                objs[i] = self.create_linked_obj(cls, id)
            direct_reqs_objs.append(objs)
        return direct_reqs_objs
    
    def fetch_direct_metadata(self, direct_reqs_objs: list[list[DLContent | Container]]) -> tuple[list[list[DLContent | Container]], list[list[dict]]]:
        direct_req_item_resps = []
        for q, objs in zip(ITEM_FETCH.values(), direct_reqs_objs):
            if not objs:
                direct_req_item_resps.append([])
                continue
            elif isinstance(objs[0], Playlist) or len(objs) == 1:
                item_resps = [obj.fetch_metadata() for obj in objs]
            else:
                with Loader(PrintChannel.PROGRESS_INFO, f"Fetching bulk {objs[0]._clsn.lower()} information..."):
                    url = f"{objs[0].url}?{MARKET_APPEND}&{BULK_APPEND}"
                    item_resps = Zotify.invoke_url_bulk(url, [obj.id for obj in objs], objs[0]._plural, q)
            direct_req_item_resps.append(item_resps)
        return direct_reqs_objs, direct_req_item_resps
    
    def parse_direct_metadata(self, direct_reqs_objs: list[list[DLContent | Container]], direct_req_item_resps: list[list[dict]]):
        """This sets self.extChildren == self.requested_objs"""
        for objs, item_resps in zip(direct_reqs_objs, direct_req_item_resps):
            if not objs:
                self.requested_objs.append([])
                continue
            with Loader(PrintChannel.PROGRESS_INFO, f"Parsing {objs[0]._clsn.lower()} information..."):
                for obj, item_resp in zip(objs, item_resps):
                    obj.parse_metadata(item_resp)
                    if isinstance(obj, Container) and obj.needs_expansion:
                        obj.grab_more_children()
            self.requested_objs.append(objs) # basic metadata complete objs
    
    def fetch_extra_metadata(self):
        alltracks = {t for t in self.subContent if isinstance(t, Track) and t.id}
        if Zotify.CONFIG.get_save_genres():
            artists = set.union(set(), *(set(track.artists) for track in alltracks))
            artist_ids: dict[str, Artist] = {artist.id: artist for artist in artists if artist.id and not artist.hasMetadata}
            if artist_ids:
                with Loader(PrintChannel.PROGRESS_INFO, f"Fetching bulk genre information..."):
                    artist_resps = Zotify.invoke_url_bulk(ARTIST_BULK_URL, list(artist_ids.keys()), ARTISTS, ITEM_FETCH[Artist])
                    for artist_resp in artist_resps:
                        artist_ids[artist_resp[ID]].parse_metadata(artist_resp)
                        artist_ids[artist_resp[ID]].needs_expansion = False
            for track in alltracks:
                genres: list[str] = [*set.union(*[set(artist.genres) for artist in track.artists])]
                genres.sort()
                track.genres = genres
        
        if Zotify.CONFIG.get_disc_track_totals() or Zotify.CONFIG.get_download_parent_album():
            albums = {track.album for track in alltracks}
            album_ids: dict[str, Album] = {album.id: album for album in albums if album.id and not album.hasMetadata}
            if album_ids:
                loader_text = "parent album" if Zotify.CONFIG.get_download_parent_album() else "track/disc total"
                with Loader(PrintChannel.PROGRESS_INFO, f"Fetching bulk {loader_text} information..."):
                    album_resps = Zotify.invoke_url_bulk(ALBUM_BULK_URL, list(album_ids.keys()), ALBUMS, ITEM_FETCH[Album])
                    for album_resp in album_resps:
                        a = album_ids[album_resp[ID]]
                        a._accepting_children = Zotify.CONFIG.get_download_parent_album()
                        a.parse_metadata(album_resp)
                        a.grab_more_children(hide_loader=True)
    
    def create_m3u8_playlists(self):
        if Zotify.CONFIG.get_m3u8_location():
            m3u8_dir: PurePath = Zotify.CONFIG.get_m3u8_location()
        for obj_list in self.requested_objs:
            if not obj_list:
                continue
            if isinstance(obj_list[0], Container):
                for obj in obj_list:
                    if not Zotify.CONFIG.get_m3u8_location():
                        allpaths = {c.filepath for c in obj.subContent if isinstance(c, DLContent) if c.filepath}
                        m3u8_dir = get_common_dir(allpaths)
                    m3u8_path = m3u8_dir / f"{obj.name}.m3u8"
                    Path(m3u8_path).unlink(missing_ok=True)
                    add_to_m3u8(m3u8_path, obj.recurse_children())
            else:
                if not Zotify.CONFIG.get_m3u8_location():
                    allpaths = {obj.filepath for obj in obj_list if obj.filepath}
                    m3u8_dir = get_common_dir(allpaths)
                m3u8_path = m3u8_dir / f"{self.id}_{obj_list[0]._plural}.m3u8"
                Path(m3u8_path).unlink(missing_ok=True)
                add_to_m3u8(m3u8_path, obj_list)
    
    def download(self):
        requested_objs = self.requested_objs
        if Zotify.CONFIG.get_optimized_dl_order():
            downloadables = [c for c in self.subContent if isinstance(c, DLContent) and c.id]
            downloadables.sort(key=lambda x: x.duration_ms); edge_zip(downloadables)
        else:
            downloadables = []
            for cats in self.requested_objs: downloadables.extend(cats)
        self.requested_objs = downloadables
        
        try:
            super().download(pbar_stack=None)
        except KeyboardInterrupt:
            Printer.hashtaged(PrintChannel.MANDATORY, "USER CANCELED DOWNLOADS EARLY\n"+
                                                      "ATTEMPTING TO CLEAN UP")
        
        self.requested_objs = requested_objs
        
        # all pbars are finished here, which will print an extra newline
        if Zotify.CONFIG.get_show_any_progress():
            Printer.back_up()
        
        if Zotify.CONFIG.get_export_m3u8():
            with Loader(PrintChannel.PROGRESS_INFO, "Creating m3u8 files..."):
                self.create_m3u8_playlists()
    
    def execute(self):
        direct_reqs_objs = self.create_direct_objs()
        self.parse_direct_metadata(*self.fetch_direct_metadata(direct_reqs_objs))
        self.fetch_extra_metadata()
        self.download()


class LikedSongs(Query):
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self._contains = Track
        self._unit = "Tracks"
        self.name = "Liked Songs"
        self.url = USER_SAVED_TRACKS_URL
    
    def create_fetch_liked_songs(self):
        with Loader(PrintChannel.PROGRESS_INFO, f"Fetching Liked Songs..."):
            liked_songs_resps = Zotify.invoke_url_nextable(self.url)
            self.parsed_request = [[t[TRACK][URI] for t in liked_songs_resps]]
            liked_songs_objs = self.create_direct_objs((Track))
        return liked_songs_objs, [liked_songs_resps]
   
    def create_m3u8_playlists(self):
        liked_tracks: list[Track] = self.requested_objs[0]
        if Zotify.CONFIG.get_m3u8_location():
            m3u8_dir: PurePath = Zotify.CONFIG.get_m3u8_location()
        else:
            allpaths = {obj.filepath for obj in liked_tracks if obj.filepath}
            m3u8_dir = get_common_dir(allpaths)
        m3u8_path = m3u8_dir / f"{self.name}.m3u8"
        
        if Zotify.CONFIG.get_liked_songs_archive_m3u8() and Path(m3u8_path).exists():
            raw_liked_archive = fetch_m3u8_songs(m3u8_path)
            newest_liked_track_path = raw_liked_archive[1]
            
            prepend = liked_tracks; append = None
            for i, liked in enumerate(liked_tracks):
                if liked.filepath == newest_liked_track_path:
                    prepend = liked_tracks[:i] # don't include matching Track
                    append = raw_liked_archive # includes matching track m3u8 entry
                    break
            
            Path(m3u8_path).unlink(missing_ok=True)
            add_to_m3u8(m3u8_path, prepend, append)
        else:
            Path(m3u8_path).unlink(missing_ok=True)
            add_to_m3u8(m3u8_path, liked_tracks)
 
    def execute(self):
        self.parse_direct_metadata(*self.create_fetch_liked_songs())
        self.fetch_extra_metadata()
        self.download()


class UserPlaylists(Query):
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self._contains = Playlist
        self._unit = "Playlist"
        self.name = "Created Playlists"
        self.url = USER_PLAYLISTS_URL
    
    def fetch_user_playlists_display(self) -> list[None | dict]:
        user_playlist_resps = Zotify.invoke_url_nextable(self.url)
        display_list = [[i+1, str(p[NAME])] for i, p in enumerate(user_playlist_resps)]
        Printer.table("PLAYLISTS", ('ID', 'Name'), [[0, "ALL PLAYLISTS"]].extend(display_list))
        return [None] + user_playlist_resps
    
    def select_user_playlists(self, user_playlist_resps: list[None | dict]) -> tuple[list[list[Artist]], list[list[dict]]]:
        selected_playlist_resps: list[None | dict] = select(user_playlist_resps)
        if selected_playlist_resps[0] == None:
            # option 0 == get all choices
            selected_playlist_resps = user_playlist_resps[1:]
        self.parsed_request = [[p[URI] for p in selected_playlist_resps]]
        return self.create_direct_objs((Playlist)), [selected_playlist_resps]
    
    def execute(self):
        # with Loader(PrintChannel.PROGRESS_INFO, f"Fetching Created Playlists..."):
        fetched_playlists = self.fetch_user_playlists_display()
        self.parse_direct_metadata(*self.select_user_playlists(fetched_playlists))
        self.fetch_extra_metadata()
        self.download()


class FollowedArtists(Query):
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self._contains = Artist
        self._unit = "Artists"
        self.name = "Followed Artists"
        self.url = USER_FOLLOWED_ARTISTS_URL
    
    def fetch_followed_artists_display(self) -> list[None | dict]:
        followed_artist_resps = Zotify.invoke_url_nextable(self.url, stripper=ARTISTS)
        display_list = [[i+1, str(a[NAME])] for i, a in enumerate(followed_artist_resps)]
        Printer.table("ARTISTS", ('ID', 'Name'), [[0, "ALL ARTISTS"]].extend(display_list))
        return [None] + followed_artist_resps
    
    def select_followed_artists(self, followed_artist_resps: list[None | dict]) -> tuple[list[list[Artist]], list[list[dict]]]:
        selected_artist_resps: list[None | dict] = select(followed_artist_resps)
        if selected_artist_resps[0] == None:
            # option 0 == get all choices
            selected_artist_resps = followed_artist_resps[1:]
        self.parsed_request = [[a[URI] for a in selected_artist_resps]]
        return self.create_direct_objs((Artist)), [selected_artist_resps]
    
    def execute(self):
        # with Loader(PrintChannel.PROGRESS_INFO, f"Fetching Followed Artists..."):
        fetched_artists = self.fetch_followed_artists_display()
        self.parse_direct_metadata(*self.select_followed_artists(fetched_artists))
        self.fetch_extra_metadata()
        self.download()


class VerifyLibrary(Query):
    def __init__(self, timestamp: str):
        super().__init__(timestamp)
        self._contains = Track
        self._unit = "Tracks"
        self.name = "Verifiable Tracks"
    
    def create_fetch_verifiable_tracks(self):
        # ONLY WORKS WITH ARCHIVED TRACKS (THEORETICALLY GUARANTEES BULK_URL TO WORK)
        archived_tracks = get_archived_entries()
        archived_ids = [entry.strip().split('\t')[0] for entry in archived_tracks]
        archived_filenames = [PurePath(entry.strip().split('\t')[4]).stem for entry in archived_tracks]
        
        verifiable_tracks: list[Track] = []
        library = walk_directory_for_tracks(Zotify.CONFIG.get_root_path())
        for entry in library:
            if entry.stem in archived_filenames:
                track: Track = self.create_linked_obj(Track, archived_ids[archived_filenames.index(entry.stem)])
                track.filepath = PurePath(entry)
                verifiable_tracks.append(track)
        
        track_resps = Zotify.invoke_url_bulk(TRACK_BULK_URL, [t.id for t in verifiable_tracks], TRACKS)
        
        return [verifiable_tracks], [track_resps]
    
    def execute(self):
        self.parse_direct_metadata(*self.create_fetch_verifiable_tracks())
        pbar, pbar_stack = self.create_pbar()
        self.fetch_extra_metadata()
        for child in pbar:
            assert isinstance(child, Track)
            child.verify_metadata()
            Printer.refresh_all_pbars(pbar_stack)
