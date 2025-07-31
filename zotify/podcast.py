import time
import ffmpy
import subprocess
from pathlib import PurePath, Path
from librespot.metadata import EpisodeId

from zotify.const import EPISODE_INFO_URL, SHOWS_URL, PARTNER_URL, PERSISTED_QUERY, ERROR, ID, ITEMS, NAME, SHOW, DURATION_MS, EXT_MAP
from zotify.termoutput import PrintChannel, Printer, Loader
from zotify.utils import create_download_directory, fix_filename, fmt_duration, wait_between_downloads
from zotify.zotify import Zotify


def get_episode_info(episode_id: str) -> tuple[str | None, str | None, str | None]:
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching episode information..."):
        (raw, resp) = Zotify.invoke_url(f'{EPISODE_INFO_URL}/{episode_id}')
    if not resp:
        Printer.hashtaged(PrintChannel.ERROR, 'INVALID EPISODE ID')
    if ERROR in resp:
        return None, None, None
    duration_ms = resp[DURATION_MS]
    return fix_filename(resp[SHOW][NAME]), duration_ms, fix_filename(resp[NAME])


def get_show_episode_ids(show_id: str) -> list:
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching episodes..."):
        episodes = Zotify.invoke_url_nextable(f'{SHOWS_URL}/{show_id}/episodes', ITEMS)
    return [episode[ID] for episode in episodes]


def download_podcast_directly(url, filename):
    import functools
    import shutil
    import requests
    from tqdm.auto import tqdm
    
    r = requests.get(url, stream=True, allow_redirects=True)
    if r.status_code != 200:
        r.raise_for_status()  # Will only raise for 4xx codes, so...
        raise RuntimeError(
            f"Request to {url} returned status code {r.status_code}")
    file_size = int(r.headers.get('Content-Length', 0))
    
    path = Path(filename).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    
    desc = "(Unknown total file size)" if file_size == 0 else ""
    r.raw.read = functools.partial(
        r.raw.read, decode_content=True)  # Decompress if needed
    with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
        with path.open("wb") as f:
            shutil.copyfileobj(r_raw, f)
    
    return path


def download_show(show_id, pbar_stack: list | None = None):
    episode_ids = get_show_episode_ids(show_id)
    
    pos, pbar_stack = Printer.pbar_position_handler(3, pbar_stack)
    pbar = Printer.pbar(episode_ids, unit='episode', pos=pos,
                        disable=not Zotify.CONFIG.get_show_playlist_pbar())
    pbar_stack.append(pbar)
    
    for episode in pbar:
        download_episode(episode, pbar_stack)
        pbar.set_description(get_episode_info(episode)[2])
        Printer.refresh_all_pbars(pbar_stack)


def download_episode(episode_id, pbar_stack: list | None = None) -> None:
    
    podcast_name, duration_ms, episode_name = get_episode_info(episode_id)
    
    if podcast_name is None or episode_name is None or duration_ms is None:
        Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING EPISODE - FAILED TO QUERY METADATA\n' +\
                                             f'Episode_ID: {str(episode_id)}')
        wait_between_downloads(); return
    
    if Zotify.CONFIG.get_regex_episode():
        regex_match = Zotify.CONFIG.get_regex_episode().search(episode_name)
        if regex_match:
            Printer.hashtaged(PrintChannel.SKIPPING, 'EPISODE MATCHES REGEX FILTER\n' +\
                                                    f'Episode_Name: {episode_name} - Episode_ID: {episode_id}\n'+\
                                                   (f'Regex Groups: {regex_match.groupdict()}' if regex_match.groups() else ""))
            wait_between_downloads(); return
    
    with Loader(PrintChannel.PROGRESS_INFO, "Preparing download..."):
        filename = f"{podcast_name} - {episode_name}"
        episode_path = PurePath(Zotify.CONFIG.get_root_podcast_path()) / podcast_name / f"{filename}.tmp"
        create_download_directory(episode_path.parent)
        
        (raw, resp) = Zotify.invoke_url(PARTNER_URL + episode_id + '"}&extensions=' + PERSISTED_QUERY)
        direct_download_url = resp["data"]["episode"]["audio"]["items"][-1]["url"]
        
        if "anon-podcast.scdn.co" in direct_download_url or "audio_preview_url" not in resp:
            episode_id = EpisodeId.from_base62(episode_id)
            stream = Zotify.get_content_stream(episode_id, Zotify.DOWNLOAD_QUALITY)
            
            if stream is None:
                Printer.hashtaged(PrintChannel.ERROR, 'SKIPPING EPISODE - FAILED TO GET CONTENT STREAM\n' +\
                                                        f'Episode_ID: {str(episode_id)}')
                wait_between_downloads(); return
            
            total_size: int = stream.input_stream.size
            episode_path_exists = Path(episode_path).is_file() and Path(episode_path).stat().st_size == total_size
            if episode_path_exists and Zotify.CONFIG.get_skip_existing():
                Printer.hashtaged(PrintChannel.SKIPPING, f'"{podcast_name} - {episode_name}" (EPISODE ALREADY EXISTS)')
                wait_between_downloads(); return
            
            time_start = time.time()
            downloaded = 0
            pos, pbar_stack = Printer.pbar_position_handler(1, pbar_stack)
            with open(episode_path, 'wb') as file, Printer.pbar(
                desc=filename,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                disable=not Zotify.CONFIG.get_show_download_pbar(),
                pos=pos
            ) as pbar:
                while True:
                #for _ in range(int(total_size / Zotify.CONFIG.get_chunk_size()) + 2):
                    data = stream.input_stream.stream().read(Zotify.CONFIG.get_chunk_size())
                    pbar.update(file.write(data))
                    downloaded += len(data)
                    if data == b'':
                        break
                    if Zotify.CONFIG.get_download_real_time():
                        delta_real = time.time() - time_start
                        delta_want = (downloaded / total_size) * (int(duration_ms)/1000)
                        if delta_want > delta_real:
                            time.sleep(delta_want - delta_real)
            
            time_dl_end = time.time()
            time_elapsed_dl = fmt_duration(time_dl_end - time_start)
        else:
            # TODO add failure catch for after this function
            download_podcast_directly(direct_download_url, episode_path)
    
    Printer.hashtaged(PrintChannel.DOWNLOADS, f'DOWNLOADED: "{filename}"\n' +\
                                              f'DOWNLOAD TOOK {time_elapsed_dl}')
    
    try:
        with Loader(PrintChannel.PROGRESS_INFO, "Identifying episode audio codec..."):
            ff_m = ffmpy.FFprobe(
                global_options=['-hide_banner', f'-loglevel {Zotify.CONFIG.get_ffmpeg_log_level()}'],
                inputs={episode_path: ["-show_entries", "stream=codec_name"]},
            )
            stdout, _ = ff_m.run(stdout=subprocess.PIPE)
            codec = stdout.decode().strip().split("=")[1].split("\r")[0]
            
            if codec in EXT_MAP:
                suffix = EXT_MAP[codec]
            else:
                # gross, but shouldn't ever happen...
                suffix = codec
            
            episode_path_codec = episode_path.with_suffix(f".{suffix}")
            if Path(episode_path_codec).exists():
                Path(episode_path_codec).unlink()
            Path(episode_path).rename(episode_path_codec)
        
        Printer.debug(f"Detected Codec: {codec}\n" +\
                      f"File Renamed: {episode_path.name}")
    
    except ffmpy.FFExecutableNotFoundError:
        Path(episode_path).rename(episode_path.with_suffix(".mp3"))
        Printer.hashtaged(PrintChannel.WARNING, 'FFMPEG NOT FOUND\n' +\
                                                'SKIPPING CODEC ANALYSIS - OUTPUT ASSUMED MP3')
    
    wait_between_downloads()
