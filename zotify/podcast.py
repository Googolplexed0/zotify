import time
from pathlib import PurePath, Path
from librespot.metadata import EpisodeId

from zotify.const import EPISODE_INFO_URL, SHOWS_URL, PARTNER_URL, PERSISTED_QUERY, ERROR, ID, ITEMS, NAME, SHOW, DURATION_MS
from zotify.termoutput import PrintChannel, Printer, Loader
from zotify.utils import create_download_directory, fix_filename, fmt_seconds, wait_between_downloads
from zotify.zotify import Zotify


def get_episode_info(episode_id_str) -> tuple[str | None, str | None, str | None]:
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching episode information..."):
        (raw, info) = Zotify.invoke_url(f'{EPISODE_INFO_URL}/{episode_id_str}')
    if not info:
        Printer.print(PrintChannel.ERRORS, "###   ERROR:  INVALID EPISODE ID   ###")
    if ERROR in info:
        return None, None, None
    duration_ms = info[DURATION_MS]
    return fix_filename(info[SHOW][NAME]), duration_ms, fix_filename(info[NAME])


def get_show_episodes(show_id_str) -> list:
    episodes = []
    offset = 0
    limit = 50
    
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching episodes..."):
        while True:
            resp = Zotify.invoke_url_with_params(
                f'{SHOWS_URL}/{show_id_str}/episodes', limit=limit, offset=offset)
            offset += limit
            for episode in resp[ITEMS]:
                episodes.append(episode[ID])
            if len(resp[ITEMS]) < limit:
                break
    
    return episodes


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
    episodes = get_show_episodes(show_id)
    
    pos, pbar_stack = Printer.pbar_position_handler(3, pbar_stack)
    pbar = Printer.pbar(episodes, unit='episode', pos=pos,
                        disable=not Zotify.CONFIG.get_show_playlist_pbar())
    pbar_stack.append(pbar)
    
    for episode in pbar:
        download_episode(episode, pbar_stack)
        pbar.set_description(get_episode_info(episode)[2])
        Printer.refresh_all_pbars(pbar_stack)


def download_episode(episode_id, pbar_stack: list | None = None) -> None:
    podcast_name, duration_ms, episode_name = get_episode_info(episode_id)
    
    Printer.print(PrintChannel.MANDATORY, "\n")
    prepare_download_loader = Loader(PrintChannel.PROGRESS_INFO, "Preparing download...")
    prepare_download_loader.start()
    
    if podcast_name is None or episode_name is None or duration_ms is None:
        prepare_download_loader.stop()
        Printer.print(PrintChannel.ERRORS, '###   ERROR:  SKIPPING EPISODE - FAILED TO QUERY METADATA   ###\n' +\
                                          f'###   Episode_ID: {str(episode_id)}   ###')
    else:
        filename = podcast_name + ' - ' + episode_name
        extra_paths = podcast_name + '/'
        
        resp = Zotify.invoke_url(
            PARTNER_URL + episode_id + '"}&extensions=' + PERSISTED_QUERY)[1]["data"]["episode"]
        direct_download_url = resp["audio"]["items"][-1]["url"]
        
        download_directory = PurePath(Zotify.CONFIG.get_root_podcast_path()).joinpath(extra_paths)
        create_download_directory(download_directory)
        
        if "anon-podcast.scdn.co" in direct_download_url or "audio_preview_url" not in resp:
            episode_id = EpisodeId.from_base62(episode_id)
            stream = Zotify.get_content_stream(episode_id, Zotify.DOWNLOAD_QUALITY)
            
            if stream is None:
                Printer.print(PrintChannel.ERRORS, '###   ERROR:  SKIPPING EPISODE - FAILED TO GET CONTENT STREAM   ###\n' +\
                                                  f'###   Episode_ID: {str(episode_id)}   ###')
            
            else:
                total_size = stream.input_stream.size
                
                filepath = PurePath(download_directory).joinpath(f"{filename}.ogg")
                if (Path(filepath).is_file()
                    and Path(filepath).stat().st_size == total_size
                    and Zotify.CONFIG.get_skip_existing()
                ):
                    prepare_download_loader.stop()
                    Printer.print(PrintChannel.SKIPS, f'###   SKIPPING:  "{podcast_name} - {episode_name}" (EPISODE ALREADY EXISTS)   ###')
                    return
                prepare_download_loader.stop()
                time_start = time.time()
                downloaded = 0
                pos, pbar_stack = Printer.pbar_position_handler(1, pbar_stack)
                with open(filepath, 'wb') as file, Printer.pbar(
                    desc=filename,
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    disable=not Zotify.CONFIG.get_show_download_pbar(),
                    pos=pos
                ) as pbar:
                    prepare_download_loader.stop()
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
                time_elapsed_dl = fmt_seconds(time_dl_end - time_start)
                
                Printer.print(PrintChannel.DOWNLOADS, f'###   DOWNLOADED: "{Path(filename).relative_to(Zotify.CONFIG.get_root_path())}"   ###\n' +\
                                                      f'###   DOWNLOAD TOOK {time_elapsed_dl}   ###')
                
                wait_between_downloads()
        else:
            filepath = PurePath(download_directory).joinpath(f"{filename}.mp3")
            download_podcast_directly(direct_download_url, filepath)
            
            wait_between_downloads()
    
    prepare_download_loader.stop()
