from zotify.const import USER_PLAYLISTS_URL, PLAYLISTS_URL, ITEMS, ID, TRACK, NAME, TYPE, TRACKS
from zotify.podcast import download_episode
from zotify.termoutput import Printer, PrintChannel
from zotify.track import download_track
from zotify.utils import split_sanitize_intrange, strptime_utc
from zotify.zotify import Zotify


def get_playlist_songs(playlist_id: str) -> tuple[list[str], list[dict]]:
    """ returns list of songs in a playlist """
    
    playlist_tracks = Zotify.invoke_url_nextable(f'{PLAYLISTS_URL}/{playlist_id}/{TRACKS}', ITEMS, 100)
    
    playlist_tracks.sort(key=lambda s: strptime_utc(s['added_at']))
    
    # Filter Before Indexing, matches prior behavior
    playlist_tracks = [track_dict[TRACK] if track_dict[TRACK] is not None and track_dict[TRACK][ID] else None for track_dict in playlist_tracks]
    
    char_num = max({len(str(len(playlist_tracks))), 2})
    playlist_num = [str(n+1).zfill(char_num) for n in range(len(playlist_tracks))]
    
    # filtering by added date inverts playlist order, ruining the .m3u8 file, so skip if exporting m3u8
    if not Zotify.CONFIG.get_export_m3u8():
        playlist_num.reverse()
        playlist_tracks.reverse()
    
    # Filter After Indexing, feels more safe
    # for i, track_dict in enumerate(playlist_tracks):
    #     if track_dict[TRACK] is None or track_dict[TRACK][ID] is None:
    #         playlist_num.pop(i)
    #         playlist_tracks.pop(i)
    
    return playlist_num, playlist_tracks


def get_playlist_info(playlist_id) -> tuple[str, str]:
    """ Returns information scraped from playlist """
    (raw, resp) = Zotify.invoke_url(f'{PLAYLISTS_URL}/{playlist_id}?fields=name,owner(display_name)&market=from_token')
    return resp['name'].strip(), resp['owner']['display_name'].strip()


def download_playlist(playlist, pbar_stack: list | None = None):
    """Downloads all the songs from a playlist"""
    playlist_num, playlist_songs = get_playlist_songs(playlist[ID])
    
    pos, pbar_stack = Printer.pbar_position_handler(3, pbar_stack)
    pbar = Printer.pbar(playlist_songs, unit='song', pos=pos,
                        disable=not Zotify.CONFIG.get_show_playlist_pbar())
    pbar_stack.append(pbar)
    
    for i, song in enumerate(pbar):
        if song is None:
            continue
        elif song[TYPE] == "episode": # Playlist item is a podcast episode
            pbar.unit = 'episode'
            download_episode(song[ID])
        else:
            pbar.unit = 'song'
            download_track('extplaylist', song[ID],
                           {'playlist_song_name': song[NAME],
                            'playlist': playlist[NAME],
                            'playlist_num': playlist_num[i],
                            'playlist_id': playlist[ID],
                            'playlist_track_id': song[ID]},
                           pbar_stack)
        pbar.set_description(song[NAME])
        Printer.refresh_all_pbars(pbar_stack)


def download_from_user_playlist():
    """ Select which playlist(s) to download """
    
    users_playlists = Zotify.invoke_url_nextable(USER_PLAYLISTS_URL, ITEMS)
    
    Printer.table("PLAYLISTS", ('ID', 'Name'), [ [i+1, playlist[NAME].strip()] for i, playlist in enumerate(users_playlists)])
    Printer.search_select()
    playlist_choices = split_sanitize_intrange(Printer.get_input('ID(s): '))
    
    pos = 5
    pbar = Printer.pbar(playlist_choices, unit='playlist', pos=pos, 
                        disable=not Zotify.CONFIG.get_show_url_pbar())
    pbar_stack = [pbar]
    
    for playlist_number in pbar:
        playlist = users_playlists[int(playlist_number) - 1]
        download_playlist(playlist, pbar_stack)
        pbar.set_description(playlist[NAME].strip())
        Printer.refresh_all_pbars(pbar_stack)
