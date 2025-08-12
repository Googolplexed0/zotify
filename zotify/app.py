from argparse import Namespace
from pathlib import Path

from zotify.api import Query, LikedSongs, FollowedArtists, VerifyLibrary, fetch_search_display, fetch_user_playlists_display
from zotify.config import Zotify
from zotify.termoutput import Printer, PrintChannel
from zotify.utils import bulk_regex_urls, select


def search_and_select(search: str = ""):
    while not search or search == ' ':
        search = Printer.get_input('Enter search: ')
    
    if any(bulk_regex_urls(search)):
        Printer.hashtaged(PrintChannel.WARNING, 'URL DETECTED IN SEARCH, TREATING SEARCH AS URL REQUEST')
        q = Query(Zotify.DATETIME_LAUNCH).request(search)
        q.execute()
        return
    
    search_result_uris = fetch_search_display(search)
    
    if not search_result_uris:
        Printer.hashtaged(PrintChannel.MANDATORY, 'NO RESULTS FOUND - EXITING...')
        return
    
    uris: list[str] = select(search_result_uris)
    q = Query(Zotify.DATETIME_LAUNCH).request(' '.join(uris))
    q.execute()


def client(args: Namespace) -> None:
    """ Connects to download server to perform query's and get songs to download """
    Zotify(args)
    Printer.splash()
    
    if args.file_of_urls:
        if Path(args.file_of_urls).exists():
            urls: list[str] = []
            with open(args.file_of_urls, 'r', encoding='utf-8') as file:
                urls.extend([line.strip() for line in file.readlines()])
            q = Query(Zotify.DATETIME_LAUNCH).request(' '.join(urls))
            q.execute()
        else:
            Printer.hashtaged(PrintChannel.ERROR, f'FILE {args.file_of_urls} NOT FOUND')
    
    elif args.urls:
        if len(args.urls) > 0:
            q = Query(Zotify.DATETIME_LAUNCH).request(args.urls)
            q.execute()
    
    elif args.playlist:
        user_playlist_uris = fetch_user_playlists_display()
        uris: list[str] = select(user_playlist_uris)
        q = Query(Zotify.DATETIME_LAUNCH).request(' '.join(uris))
        q.execute()
    
    elif args.liked_songs:
        LikedSongs(Zotify.DATETIME_LAUNCH).execute()
    
    elif args.followed_artists:
        FollowedArtists(Zotify.DATETIME_LAUNCH).execute()
    
    elif args.search:
        search_and_select(args.search)
    
    elif args.verify_library:
        VerifyLibrary(Zotify.DATETIME_LAUNCH).execute()
    
    else:
        search_and_select()
    
    Printer.debug(f"Total API Calls: {Zotify.TOTAL_API_CALLS}")
