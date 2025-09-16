from argparse import Namespace, Action
from pathlib import Path

from zotify.api import Query, LikedSongs, UserPlaylists, FollowedArtists, VerifyLibrary, fetch_search_display
from zotify.config import Zotify
from zotify.termoutput import Printer, PrintChannel
from zotify.utils import bulk_regex_urls, select


def search_and_select(search: str = ""):
    while not search or search == ' ':
        search = Printer.get_input('Enter search: ')
    
    if any(bulk_regex_urls(search)):
        Printer.hashtaged(PrintChannel.WARNING, 'URL DETECTED IN SEARCH, TREATING SEARCH AS URL REQUEST')
        Query(Zotify.DATETIME_LAUNCH).request(search).execute()
        return
    
    search_result_uris = fetch_search_display(search)
    
    if not search_result_uris:
        Printer.hashtaged(PrintChannel.MANDATORY, 'NO RESULTS FOUND - EXITING...')
        return
    
    uris: list[str] = select(search_result_uris)
    Query(Zotify.DATETIME_LAUNCH).request(' '.join(uris)).execute()


def perform_query(args: Namespace) -> None:
    """ Connects to download server to perform query """
    try:
        if args.urls or args.file_of_urls:
            urls = ""
            if args.urls:
                urls: str = args.urls
            elif args.file_of_urls:
                if Path(args.file_of_urls).exists():
                    with open(args.file_of_urls, 'r', encoding='utf-8') as file:
                        urls = " ".join([line.strip() for line in file.readlines()])
                else:
                    Printer.hashtaged(PrintChannel.ERROR, f'FILE {args.file_of_urls} NOT FOUND')
            
            if len(urls) > 0:
                Query(Zotify.DATETIME_LAUNCH).request(urls).execute()
        
        elif args.liked_songs:
            LikedSongs(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.followed_artists:
            FollowedArtists(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.playlists:
            UserPlaylists(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.verify_library:
            VerifyLibrary(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.search:
            search_and_select(args.search)
        
        else:
            search_and_select()
        
        Printer.debug(f"Total API Calls: {Zotify.TOTAL_API_CALLS}")
        Zotify.TOTAL_API_CALLS = 0
    
    except BaseException as e:
        Printer.debug(f"Total API Calls: {Zotify.TOTAL_API_CALLS}")
        Zotify.cleanup()
        print("\n")
        raise e


def client(args: Namespace, modes: list[Action]) -> None:
    """ Loads config, creates Session, and performs queries as needed """
    Zotify(args)
    Printer.splash()
    
    ask_mode = False
    if any([getattr(args, mode.dest) for mode in modes]):
        perform_query(args)
    else:
        if not args.persist:
            # this maintains current behavior when no mode/url present
            Printer.hashtaged(PrintChannel.MANDATORY, "NO MODE SELECTED, DEFAULTING TO SEARCH")
            perform_query(args)
            
            # TODO: decide if this alt behavior should be implemented
            # Printer.hashtaged(PrintChannel.MANDATORY, "NO MODE SELECTED, PLEASE SELECT ONE")
            # ask_mode = True
    
    while args.persist or ask_mode:
        mode_data = [[i+1, mode.dest.upper().replace("_", " ")] for i, mode in enumerate(modes)]
        Printer.table("Modes", ("ID", "MODE"), mode_data + [[0, "EXIT"]])
        selected_mode: Action | None = select(modes + [None], get_input_prompt="MODE SELECTION: ", first_ID=0)[-1]
        ask_mode = False
        
        if selected_mode is None:
            Printer.hashtaged(PrintChannel.MANDATORY, "CLOSING SESSION")
            break
        
        # clear previous run modes
        for mode in modes:
            if mode.nargs:
                setattr(args, mode.dest, None)
            else:
                setattr(args, mode.dest, False)
        
        # set new mode
        if selected_mode.nargs:
            mode_args = Printer.get_input(f"\nMODE ARGUMENTS ({mode.dest.upper().replace("_", " ")}): ")
            setattr(args, mode.dest, mode_args)
        else:
            setattr(args, mode.dest, True)
        
        perform_query(args)
    
    Zotify.cleanup()
    print("\n")
