from argparse import Namespace, Action
from pathlib import Path

from zotify.config import Zotify
from zotify.const import *
from zotify.termoutput import Printer, PrintChannel
from zotify.utils import bulk_regex_urls, clamp, select


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
        if   k == TYPE:                fv = ",".join([t for t in v[-1].split() if t in item_types])
        elif k == SEARCH_QUERY_SIZE:   fv = str(clamp(0, int(v[-1]), max_offset + max_limit))
        elif k == OFFSET:              fv = str(clamp(0, int(v[-1]), max_offset            ))
        elif k == INCLUDE_EXTERNAL:    fv = "audio" if v[-1].lower() == "true" else ""
        else:                          fv = v[-1]
        if fv:     search_filters[k] = fv
        else:  del search_filters[k]
    
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
    items = Zotify.invoke_url_nextable(url, stop=stop, params=params,
                                       stripper=tuple(t for t in table_headers if t[:-1] in params[TYPE]))
    
    search_result_uris = []
    for item_type, headers in table_headers.items():
        if item_type not in items or not len(items[item_type]): continue
        resps: list[dict] = [i for i in items[item_type] if i is not None]
        counter = len(search_result_uris) + 1
        if   item_type == TRACKS:
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
        if   item_type == EPISODES:
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


def search_and_select(search: str = ""):
    """ Perform search Queries and allow user to select results """
    
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
    
    from zotify.api import Query
    uris: list[str] = select(search_result_uris)
    Query(Zotify.DATETIME_LAUNCH).request(' '.join(uris)).execute()


def perform_query(args: Namespace) -> None:
    """ Perform Query according to type """
    from zotify.api import Query, LikedSong, UserPlaylist, FollowedArtist, SavedAlbum, VerifyLibrary
    
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
        
        elif Zotify.CONFIG.get_bypass_metadata():
            Printer.hashtaged(PrintChannel.MANDATORY, 'METADATA BYPASS ENABLED - NON-URL MODES NON-FUNCTIONAL')
            return
        
        elif args.liked_songs:
            LikedSong(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.user_playlists:
            UserPlaylist(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.followed_artists:
            FollowedArtist(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.followed_albums:
            SavedAlbum(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.verify_library:
            VerifyLibrary(Zotify.DATETIME_LAUNCH).execute()
        
        elif args.search:
            search_and_select(args.search)
        
        else:
            search_and_select()
    
    except BaseException as e:
        Zotify.cleanup()
        raise e


def client(args: Namespace, modes: list[Action]) -> None:
    """ Perform Queries as needed """
    
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
        ask_mode = False
        mode_data = [[i+1, mode.dest.upper().replace('_', ' ')] for i, mode in enumerate(modes)]
        Printer.table("Modes", ("ID", "MODE"), [[0, "EXIT"]] + mode_data)
        try:
            selected_mode: Action | None = select([None] + modes, inline_prompt="MODE SELECTION: ", first_ID=0, only_one=True)[0]
        except KeyboardInterrupt:
            selected_mode = None
        
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
            mode_args = Printer.get_input(f"\nMODE ARGUMENTS ({selected_mode.dest.upper().replace('_', ' ')}): ")
            setattr(args, selected_mode.dest, mode_args)
        else:
            setattr(args, selected_mode.dest, True)
        
        Zotify.start()
        perform_query(args)
    
    Zotify.cleanup()
