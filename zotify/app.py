from argparse import Namespace
import time
from librespot.audio.decoders import AudioQuality
from pathlib import Path, PurePath

from zotify.album import download_album, download_artist_albums
from zotify.config import Zotify
from zotify.const import (
    TRACK,
    NAME,
    ID,
    ARTIST,
    ARTISTS,
    ITEMS,
    TRACKS,
    EXPLICIT,
    ALBUM,
    ALBUMS,
    OWNER,
    PLAYLIST,
    PLAYLISTS,
    DISPLAY_NAME,
    USER_FOLLOWED_ARTISTS_URL,
    USER_SAVED_TRACKS_URL,
    SEARCH_URL,
    TRACK_BULK_URL,
)
from zotify.playlist import (
    get_playlist_info,
    download_from_user_playlist,
    download_playlist,
)
from zotify.podcast import download_episode, download_show
from zotify.termoutput import Printer, PrintChannel
from zotify.track import download_track, update_track_metadata
from zotify.utils import (
    split_sanitize_intrange,
    regex_input_for_urls,
    walk_directory_for_tracks,
    get_archived_entries,
)


def download_from_urls(urls: list[str]) -> int:
    """Downloads from a list of urls"""
    download = 0

    pos = 7
    pbar = Printer.pbar(
        urls, unit="url", pos=pos, disable=not Zotify.CONFIG.get_show_url_pbar()
    )
    pbar_stack = [pbar]

    for url in pbar:
        result = regex_input_for_urls(url)
        if all({res is None for res in result}):
            continue

        track_id, album_id, playlist_id, episode_id, show_id, artist_id = result
        if track_id is not None:
            download_track("single", track_id, None, pbar_stack)
        elif album_id is not None:
            download_album(album_id, pbar_stack)
        elif playlist_id is not None:
            download_playlist(
                {ID: playlist_id, NAME: get_playlist_info(playlist_id)[0]}, pbar_stack
            )
        elif episode_id is not None:
            download_episode(episode_id, pbar_stack)
        elif show_id is not None:
            download_show(show_id, pbar_stack)
        elif artist_id is not None:
            download_artist_albums(artist_id, pbar_stack)

        download += 1
        Printer.refresh_all_pbars(pbar_stack)

    return download


def search(search_term) -> None:
    """Searches download server's API for relevant data"""
    params = {
        "limit": "10",
        "offset": "0",
        "q": search_term,
        "type": "track,album,artist,playlist",
    }

    # Parse args
    splits = search_term.split()
    for split in splits:
        index = splits.index(split)

        if split[0] == "-" and len(split) > 1:
            if len(splits) - 1 == index:
                raise IndexError(
                    "No parameters passed after option: {}\n".format(split)
                )

        if split == "-l" or split == "-limit":
            try:
                int(splits[index + 1])
            except ValueError:
                raise ValueError(
                    "Parameter passed after {} option must be an integer.\n".format(
                        split
                    )
                )
            if int(splits[index + 1]) > 50:
                raise ValueError("Invalid limit passed. Max is 50.\n")
            params["limit"] = splits[index + 1]

        if split == "-t" or split == "-type":

            allowed_types = ["track", "playlist", "album", "artist"]
            passed_types = []
            for i in range(index + 1, len(splits)):
                if splits[i][0] == "-":
                    break

                if splits[i] not in allowed_types:
                    raise ValueError(
                        "Parameters passed after {} option must be from this list:\n{}".format(
                            split, "\n".join(allowed_types)
                        )
                    )

                passed_types.append(splits[i])
            params["type"] = ",".join(passed_types)

    if len(params["type"]) == 0:
        params["type"] = "track,album,artist,playlist"

    # Clean search term
    search_term_list = []
    for split in splits:
        if split[0] == "-":
            break
        search_term_list.append(split)
    if not search_term_list:
        raise ValueError("Invalid query.")
    params["q"] = " ".join(search_term_list)

    resp = Zotify.invoke_url_with_params(SEARCH_URL, **params)

    counter = 1
    search_results = []

    total_tracks = 0
    if TRACK in params["type"].split(","):
        tracks = resp[TRACKS][ITEMS]
        if len(tracks) > 0:
            track_data = []
            for track in tracks:
                if track[EXPLICIT]:
                    explicit = "[E]"
                else:
                    explicit = ""

                track_data.append(
                    [
                        counter,
                        f"{track[NAME]} {explicit}",
                        ",".join([artist[NAME] for artist in track[ARTISTS]]),
                    ]
                )
                search_results.append(
                    {
                        ID: track[ID],
                        NAME: track[NAME],
                        "type": TRACK,
                    }
                )
                counter += 1
            total_tracks = counter - 1
            Printer.table("TRACKS", ("ID", "Name", "Artists"), track_data)
            del tracks
            del track_data

    total_albums = 0
    if ALBUM in params["type"].split(","):
        albums = resp[ALBUMS][ITEMS]
        if len(albums) > 0:
            album_data = []
            for album in albums:
                album_data.append(
                    [
                        counter,
                        album[NAME],
                        ",".join([artist[NAME] for artist in album[ARTISTS]]),
                    ]
                )
                search_results.append(
                    {
                        ID: album[ID],
                        NAME: album[NAME],
                        "type": ALBUM,
                    }
                )

                counter += 1
            total_albums = counter - total_tracks - 1
            Printer.table("ALBUMS", ("ID", "Album", "Artists"), album_data)
            del albums
            del album_data

    total_artists = 0
    if ARTIST in params["type"].split(","):
        artists = resp[ARTISTS][ITEMS]
        if len(artists) > 0:
            artist_data = []
            for artist in artists:
                artist_data.append([counter, artist[NAME]])
                search_results.append(
                    {
                        ID: artist[ID],
                        NAME: artist[NAME],
                        "type": ARTIST,
                    }
                )
                counter += 1
            total_artists = counter - total_tracks - total_albums - 1
            Printer.table("ARTISTS", ("ID", "Name"), artist_data)
            del artists
            del artist_data

    total_playlists = 0
    if PLAYLIST in params["type"].split(","):
        playlists = resp[PLAYLISTS][ITEMS]
        if len(playlists) > 0:
            playlist_data = []
            for playlist in playlists:
                playlist_data.append(
                    [counter, playlist[NAME], playlist[OWNER][DISPLAY_NAME]]
                )
                search_results.append(
                    {
                        ID: playlist[ID],
                        NAME: playlist[NAME],
                        "type": PLAYLIST,
                    }
                )
                counter += 1
            total_playlists = counter - total_artists - total_tracks - total_albums - 1
            Printer.table("PLAYLISTS", ("ID", "Name", "Owner"), playlist_data)
            del playlists
            del playlist_data

    if total_tracks + total_albums + total_artists + total_playlists == 0:
        Printer.hashtaged(PrintChannel.MANDATORY, "NO RESULTS FOUND - EXITING...")
        return

    Printer.search_select()
    choices = split_sanitize_intrange(Printer.get_input("ID(s): "))

    if choices == [0]:
        return

    pos = 7
    pbar = Printer.pbar(
        choices, unit="choice", pos=pos, disable=not Zotify.CONFIG.get_show_url_pbar()
    )
    pbar_stack = [pbar]

    for choice in pbar:
        if choice > len(search_results):
            continue

        selection = search_results[choice - 1]
        if selection["type"] == TRACK:
            download_track("single", selection[ID], None, pbar_stack)
        elif selection["type"] == ALBUM:
            download_album(selection[ID], pbar_stack)
        elif selection["type"] == ARTIST:
            download_artist_albums(selection[ID], pbar_stack)
        else:
            download_playlist(selection, pbar_stack)
        Printer.refresh_all_pbars(pbar_stack)


def client(args: Namespace) -> None:
    """Connects to download server to perform query's and get songs to download"""

    Printer.clear()
    Zotify(args)

    time.sleep(2)
    Printer.clear()
    print("\n\n")
    Printer.new_print(
        PrintChannel.MANDATORY,
        "    ███████╗ ██████╗ ████████╗██╗███████╗██╗   ██╗"
        + "\n"
        + "    ╚══███╔╝██╔═══██╗╚══██╔══╝██║██╔════╝╚██╗ ██╔╝"
        + "\n"
        + "      ███╔╝ ██║   ██║   ██║   ██║█████╗   ╚████╔╝ "
        + "\n"
        + "     ███╔╝  ██║   ██║   ██║   ██║██╔══╝    ╚██╔╝  "
        + "\n"
        + "    ███████╗╚██████╔╝   ██║   ██║██║        ██║   "
        + "\n"
        + "    ╚══════╝ ╚═════╝    ╚═╝   ╚═╝╚═╝        ╚═╝   "
        + "\n",
        skip_toggle=True,
    )
    print("\n")

    quality_options = {
        "auto": AudioQuality.VERY_HIGH if Zotify.check_premium() else AudioQuality.HIGH,
        "normal": AudioQuality.NORMAL,
        "high": AudioQuality.HIGH,
        "very_high": AudioQuality.VERY_HIGH,
    }
    Zotify.DOWNLOAD_QUALITY = quality_options[Zotify.CONFIG.get_download_quality()]

    if args.file_of_urls:
        urls: list[str] = []
        filename: str = args.file_of_urls
        if Path(filename).exists():
            with open(filename, "r", encoding="utf-8") as file:
                urls.extend([line.strip() for line in file.readlines()])

            download_from_urls(urls)

        else:
            Printer.hashtaged(PrintChannel.ERROR, f"FILE {filename} NOT FOUND")

    elif args.urls:
        if len(args.urls) > 0:
            if len(args.urls) == 1 and " " in args.urls[0]:
                args.urls = args.urls[0].split(" ")
            download_from_urls(args.urls)

    elif args.playlist:
        download_from_user_playlist()

    elif args.liked_songs:

        liked_songs = Zotify.invoke_url_nextable(USER_SAVED_TRACKS_URL, ITEMS)
        pos = 3
        pbar = Printer.pbar(
            liked_songs,
            unit="song",
            pos=pos,
            disable=not Zotify.CONFIG.get_show_playlist_pbar(),
        )
        pbar_stack = [pbar]

        for song in pbar:
            if not song[TRACK][NAME] or not song[TRACK][ID]:
                Printer.hashtaged(
                    PrintChannel.SKIPPING,
                    "SONG NO LONGER EXISTS\n"
                    + f"Track_Name: {song[TRACK][NAME]} - Track_ID: {song[TRACK][ID]}",
                )
            else:
                download_track("liked", song[TRACK][ID], None, pbar_stack)
                pbar.set_description(song[TRACK][NAME])
                Printer.refresh_all_pbars(pbar_stack)

    elif args.followed_artists:
        followed_artists = Zotify.invoke_url_nextable(
            USER_FOLLOWED_ARTISTS_URL, ITEMS, stripper=ARTISTS
        )
        pos = 7
        pbar = Printer.pbar(
            followed_artists,
            unit="artist",
            pos=pos,
            disable=not Zotify.CONFIG.get_show_url_pbar(),
        )
        pbar_stack = [pbar]

        for artist in pbar:
            download_artist_albums(artist[ID], pbar_stack)
            pbar.set_description(artist[NAME])
            Printer.refresh_all_pbars(pbar_stack)

    elif args.search:
        if args.search == " ":
            search(Printer.get_input("Enter search: "))
        else:
            # this seems unnecessay, but the original code had this check so it gets to live another day
            if regex_input_for_urls(args.search, non_global=True) != (
                None,
                None,
                None,
                None,
                None,
                None,
            ):
                Printer.hashtaged(
                    PrintChannel.WARNING,
                    "URL DETECTED IN SEARCH, TREATING SEARCH AS URL REQUEST",
                )
                download_from_urls([args.search])
            else:
                search(args.search)

    elif args.verify_library:
        # ONLY WORKS WITH ARCHIVED TRACKS (THEORETICALLY GUARANTEES BULK_URL TO WORK)
        archived_tracks = get_archived_entries()
        archived_ids = [entry.strip().split("\t")[0] for entry in archived_tracks]
        archived_filenames = [
            PurePath(entry.strip().split("\t")[4]).stem for entry in archived_tracks
        ]

        track_paths: list[Path] = []
        track_ids: list[str] = []
        library = walk_directory_for_tracks(Zotify.CONFIG.get_root_path())
        for entry in library:
            if entry.stem in archived_filenames:
                track_paths.append(entry)
                track_ids.append(archived_ids[archived_filenames.index(entry.stem)])

        tracks = Zotify.invoke_url_bulk(TRACK_BULK_URL, track_ids, TRACKS)

        pos = 1
        pbar = Printer.pbar(
            track_paths,
            unit="tracks",
            pos=pos,
            disable=not Zotify.CONFIG.get_show_url_pbar(),
        )
        for i, track_path in enumerate(pbar):
            update_track_metadata(track_ids[i], track_path, tracks[i])

    else:
        search(Printer.get_input("Enter search: "))

    Printer.debug(f"Total API Calls: {Zotify.TOTAL_API_CALLS}")
