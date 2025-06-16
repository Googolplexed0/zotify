import json
import datetime
import requests
from time import sleep
from pathlib import Path
from librespot.audio.decoders import VorbisOnlyAudioQuality

from zotify import OAuth, Session
from zotify.const import TYPE, \
    PREMIUM, USER_READ_EMAIL, OFFSET, LIMIT, \
    PLAYLIST_READ_PRIVATE, USER_LIBRARY_READ, USER_FOLLOW_READ
from zotify.config import Config
from zotify.termoutput import Printer, PrintChannel, Loader


class Zotify:    
    SESSION: Session = None
    DOWNLOAD_QUALITY = None
    CONFIG: Config = Config()
    
    def __init__(self, args):
        Zotify.CONFIG.load(args)
        Printer.print(PrintChannel.MANDATORY, "\n\n\n")
        login_loader = Loader(PrintChannel.MANDATORY, "Logging in...", end="\n")
        login_loader.start()
        Zotify.login(args)
        login_loader.stop()
        Zotify.datetime_launch = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    @classmethod
    def login(cls, args):
        """ Authenticates and saves credentials to a file """
        # Create session
        if args.username not in {None, ""} and args.token not in {None, ""}:
            oauth = OAuth(args.username, *cls.CONFIG.get_oauth_addresses())
            oauth.set_token(args.token, OAuth.RequestType.REFRESH)
            cls.SESSION = Session.from_oauth(
                oauth, cls.CONFIG.get_credentials_location(), cls.CONFIG.get_language()
            )
        elif cls.CONFIG.get_credentials_location() and Path(cls.CONFIG.get_credentials_location()).exists():
            cls.SESSION = Session.from_file(
                cls.CONFIG.get_credentials_location(),
                cls.CONFIG.get_language(),
            )
        else:
            username = args.username
            while username == "":
                username = input("Username: ")
            oauth = OAuth(username, *cls.CONFIG.get_oauth_addresses())
            auth_url = oauth.auth_interactive()
            Printer.print(PrintChannel.MANDATORY, f"Click on the following link to login:\n{auth_url}")
            cls.SESSION = Session.from_oauth(
                oauth, cls.CONFIG.get_credentials_location(), cls.CONFIG.get_language()
            )
    
    @classmethod
    def get_content_stream(cls, content_id, quality):
        try:
            return cls.SESSION.content_feeder().load(content_id, VorbisOnlyAudioQuality(quality), False, None)
        except RuntimeError as e:
            if 'Failed fetching audio key!' in e.args[0]:
                gid, fileid = e.args[0].split('! ')[1].split(', ')
                Printer.print(PrintChannel.ERRORS, '###   ERROR:  FAILED TO FETCH AUDIO KEY   ###\n' +\
                                                   '###   MAY BE CAUSED BY RATE LIMITS - CONSIDER INCREASING `BULK_WAIT_TIME`   ###\n' +\
                                                  f'###   GID: {gid[5:]} - File_ID: {fileid[8:]}   ###')
            else:
                raise e
    
    @classmethod
    def __get_auth_token(cls):
        return cls.SESSION.tokens().get_token(
            USER_READ_EMAIL, PLAYLIST_READ_PRIVATE, USER_LIBRARY_READ, USER_FOLLOW_READ
        ).access_token
    
    @classmethod
    def get_auth_header(cls):
        return {
            'Authorization': f'Bearer {cls.__get_auth_token()}',
            'Accept-Language': f'{cls.CONFIG.get_language()}',
            'Accept': 'application/json',
            'app-platform': 'WebPlayer',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0'
        }
    
    @classmethod
    def invoke_url_with_params(cls, url, limit, offset, **kwargs):
        headers = cls.get_auth_header()
        params = {LIMIT: limit, OFFSET: offset}
        params.update(kwargs)
        return requests.get(url, headers=headers, params=params).json()
    
    @classmethod
    def invoke_url(cls, url: str, tryCount: int = 0):
        headers = cls.get_auth_header()
        response = requests.get(url, headers=headers)
        responsetext = response.text
        try:
            responsejson = response.json()
            # responsejson = {"error": {"status": "Unknown", "message": "Received an empty response"}}
        except json.decoder.JSONDecodeError:
            responsejson = {"error": {"status": "Unknown", "message": "Received an empty response"}}
        
        if not responsejson or 'error' in responsejson:
            if tryCount < cls.CONFIG.get_retry_attempts():
                Printer.print(PrintChannel.WARNINGS, f"###   WARNING:  API ERROR (TRY {tryCount}) - RETRYING   ###\n" +\
                                                     f"###   {responsejson['error']['status']}: {responsejson['error']['message']}")
                sleep(5)
                return cls.invoke_url(url, tryCount + 1)
            
            Printer.print(PrintChannel.API_ERRORS, f"###   API ERROR:  API ERROR (TRY {tryCount}) - RETRY LIMIT EXCEDED   ###\n" +\
                                                   f"###   {responsejson['error']['status']}: {responsejson['error']['message']}")
        
        return responsetext, responsejson
    
    @classmethod
    def check_premium(cls) -> bool:
        return (cls.SESSION.get_user_attribute(TYPE) == PREMIUM)
