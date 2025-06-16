from __future__ import annotations
__version__ = "0.7.15"

from enum import IntEnum
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from typing import Any
from time import time_ns
from urllib.parse import urlencode, urlparse, parse_qs

from librespot.audio import AudioKeyManager, CdnManager
from librespot.audio.storage import ChannelManager
from librespot.cache import CacheManager
from librespot.core import (
    ApResolver,
    DealerClient,
    EventService,
    PlayableContentFeeder,
    SearchManager,
    ApiClient as LibrespotApiClient,
    Session as LibrespotSession,
    TokenProvider as LibrespotTokenProvider,
)
from librespot.mercury import MercuryClient
from librespot.proto import Authentication_pb2 as Authentication
from pkce import generate_code_verifier, get_code_challenge
from requests import HTTPError, get, post
from logging import getLogger
getLogger("Librespot:AudioKeyManager").disabled = True


API_URL = "https://api.sp" + "otify.com/v1/"
AUTH_URL = "https://accounts.sp" + "otify.com/"
CLIENT_ID = "65b70807" + "3fc0480e" + "a92a0772" + "33ca87bd"
SCOPES = [
    "app-remote-control",
    "playlist-modify",
    "playlist-modify-private",
    "playlist-modify-public",
    "playlist-read",
    "playlist-read-collaborative",
    "playlist-read-private",
    "streaming",
    "ugc-image-upload",
    "user-follow-modify",
    "user-follow-read",
    "user-library-modify",
    "user-library-read",
    "user-modify",
    "user-modify-playback-state",
    "user-modify-private",
    "user-personalized",
    "user-read-birthdate",
    "user-read-currently-playing",
    "user-read-email",
    "user-read-play-history",
    "user-read-playback-position",
    "user-read-playback-state",
    "user-read-private",
    "user-read-recently-played",
    "user-top-read",
]


class Session(LibrespotSession):
    def __init__(
        self,
        session_builder: LibrespotSession.Builder,
        language: str = "en",
        oauth: OAuth | None = None,
    ) -> None:
        """
        Authenticates user, saves credentials to a file and generates api token.
        Args:
            session_builder: An instance of the Librespot Session builder
            langauge: ISO 639-1 language code
        """
        super(Session, self).__init__(
            LibrespotSession.Inner(
                session_builder.device_type,
                session_builder.device_name,
                session_builder.preferred_locale,
                session_builder.conf,
                session_builder.device_id,
            ),
            ApResolver.get_random_accesspoint(),
        )
        self.__oauth = oauth
        self.__language = language
        self.connect()
        self.authenticate(session_builder.login_credentials)
    
    @staticmethod
    def from_file(cred_file: Path | str, language: str = "en") -> Session:
        """
        Creates session using saved credentials file
        Args:
            cred_file: Path to credentials file
            language: ISO 639-1 language code for API responses
        Returns:
            Zotify session
        """
        if not isinstance(cred_file, Path):
            cred_file = Path(cred_file).expanduser()
        config = (
            LibrespotSession.Configuration.Builder()
            .set_store_credentials(False)
            .build()
        )
        session = LibrespotSession.Builder(config).stored_file(str(cred_file))
        return Session(session, language)
    
    @staticmethod
    def from_oauth(
        oauth: OAuth,
        save_file: Path | str | None = None,
        language: str = "en",
    ) -> Session:
        """
        Creates a session using OAuth2
        Args:
            save_file: Path to save login credentials to, optional.
            language: ISO 639-1 language code for API responses
        Returns:
            Zotify session
        """
        config = LibrespotSession.Configuration.Builder()
        if save_file:
            if not isinstance(save_file, Path):
                save_file = Path(save_file).expanduser()
            save_file.parent.mkdir(parents=True, exist_ok=True)
            config.set_stored_credential_file(str(save_file))
        else:
            config.set_store_credentials(False)

        token = oauth.await_token()

        builder = LibrespotSession.Builder(config.build())
        builder.login_credentials = Authentication.LoginCredentials(
            username=oauth.username,
            typ=Authentication.AuthenticationType.values()[3],
            auth_data=token.access_token.encode(),
        )
        return Session(builder, language, oauth)
    
    def oauth(self) -> OAuth | None:
        """Returns OAuth service"""
        return self.__oauth
    
    def language(self) -> str:
        """Returns session language"""
        return self.__language
    
    def is_premium(self) -> bool:
        """Returns users premium account status"""
        return self.get_user_attribute("type") == "premium"
    
    def authenticate(self, credential: Authentication.LoginCredentials) -> None: # type: ignore
        """
        Log in to the thing
        Args:
            credential: Account login information
        """
        self.__authenticate_partial(credential, False)
        with self.__auth_lock:
            self.__mercury_client = MercuryClient(self)
            self.__token_provider = TokenProvider(self)
            self.__audio_key_manager = AudioKeyManager(self)
            self.__channel_manager = ChannelManager(self)
            self.__api = ApiClient(self)
            self.__cdn_manager = CdnManager(self)
            self.__content_feeder = PlayableContentFeeder(self)
            self.__cache_manager = CacheManager(self)
            self.__dealer_client = DealerClient(self)
            self.__search = SearchManager(self)
            self.__event_service = EventService(self)
            self.__auth_lock_bool = False
            self.__auth_lock.notify_all()
        self.mercury().interested_in("sp" + "otify:user:attributes:update", self)


class ApiClient(LibrespotApiClient):
    def __init__(self, session: Session):
        super(ApiClient, self).__init__(session)
        self.__session = session

    def invoke_url(
        self,
        url: str,
        params: dict[str, Any] = {},
        limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        """
        Requests data from API
        Args:
            url: API URL and to get data from
            params: parameters to be sent in the request
            limit: The maximum number of items in the response
            offset: The offset of the items returned
        Returns:
            Dictionary representation of JSON response
        """
        headers = {
            "Authorization": f"Bearer {self.__get_token()}",
            "Accept": "application/json",
            "Accept-Language": self.__session.language(),
            "app-platform": "WebPlayer",
        }
        params["limit"] = limit
        params["offset"] = offset

        response = get(API_URL + url, headers=headers, params=params)
        data = response.json()

        try:
            raise HTTPError(
                f"{url}\nAPI Error {data['error']['status']}: {data['error']['message']}"
            )
        except KeyError:
            return data

    def __get_token(self) -> str:
        return (
            self.__session.tokens()
            .get_token(
                "playlist-read-private",  # Private playlists
                "user-follow-read",  # Followed artists
                "user-library-read",  # Liked tracks/episodes/etc.
                "user-read-private",  # Country
            )
            .access_token
        )


class TokenProvider(LibrespotTokenProvider):
    def __init__(self, session: Session):
        super(TokenProvider, self).__init__(session)
        self._session = session

    def get_token(self, *scopes) -> TokenProvider.StoredToken:
        oauth = self._session.oauth()
        if oauth is None:
            return super().get_token(*scopes)
        return oauth.get_token()

    class StoredToken(LibrespotTokenProvider.StoredToken):
        def __init__(self, obj):
            self.timestamp = int(time_ns() / 1000)
            self.expires_in = int(obj["expires_in"])
            self.access_token = obj["access_token"]
            self.scopes = obj["scope"].split()
            self.refresh_token = obj["refresh_token"]


class OAuth:
    __code_verifier: str
    __server_thread: Thread
    __token: TokenProvider.StoredToken
    username: str
    
    def __init__(self, username: str, redirect_address: str | None, oauth_address: str | None) -> None:
        self.username = username
        self.port = 4381
        self.oauth_address = oauth_address if oauth_address else "0.0.0.0"
        self.redirect_uri = f"http://{redirect_address if redirect_address else "127.0.0.1"}:{self.port}/login"
    
    def auth_interactive(self) -> str:
        """
        Starts local server for token callback
        Returns:
            OAuth URL
        """
        self.__server_thread = Thread(target=self.__run_server)
        self.__server_thread.start()
        self.__code_verifier = generate_code_verifier()
        code_challenge = get_code_challenge(self.__code_verifier)
        params = {
            "client_id": CLIENT_ID,
            "response_type": "code",
            "redirect_uri": self.redirect_uri,
            "scope": ",".join(SCOPES),
            "code_challenge_method": "S256",
            "code_challenge": code_challenge,
        }
        return f"{AUTH_URL}authorize?{urlencode(params)}"
    
    def await_token(self) -> TokenProvider.StoredToken:
        """
        Blocks until server thread gets token
        Returns:
            StoredToken
        """
        self.__server_thread.join()
        return self.__token
    
    def get_token(self) -> TokenProvider.StoredToken:
        """
        Gets a valid token
        Returns:
            StoredToken
        """
        if self.__token is None:
            raise RuntimeError("Session isn't authenticated!")
        elif self.__token.expired():
            self.set_token(self.__token.refresh_token, OAuth.RequestType.REFRESH)
        return self.__token
    
    def set_token(self, code: str, request_type: RequestType) -> None:
        """
        Fetches and sets stored token
        Returns:
            StoredToken
        """
        token_url = f"{AUTH_URL}api/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if request_type == OAuth.RequestType.LOGIN:
            body = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.redirect_uri,
                "client_id": CLIENT_ID,
                "code_verifier": self.__code_verifier,
            }
        elif request_type == OAuth.RequestType.REFRESH:
            body = {
                "grant_type": "refresh_token",
                "refresh_token": code,
                "client_id": CLIENT_ID,
            }
        response = post(token_url, headers=headers, data=body)
        if response.status_code != 200:
            raise IOError(
                f"Error fetching token: {response.status_code}, {response.text}"
            )
        self.__token = TokenProvider.StoredToken(response.json())
    
    def __run_server(self) -> None:
        server_address = (self.oauth_address, self.port)
        httpd = self.OAuthHTTPServer(server_address, self.RequestHandler, self)
        httpd.authenticator = self
        httpd.serve_forever()
    
    class RequestType(IntEnum):
        LOGIN = 0
        REFRESH = 1
    
    class OAuthHTTPServer(HTTPServer):
        authenticator: OAuth
        
        def __init__(
            self,
            server_address: tuple[str, int],
            RequestHandlerClass: type[BaseHTTPRequestHandler],
            authenticator: OAuth,
        ):
            super().__init__(server_address, RequestHandlerClass)
            self.authenticator = authenticator
    
    class RequestHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args):
            return
        
        def do_GET(self) -> None:
            parsed_path = urlparse(self.path)
            query_params = parse_qs(parsed_path.query)
            code = query_params.get("code")
            
            if code:
                if isinstance(self.server, OAuth.OAuthHTTPServer):
                    self.server.authenticator.set_token(
                        code[0], OAuth.RequestType.LOGIN
                    )
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"Authorization successful. You can close this window."
                )
                Thread(target=self.server.shutdown).start()
            else:
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"Authorization code not found.")
                Thread(target=self.server.shutdown).start()
