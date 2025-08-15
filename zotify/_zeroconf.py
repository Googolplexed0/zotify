from __future__ import annotations
from Cryptodome.Cipher import AES
from Cryptodome.Hash import HMAC, SHA1
from Cryptodome.Util import Counter
from librespot import util, Version
from librespot.core import Session
from librespot.crypto import DiffieHellman
from librespot.proto import Connect_pb2 as Connect
from librespot.structure import Closeable, Runnable, SessionListener
import base64
import concurrent.futures
import copy
import io
import json
import logging
import random
import socket
import threading
import typing
import urllib.parse
import zeroconf


class ZeroconfServer(Closeable):
    logger = logging.getLogger("Librespot:ZeroconfServer")
    logger.propagate = False

    service = "_spotify-connect._tcp.local."
    __connecting_username: typing.Union[str, None] = None
    __connection_lock = threading.Condition()
    __default_get_info_fields = {
        "status": 101,
        "statusString": "OK",
        "spotifyError": 0,
        "version": "2.9.0",
        "libraryVersion": Version.version_name,
        "accountReq": "FREE",
        "brandDisplayName": "Zotify",
        "modelDisplayName": "Zotify",
        "voiceSupport": "NO",
        "availability": "1",
        "productID": 0,
        "tokenType": "default",
        "groupStatus": "NONE",
        "resolverVersion": "1",
        "scope": "streaming,client-authorization-universal",
        "clientID": "65b708073fc0480ea92a077233ca87bd",
    }
    __default_successful_add_user = {
        "status": 101,
        "spotifyError": 0,
        "statusString": "OK",
    }
    __eol = b"\r\n"
    __max_port = 65536
    __min_port = 1024
    __runner: HttpRunner
    __service_info: zeroconf.ServiceInfo
    __session: typing.Union[Session, None] = None
    __session_listeners: typing.List[SessionListener] = []
    __zeroconf: zeroconf.Zeroconf

    def __init__(self, inner: Inner, listen_port):
        self.__inner = inner
        self.__keys = DiffieHellman()
        if listen_port == -1:
            listen_port = random.randint(self.__min_port + 1, self.__max_port)
        self.__runner = ZeroconfServer.HttpRunner(self, listen_port)
        threading.Thread(
            target=self.__runner.run, name="zeroconf-http-server", daemon=True
        ).start()

        self.__zeroconf = zeroconf.Zeroconf()

        advertised_ip_str = self._get_local_ip()

        server_hostname = socket.gethostname()
        if not server_hostname or server_hostname == "localhost":
            self.logger.warning(
                f"Machine hostname is '{server_hostname}', which is not ideal for mDNS. "
                f"Consider setting a unique hostname for this machine. "
                f"Using device name '{inner.device_name}' as part of the service instance name, "
                f"but relying on zeroconf library to handle server resolution for IP {advertised_ip_str}."
            )

        service_addresses = None
        if advertised_ip_str != "0.0.0.0":
            try:
                service_addresses = [socket.inet_aton(advertised_ip_str)]
            except socket.error:  # Catches errors like invalid IP string format
                self.logger.error(
                    f"Failed to convert IP string '{advertised_ip_str}' to packed address. Zeroconf will attempt to determine addresses from hostname."
                )

        self.__service_info = zeroconf.ServiceInfo(
            ZeroconfServer.service,  # type, e.g., "_spotify-connect._tcp.local."
            f"{inner.device_name}.{ZeroconfServer.service}",  # name, e.g., "MyDevice._spotify-connect._tcp.local."
            listen_port,
            0,  # weight
            0,  # priority
            {  # properties
                "CPath": "/",
                "VERSION": "1.0",
                "STACK": "SP",
            },
            server=f"{server_hostname}.local.",  # server FQDN
            addresses=service_addresses,  # Pass resolved IP, or None if 0.0.0.0 or conversion failed
        )

        self.__zeroconf.register_service(self.__service_info)

    def _get_local_ip(self) -> str:
        """Tries to determine a non-loopback local IP address for network advertisement."""
        s = None
        ip_address = (
            "0.0.0.0"  # Default to all interfaces if specific IP cannot be found
        )
        try:
            # Attempt to connect to an external address to find the appropriate local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.1)  # Short timeout for the connect attempt
            s.connect(("8.8.8.8", 80))  # Google's public DNS server
            ip_address = s.getsockname()[0]
            self.logger.info(f"Determined local IP via connect trick: {ip_address}")
        except (
            OSError
        ) as e:  # Catches socket errors like [Errno 101] Network is unreachable
            self.logger.warning(
                f"Could not determine local IP via connect trick ({e}). Trying socket.gethostname()."
            )
            try:
                hostname = socket.gethostname()
                # gethostbyname can return 127.0.0.1 if hostname resolves to it
                ip_address = socket.gethostbyname(hostname)
                self.logger.info(
                    f"IP from socket.gethostname('{hostname}'): {ip_address}"
                )
            except socket.gaierror:
                self.logger.error(
                    f"socket.gaierror resolving hostname '{socket.gethostname()}'. Falling back to 0.0.0.0."
                )
                ip_address = "0.0.0.0"
        finally:
            if s:
                s.close()

        # If the IP is loopback (127.x.x.x) or still 0.0.0.0, try to find a better one from all interfaces
        if ip_address.startswith("127.") or ip_address == "0.0.0.0":
            self.logger.warning(
                f"Current IP ('{ip_address}') is loopback or 0.0.0.0. Attempting to find a non-loopback IP from host interfaces."
            )
            try:
                current_hostname = socket.gethostname()
                all_ips_info = socket.gethostbyname_ex(current_hostname)
                # all_ips_info is a tuple: (hostname, aliaslist, ipaddrlist)
                non_loopback_ips = [
                    ip for ip in all_ips_info[2] if not ip.startswith("127.")
                ]

                if non_loopback_ips:
                    ip_address = non_loopback_ips[0]  # Pick the first non-loopback IP
                    self.logger.info(
                        f"Found non-loopback IP from host interfaces ('{current_hostname}'): {ip_address}"
                    )
                else:
                    self.logger.warning(
                        f"No non-loopback IPs found for hostname '{current_hostname}'. Retaining '{ip_address}'."
                    )
                    # If ip_address was 0.0.0.0, it remains so. If it was 127.0.0.1, it remains so.
            except socket.gaierror:
                self.logger.error(
                    f"socket.gaierror during fallback IP search for hostname '{socket.gethostname()}'. Retaining '{ip_address}'."
                )

        if ip_address == "0.0.0.0":
            self.logger.warning(
                "Failed to determine a specific non-loopback IP. Zeroconf will attempt to use all available interfaces."
            )
        elif ip_address.startswith("127."):
            self.logger.warning(
                f"Final IP for advertisement is loopback ('{ip_address}'). Service discovery may not work correctly on the network."
            )
        else:
            self.logger.info(
                f"Using IP address for Zeroconf advertisement: {ip_address}"
            )

        return ip_address

    def add_session_listener(self, listener: ZeroconfServer):
        self.__session_listeners.append(listener)

    def close(self) -> None:
        self.__zeroconf.close()
        self.__runner.close()

    def close_session(self) -> None:
        if self.__session is None:
            return
        for session_listener in self.__session_listeners:
            session_listener.session_closing(self.__session)
        self.__session.close()
        self.__session = None

    def handle_add_user(
        self, __socket: socket.socket, params: dict[str, str], http_version: str
    ) -> None:
        username = params.get("userName")
        if not username:
            self.logger.error("Missing userName!")
            return
        blob_str = params.get("blob")
        if not blob_str:
            self.logger.error("Missing blob!")
            return
        client_key_str = params.get("clientKey")
        if not client_key_str:
            self.logger.error("Missing clientKey!")
        with self.__connection_lock:
            if username == self.__connecting_username:
                self.logger.info("{} is already trying to connect.".format(username))
                __socket.send(http_version.encode())
                __socket.send(b" 403 Forbidden")
                __socket.send(self.__eol)
                __socket.send(self.__eol)
                return
        shared_key = util.int_to_bytes(
            self.__keys.compute_shared_key(base64.b64decode(client_key_str.encode()))
        )
        blob_bytes = base64.b64decode(blob_str)
        iv = blob_bytes[:16]
        encrypted = blob_bytes[16 : len(blob_bytes) - 20]
        checksum = blob_bytes[len(blob_bytes) - 20 :]
        sha1 = SHA1.new()
        sha1.update(shared_key)
        base_key = sha1.digest()[:16]
        hmac = HMAC.new(base_key, digestmod=SHA1)
        hmac.update(b"checksum")
        checksum_key = hmac.digest()
        hmac = HMAC.new(base_key, digestmod=SHA1)
        hmac.update(b"encryption")
        encryption_key = hmac.digest()
        hmac = HMAC.new(checksum_key, digestmod=SHA1)
        hmac.update(encrypted)
        mac = hmac.digest()
        if mac != checksum:
            self.logger.error("Mac and checksum don't match!")
            __socket.send(http_version.encode())
            __socket.send(b" 400 Bad Request")
            __socket.send(self.__eol)
            __socket.send(self.__eol)
            return
        aes = AES.new(
            encryption_key[:16],
            AES.MODE_CTR,
            counter=Counter.new(128, initial_value=int.from_bytes(iv, "big")),
        )
        decrypted = aes.decrypt(encrypted)
        self.close_session()
        with self.__connection_lock:
            self.__connecting_username = username
        self.logger.info(
            "Accepted new user from {}. [deviceId: {}]".format(
                params.get("deviceName"), self.__inner.device_id
            )
        )
        response = json.dumps(self.__default_successful_add_user)
        __socket.send(http_version.encode())
        __socket.send(b" 200 OK")
        __socket.send(self.__eol)
        __socket.send(b"Content-Length: ")
        __socket.send(str(len(response)).encode())
        __socket.send(self.__eol)
        __socket.send(self.__eol)
        __socket.send(response.encode())
        self.__session = (
            Session.Builder(self.__inner.conf)
            .set_device_id(self.__inner.device_id)
            .set_device_name(self.__inner.device_name)
            .set_device_type(self.__inner.device_type)
            .set_preferred_locale(self.__inner.preferred_locale)
            .blob(username, decrypted)
            .create()
        )

        # Let librespot save credentials to current directory (like the working root files)
        # Then move them to the desired location
        try:
            import os
            import pathlib
            from pathlib import Path
            import shutil
            import time

            # Wait a moment for librespot to save credentials to current directory
            time.sleep(2)

            # Check if credentials were saved to current directory
            current_creds_path = Path.cwd() / "credentials.json"

            if current_creds_path.exists():
                self.logger.info(
                    f"Credentials found in current directory: {current_creds_path}"
                )

                # Move credentials to AppData location for zotify package
                if os.name == "nt":  # Windows
                    appdata_creds_path = (
                        Path.home() / "AppData/Roaming/Zotify/credentials.json"
                    )
                elif os.name == "posix":  # Linux/macOS
                    if os.uname().sysname == "Darwin":  # macOS
                        appdata_creds_path = (
                            Path.home()
                            / "Library/Application Support/Zotify/credentials.json"
                        )
                    else:  # Linux
                        appdata_creds_path = (
                            Path.home() / ".local/share/zotify/credentials.json"
                        )
                else:
                    appdata_creds_path = Path.cwd() / ".zotify/credentials.json"

                # Ensure AppData directory exists
                appdata_creds_path.parent.mkdir(parents=True, exist_ok=True)

                # Copy credentials to AppData location
                shutil.copy2(current_creds_path, appdata_creds_path)
                self.logger.info(f"Credentials copied to AppData: {appdata_creds_path}")

                # Remove the temporary file from current directory
                try:
                    current_creds_path.unlink()
                    self.logger.info(
                        "Temporary credentials file removed from current directory"
                    )
                except Exception as e:
                    self.logger.warning(f"Could not remove temporary file: {e}")

                self.logger.info(
                    f"Credentials successfully moved to {appdata_creds_path} for user: {username}"
                )

            else:
                self.logger.warning(
                    "No credentials found in current directory - librespot may not have saved them"
                )

        except Exception as e:
            self.logger.error(f"Failed to move credentials: {e}")

        with self.__connection_lock:
            self.__connecting_username = None
        for session_listener in self.__session_listeners:
            session_listener.session_changed(self.__session)

    def handle_get_info(self, __socket: socket.socket, http_version: str) -> None:
        info = copy.deepcopy(self.__default_get_info_fields)
        info["deviceID"] = self.__inner.device_id
        info["remoteName"] = self.__inner.device_name
        info["publicKey"] = base64.b64encode(self.__keys.public_key_bytes()).decode()
        device_type_name = Connect.DeviceType.Name(self.__inner.device_type)
        info["deviceType"] = device_type_name.title()
        with self.__connection_lock:
            info["activeUser"] = (
                self.__connecting_username
                if self.__connecting_username is not None
                else self.__session.username() if self.has_valid_session() else ""
            )
        __socket.send(http_version.encode())
        __socket.send(b" 200 OK")
        __socket.send(self.__eol)
        __socket.send(b"Content-Type: application/json")
        __socket.send(self.__eol)
        __socket.send(self.__eol)
        __socket.send(json.dumps(info).encode())

    def has_valid_session(self) -> bool:
        valid = self.__session and self.__session.is_valid()
        if not valid:
            self.__session = None
        return valid

    def parse_path(self, path: str) -> dict[str, str]:
        url = "https://host" + path
        parsed = {}
        params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        for key, values in params.items():
            for value in values:
                parsed[key] = value
        return parsed

    def remove_session_listener(self, listener: SessionListener):
        self.__session_listeners.remove(listener)

    class Builder(Session.Builder):
        listen_port: int = -1

        def set_listen_port(self, listen_port: int):
            self.listen_port = listen_port
            return self

        def create(self) -> ZeroconfServer:
            return ZeroconfServer(
                ZeroconfServer.Inner(
                    self.device_type,
                    self.device_name,
                    self.device_id,
                    self.preferred_locale,
                    self.conf,
                ),
                self.listen_port,
            )

    class HttpRunner(Closeable, Runnable):
        __should_stop = False
        __socket: socket.socket
        __worker = concurrent.futures.ThreadPoolExecutor()
        __zeroconf_server: ZeroconfServer

        def __init__(self, zeroconf_server: ZeroconfServer, port: int):
            self.__socket = socket.socket()
            self.__socket.bind((".".join(["0"] * 4), port))
            self.__socket.listen(5)
            self.__zeroconf_server = zeroconf_server
            self.__zeroconf_server.logger.info(
                "Zeroconf HTTP server started successfully on port {}!".format(port)
            )

        def close(self) -> None:
            pass

        def run(self):
            while not self.__should_stop:
                __socket, address = self.__socket.accept()

                def anonymous():
                    self.__handle(__socket)
                    __socket.close()

                self.__worker.submit(anonymous)

        def __handle(self, __socket: socket.socket) -> None:
            request = io.BytesIO(__socket.recv(1024 * 1024))
            request_line = request.readline().strip().split(b" ")
            if len(request_line) != 3:
                self.__zeroconf_server.logger.warning(
                    "Unexpected request line: {}".format(request_line)
                )
            method = request_line[0].decode()
            path = request_line[1].decode()
            http_version = request_line[2].decode()
            headers = {}
            while True:
                header = request.readline().strip()
                if not header:
                    break
                split = header.split(b":")
                headers[split[0].decode()] = split[1].strip().decode()
            if not self.__zeroconf_server.has_valid_session():
                self.__zeroconf_server.logger.debug(
                    "Handling request: {}, {}, {}, headers: {}".format(
                        method, path, http_version, headers
                    )
                )
            params = {}
            if method == "POST":
                content_type = headers.get("Content-Type")
                if content_type != "application/x-www-form-urlencoded":
                    self.__zeroconf_server.logger.error(
                        "Bad Content-Type: {}".format(content_type)
                    )
                    return
                content_length_str = headers.get("Content-Length")
                if content_length_str is None:
                    self.__zeroconf_server.logger.error(
                        "Missing Content-Length header!"
                    )
                    return
                content_length = int(content_length_str)
                body = request.read(content_length).decode()
                pairs = body.split("&")
                for pair in pairs:
                    split = pair.split("=")
                    params[urllib.parse.unquote(split[0])] = urllib.parse.unquote(
                        split[1]
                    )
            else:
                params = self.__zeroconf_server.parse_path(path)
            action = params.get("action")
            if action is None:
                self.__zeroconf_server.logger.debug("Request is missing action.")
                return
            self.handle_request(__socket, http_version, action, params)

        def handle_request(
            self,
            __socket: socket.socket,
            http_version: str,
            action: str,
            params: dict[str, str],
        ) -> None:
            if action == "addUser":
                if params is None:
                    raise RuntimeError
                self.__zeroconf_server.handle_add_user(__socket, params, http_version)
            elif action == "getInfo":
                self.__zeroconf_server.handle_get_info(__socket, http_version)
            else:
                self.__zeroconf_server.logger.warning(
                    "Unknown action: {}".format(action)
                )

    class Inner:
        conf: typing.Final[Session.Configuration]
        device_name: typing.Final[str]
        device_id: typing.Final[str]
        device_type: typing.Final[Connect.DeviceType]
        preferred_locale: typing.Final[str]

        def __init__(
            self,
            device_type: Connect.DeviceType,
            device_name: str,
            device_id: str,
            preferred_locale: str,
            conf: Session.Configuration,
        ):
            self.conf = conf
            self.device_name = "Zotify"
            self.device_id = (
                util.random_hex_string(40).lower() if not device_id else device_id
            )
            self.device_type = device_type
            self.preferred_locale = preferred_locale
