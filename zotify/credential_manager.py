import logging
import time
import json
from pathlib import Path
from typing import Optional, Dict, Any

# Import the ZeroconfServer - handle both package and direct execution
try:
    # Try relative import first (when running as package)
    from ._zeroconf import ZeroconfServer
except ImportError:
    try:
        # Try direct import (when running from folder)
        from _zeroconf import ZeroconfServer
    except ImportError:
        # Try importing from parent directory (when running from root)
        import sys
        import os

        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from _zeroconf import ZeroconfServer


class ZeroconfCredentialManager:
    """
    Credential manager that uses ZeroconfServer to generate credentials
    by allowing desktop Spotify clients to transfer playback
    """

    def __init__(self, credentials_path: str = "credentials.json"):
        self.credentials_path = Path(credentials_path)
        self.zeroconf_server = None
        self.logger = logging.getLogger("Zotify:ZeroconfCredentialManager")

    def generate_credentials(self, timeout: int = 300) -> Optional[Dict[str, Any]]:
        self.logger.info(f"Starting ZeroconfServer to generate credentials...")
        self.logger.warning(
            f"Transfer playback from desktop Spotify client to 'Zotify' via Spotify Connect"
        )
        self.logger.warning(f"Waiting for credentials to be generated...")

        try:
            # Create and start the ZeroconfServer
            self.zeroconf_server = ZeroconfServer.Builder().create()
            self.logger.info("ZeroconfServer started successfully")

            # Wait for credentials to be generated
            start_time = time.time()
            session_established = False

            while time.time() - start_time < timeout:
                # First check if session is established
                if (
                    not session_established
                    and self.zeroconf_server._ZeroconfServer__session
                ):
                    username = self.zeroconf_server._ZeroconfServer__session.username()
                    self.logger.info(f"Session established for user: {username}")
                    session_established = True
                    self.logger.info(
                        "Session established, waiting for credentials file..."
                    )

                # Check if credentials file exists in the AppData location (where ZeroconfServer now saves them)
                import os

                if os.name == "nt":  # Windows
                    default_credentials_path = (
                        Path.home() / "AppData/Roaming/Zotify/credentials.json"
                    )
                elif os.name == "posix":  # Linux/macOS
                    if os.uname().sysname == "Darwin":  # macOS
                        default_credentials_path = (
                            Path.home()
                            / "Library/Application Support/Zotify/credentials.json"
                        )
                    else:  # Linux
                        default_credentials_path = (
                            Path.home() / ".local/share/zotify/credentials.json"
                        )
                else:
                    default_credentials_path = Path.cwd() / ".zotify/credentials.json"

                # Log progress every 10 seconds
                if (
                    int(time.time() - start_time) % 10 == 0
                    and time.time() - start_time > 0
                ):
                    self.logger.info(
                        f"Still waiting for credentials... ({int(time.time() - start_time)}s elapsed)"
                    )
                    if session_established:
                        self.logger.info(
                            "Session is established, waiting for credentials file..."
                        )
                        # Check if the credentials file exists
                        if default_credentials_path.exists():
                            self.logger.info(
                                f"Credentials file found: {default_credentials_path}"
                            )
                        else:
                            self.logger.info(
                                f"Credentials file not yet created at: {default_credentials_path}"
                            )
                    else:
                        self.logger.info("Waiting for session establishment...")

                if default_credentials_path.exists():
                    self.logger.info(f"Credentials found in {default_credentials_path}")

                    # Copy credentials to the desired location if different
                    if default_credentials_path != self.credentials_path:
                        import shutil

                        shutil.copy2(default_credentials_path, self.credentials_path)
                        self.logger.info(
                            f"Credentials copied to {self.credentials_path}"
                        )
                    else:
                        self.logger.info(
                            f"Credentials already in correct location: {self.credentials_path}"
                        )

                    self.logger.info(f"Credentials saved to {self.credentials_path}")
                    return self._load_credentials()

                time.sleep(1)

            self.logger.error(
                f"Timeout reached ({timeout}s). No credentials generated."
            )
            if session_established:
                self.logger.error(
                    "Session was established but credentials file was never created"
                )
                self.logger.error(
                    "This suggests the credential saving logic in _zeroconf.py is not working"
                )
            else:
                self.logger.error("Session was never established")
            return None

        except Exception as e:
            self.logger.error(f"Error generating credentials: {e}")
            return None
        finally:
            if self.zeroconf_server:
                self.zeroconf_server.close()

    def _load_credentials(self) -> Optional[Dict[str, Any]]:
        """Load credentials from the generated file"""
        try:
            with open(self.credentials_path, "r") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading credentials: {e}")
            return None

    def has_credentials(self) -> bool:
        """Check if credentials file exists"""
        return self.credentials_path.exists()

    def get_credentials_path(self) -> Path:
        """Get the path to the credentials file"""
        return self.credentials_path

    def cleanup(self):
        """Clean up resources"""
        if self.zeroconf_server:
            self.zeroconf_server.close()


def generate_credentials_interactive(
    credentials_path: str = None,
) -> Optional[Dict[str, Any]]:
    manager = ZeroconfCredentialManager(credentials_path or "credentials.json")

    try:
        credentials = manager.generate_credentials()
        if credentials:
            return credentials
        else:
            return None
    finally:
        manager.cleanup()
