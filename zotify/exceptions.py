class ZotifyException(Exception):
    """Base exception for Zotify"""
    pass

class LoginError(ZotifyException):
    """Raised when Zotify fails to log in"""
    pass

class RateLimitError(ZotifyException):
    """Raised when Spotify rate limits the session"""
    pass

class ConnectionDropError(ZotifyException):
    """Raised when the connection to Spotify drops unexpectedly"""
    pass
