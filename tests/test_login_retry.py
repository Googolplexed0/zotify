"""Run with the repository root on sys.path; no Spotify account is used."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch
from zotify.config import Zotify

with TemporaryDirectory() as directory:
    config = MagicMock()
    config.get_root_path.return_value = Path(directory)
    config.get_retry_attempts.return_value = 1
    config.debug.return_value = False
    with patch.object(Zotify, 'CONFIG', config), \
         patch.object(Zotify, 'login') as login, \
         patch.object(Zotify, 'parse_dl_quality', return_value=(False, None, None)), \
         patch('zotify.config.Loader'), \
         patch('zotify.config.Printer'), \
         patch('zotify.config.logging.basicConfig'), \
         patch('zotify.config.sleep'):
        Zotify.boot(MagicMock())
        assert login.call_count == 1, 'Successful login must not be repeated'
        login.reset_mock()
        login.side_effect = [ConnectionError('test failure'), None]
        Zotify.boot(MagicMock())
        assert login.call_count == 2, 'Failed login should still be retried'
print('PASS: successful login stops retries; failed login retries')
