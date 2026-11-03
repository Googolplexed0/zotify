"""Offline regression coverage for the source review findings."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


from zotify.config import CONFIG_VALUES, Config, LoginHandler, Zotify
from zotify.const import DEFAULT, TYPE, ROOT_PATH, ROOT_PODCAST_PATH, FETCH_DELAY, RETRY_ATTEMPTS, EXPORT_ZMD_LOCATION
from zotify.utils import safe_typecast  # pylint: disable=wrong-import-position

IMPORT_ROOT = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
    prefix='zotify-review-import-'
)
Config.Values.update({key: safe_typecast(value, DEFAULT, value[TYPE])
                      for key, value in CONFIG_VALUES.items()})
Config.Values.update({ROOT_PATH: IMPORT_ROOT.name, ROOT_PODCAST_PATH: IMPORT_ROOT.name})

from zotify.api import Content  # pylint: disable=wrong-import-position


def tearDownModule():  # pylint: disable=invalid-name
    """Remove only this module's disposable import directory."""
    IMPORT_ROOT.cleanup()


class RegressionTests(unittest.TestCase):
    """Test actual code paths without credentials, accounts or real media."""

    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(
            tempfile.TemporaryDirectory(prefix='zotify-review-test-')
        ))
        config = {key: safe_typecast(value, DEFAULT, value[TYPE])
                  for key, value in CONFIG_VALUES.items()}
        config.update({ROOT_PATH: str(self.root), ROOT_PODCAST_PATH: str(self.root),
                       FETCH_DELAY: 0.0, RETRY_ATTEMPTS: 0,
                       EXPORT_ZMD_LOCATION: str(self.root / 'cache')})
        self.enter_context(patch.object(Config, 'Values', config))
        self.enter_context(patch.object(Zotify, 'TOTAL_API_CALLS', 0))
        self.enter_context(patch.object(Content, 'ALL_NODES', {}))
        self.enter_context(patch('requests.sessions.Session.request',
                                side_effect=AssertionError('Network forbidden')))
        self.enter_context(patch('socket.create_connection',
                                side_effect=AssertionError('Network forbidden')))

    def enter_context(self, context):
        """Use cleanup-backed contexts on the supported Python 3.10 baseline."""
        return self.stack.enter_context(context)


    def test_session_credentials_are_private_even_when_replacing_old_file(self):
        """Replace a dummy permissive file without exposing a partially written token."""
        path = self.root / 'credentials.json'
        path.write_text('{"old":true}')
        path.chmod(0o644)
        session = SimpleNamespace(credentials=lambda: {'dummy': 'fixture'})
        previous_umask = os.umask(0o022)
        try:
            with patch.object(Config, 'get_credentials_location', return_value=path), \
                 patch.object(LoginHandler, 'SESSION', session), patch.object(LoginHandler, 'OAUTH', None):
                LoginHandler.save_credentials()
        finally:
            os.umask(previous_umask)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(path.read_text()), {'dummy': 'fixture'})
        from zotify.utils import write_private_json  # pylint: disable=import-outside-toplevel
        with patch('zotify.utils.json.dump', side_effect=ValueError('serialization failed')):
            with self.assertRaises(ValueError):
                write_private_json(path, {'dummy': 'replacement'})
        self.assertEqual(json.loads(path.read_text()), {'dummy': 'fixture'})
        self.assertEqual(list(self.root.glob('.credentials.json-*')), [])


if __name__ == '__main__':
    unittest.main()
