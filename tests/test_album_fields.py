"""Offline regression coverage for the source review findings."""
from contextlib import ExitStack
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


from zotify.config import CONFIG_VALUES, Config, Zotify
from zotify.const import DEFAULT, TYPE, ROOT_PATH, ROOT_PODCAST_PATH, FETCH_DELAY, RETRY_ATTEMPTS, EXPORT_ZMD_LOCATION
from zotify.utils import safe_typecast  # pylint: disable=wrong-import-position

IMPORT_ROOT = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
    prefix='zotify-review-import-'
)
Config.Values.update({key: safe_typecast(value, DEFAULT, value[TYPE])
                      for key, value in CONFIG_VALUES.items()})
Config.Values.update({ROOT_PATH: IMPORT_ROOT.name, ROOT_PODCAST_PATH: IMPORT_ROOT.name})

from zotify.api import Album, Artist, Content, Track  # pylint: disable=wrong-import-position
from zotify.metadata import Tagger  # pylint: disable=wrong-import-position


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


    def test_album_group_is_linked_to_the_artist(self):
        """The grouping string must not depend on unrelated booleans."""
        album, artist = Album('album:review'), Artist('artist:review')
        album.parse_metadata(artist, {'name': 'Release', 'album_group': 'appears_on', 'public': True})
        self.assertEqual(album.album_group[artist], 'appears_on')

    def test_album_date_and_label_reach_track_tags(self):
        """Preserve the full release date alongside the existing year tag."""
        album = Album('album:tags')
        album.release_date, album.label = '2026-10-01', 'Example label'
        track = Track('track:tags')
        track.album = album
        with patch.object(Album, 'get_image', return_value=None):
            _, _, custom = Tagger._content_to_tags(track)
        self.assertEqual(custom['date'], '2026-10-01')
        self.assertEqual(custom['label'], 'Example label')


if __name__ == '__main__':
    unittest.main()
