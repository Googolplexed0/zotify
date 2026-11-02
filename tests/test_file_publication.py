"""Offline regression coverage for the source review findings."""
from contextlib import ExitStack, nullcontext
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import wave


from zotify.config import CONFIG_VALUES, Config, Zotify
from zotify.const import DEFAULT, TYPE, ROOT_PATH, ROOT_PODCAST_PATH, FETCH_DELAY, RETRY_ATTEMPTS, EXPORT_ZMD_LOCATION
from zotify.utils import safe_typecast, check_path_dupes, pathlike_move_safe

IMPORT_ROOT = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
    prefix='zotify-review-import-'
)
Config.Values.update({key: safe_typecast(value, DEFAULT, value[TYPE])
                      for key, value in CONFIG_VALUES.items()})
Config.Values.update({ROOT_PATH: IMPORT_ROOT.name, ROOT_PODCAST_PATH: IMPORT_ROOT.name})

from zotify.api import Content, DLContent, Episode  # pylint: disable=wrong-import-position


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

    def test_collision_selection_checks_candidates_instead_of_counting(self):
        """Gaps in suffixes, empty files and broken links remain occupied."""
        base = self.root / 'song.ogg'
        base.write_bytes(b'first')
        (self.root / 'song_2.ogg').write_bytes(b'second')
        candidate = check_path_dupes(base)
        self.assertEqual(candidate.name, 'song_1.ogg')
        candidate.touch()
        (self.root / 'song_3.ogg').symlink_to(self.root / 'missing')
        self.assertEqual(check_path_dupes(base).name, 'song_4.ogg')

    def test_publication_refuses_overwrite_and_preserves_source(self):
        """A destination appearing after selection cannot be replaced."""
        src, dst = self.root / 'source', self.root / 'destination'
        src.write_bytes(b'new')
        dst.write_bytes(b'existing')
        with self.assertRaises(FileExistsError):
            pathlike_move_safe(src, dst)
        self.assertEqual(dst.read_bytes(), b'existing')
        self.assertEqual(src.read_bytes(), b'new')

    def test_publication_supports_bytes_stream_copy_and_move(self):
        """All supported inputs publish their bytes and respect copy mode."""
        src = self.root / 'source'
        src.write_bytes(b'path')
        src.chmod(0o640)
        copied = pathlike_move_safe(src, self.root / 'copy', copy=True)
        self.assertTrue(src.exists())
        self.assertEqual(copied.stat().st_mode & 0o777, 0o640)
        moved = pathlike_move_safe(src, self.root / 'move')
        self.assertFalse(src.exists())
        self.assertEqual(moved.read_bytes(), b'path')
        for name, value in [('bytes', b'bytes'), ('stream', io.BytesIO(b'stream'))]:
            self.assertEqual(pathlike_move_safe(value, self.root / name).read_bytes(), name.encode())

    def test_failed_stream_does_not_leave_partial_destination(self):
        """Interrupted publication removes only the file it created."""
        stream = SimpleNamespace(read=unittest.mock.Mock(side_effect=[b'partial', OSError('read failed')]))
        dst = self.root / 'partial'
        with self.assertRaises(OSError):
            pathlike_move_safe(stream, dst)
        self.assertFalse(dst.exists())

    def test_ffmpeg_publication_handles_concurrent_destination(self):
        """Use a synthetic WAV to verify conversion and no-clobber publication."""
        source = self.root / 'synthetic.wav'
        with wave.Wave_write(str(source)) as audio:
            audio.setparams((1, 2, 8000, 800, 'NONE', 'not compressed'))
            audio.writeframes(b'\0\0' * 800)
        destination = self.root / 'synthetic.ogg'
        destination.write_bytes(b'existing')
        with self.assertRaises(FileExistsError):
            DLContent.run_ffm(source, None, destination, ['-c:a', 'libvorbis'])
        self.assertEqual(destination.read_bytes(), b'existing')
        self.assertTrue(source.exists())
        destination.unlink()
        DLContent.run_ffm(source, None, destination, ['-c:a', 'libvorbis'])
        self.assertTrue(destination.read_bytes().startswith(b'OggS'))
        self.assertFalse(source.exists())
        self.assertEqual(list(self.root.glob('.zotify-*')), [])


    def test_direct_podcast_copy_never_consumes_response_content(self):
        """The raw stream is copied once and the response is always closed."""
        class FakeResponse:
            """A streamed response whose eager content access is forbidden."""
            status_code = 200
            headers = {'Content-Length': '7'}
            raw = io.BytesIO(b'episode')
            closed = False

            @property
            def content(self):
                """Forbid preloading a streamed response."""
                raise AssertionError('Eager content access drains the stream')

            def __enter__(self):
                return self

            def __exit__(self, *_):
                self.closed = True

        response = FakeResponse()
        episode = Episode('episode:review')
        destination = self.root / 'episode.tmp'
        with patch.object(Zotify, 'get_request', return_value=response), \
             patch('zotify.api.Printer.pbar_stream', side_effect=lambda raw, **_: nullcontext(raw)), \
             patch.object(episode, 'set_dl_status'):
            episode.download_directly(destination)
        self.assertEqual(destination.read_bytes(), b'episode')
        self.assertTrue(response.closed)


if __name__ == '__main__':
    unittest.main()
