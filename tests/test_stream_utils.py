"""Regression coverage for the Spotify audio framing prefix."""
import unittest

from zotify.stream_utils import expected_stream_size, mark_stream_prefix_skipped


class FakeStream:
    def __init__(self, size):
        self.size = size


class FakeInputStream:
    def __init__(self, size, pos):
        self._size = size
        self._pos = pos

    def available(self):
        return self._size - self._pos


class FakeStreamer(FakeStream):
    def __init__(self, size, pos=0):
        super().__init__(size)
        self._input = FakeInputStream(size, pos)

    def stream(self):
        return self._input


class StreamSizeTests(unittest.TestCase):
    def test_unmodified_stream_uses_its_full_size(self):
        self.assertEqual(expected_stream_size(FakeStream(1000)), 1000)

    def test_skipped_header_is_excluded_from_download_size(self):
        stream = FakeStream(5_995_405)
        mark_stream_prefix_skipped(stream, 0xA7)
        self.assertEqual(expected_stream_size(stream), 5_995_238)

    def test_prefix_larger_than_stream_is_rejected(self):
        with self.assertRaises(ValueError):
            mark_stream_prefix_skipped(FakeStream(10), 0xA7)

    def test_header_skipped_inside_librespot_is_excluded(self):
        # The content feeder skips 0xA7 bytes without marking the streamer.
        self.assertEqual(expected_stream_size(FakeStreamer(5_995_405, pos=0xA7)), 5_995_238)

    def test_unskipped_streamer_uses_its_full_size(self):
        self.assertEqual(expected_stream_size(FakeStreamer(1000)), 1000)


if __name__ == "__main__":
    unittest.main()
