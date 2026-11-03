"""Offline regression tests for album totals and per-disc audio tags."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from zotify.config import CONFIG_VALUES, Config, Zotify
from zotify.const import DEFAULT, MD_DISC_TRACK_TOTALS, ROOT_PATH, ROOT_PODCAST_PATH, TYPE
from zotify.utils import safe_typecast

# Configure imports in memory. Never read credentials or create a Session.
# Keep import-time paths alive until tearDownModule performs explicit cleanup.
TEST_ROOT = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
    prefix="zotify-metadata-tests-"
)
Config.Values.update({key: safe_typecast(value, DEFAULT, value[TYPE])
                      for key, value in CONFIG_VALUES.items()})
Config.Values[ROOT_PATH] = TEST_ROOT.name
Config.Values[ROOT_PODCAST_PATH] = TEST_ROOT.name

# Content classes read configured paths at import time.
from zotify.api import Album, Content, Track  # pylint: disable=wrong-import-position
from zotify.metadata import Tagger  # pylint: disable=wrong-import-position


def tearDownModule():  # pylint: disable=invalid-name
    """Remove the disposable import-time directories."""
    TEST_ROOT.cleanup()


class MemoryTags(dict):
    """Minimal music_tag storage that never opens an audio file."""

    def __init__(self):
        super().__init__(artwork=b"existing cover", lyrics="existing lyrics")
        self.tag_map = {}

    def save(self):
        """Simulate a completed write without touching a file."""


class MetadataTotalsTests(unittest.TestCase):
    """Exercise the actual response parser, pagination completion and tagger."""

    def setUp(self):
        Content.ALL_NODES.clear()
        self.addCleanup(Content.ALL_NODES.clear)
        for method in ("invoke_url", "invoke_libre_md", "invoke_libre_bulk_md"):
            guard = patch.object(
                Zotify, method, side_effect=AssertionError("Unexpected network request")
            )
            guard.start()
            self.addCleanup(guard.stop)

    @staticmethod
    def track_response(index, disc=1, number=None):
        """Build a simplified Spotify track response."""
        return {"uri": f"spotify:track:test{index}", "type": "track",
                "name": f"Track {index}", "disc_number": disc,
                "track_number": number or index, "duration_ms": 1000}

    def album(self, discs, total=None, next_page=None):
        """Parse a Web API album, optionally with more pages pending."""
        album = Album("album:test")
        response = {"name": "Album", "type": "album",
                    "tracks": {"items": [self.track_response(i, disc)
                                         for i, disc in enumerate(discs, 1)], "next": next_page}}
        if total is not None:
            response["total_tracks"] = total
        album.parse_metadata(None, response)
        return album

    def tags(self, track):
        """Run the public write path against in-memory tag storage."""
        storage = MemoryTags()
        with patch("zotify.metadata.load_file", return_value=storage), \
             patch.object(Album, "get_image", return_value=None):
            Tagger(Path(TEST_ROOT.name) / "unused.ogg").write_tags(track)
        return storage

    def test_single_disc_totals_and_existing_artwork_are_preserved(self):
        """Single-disc tags retain their counts and unrelated embedded metadata."""
        album = self.album([1, 1, 1], total=3)
        tags = self.tags(album.tracks[1])
        self.assertEqual((tags["totaltracks"], tags["totaldiscs"]), (3, 1))
        self.assertEqual(tags["artwork"], b"existing cover")
        self.assertEqual(tags["lyrics"], "existing lyrics")

    def test_two_disc_tags_use_their_own_track_count(self):
        """The file denominator describes its disc, not the entire release."""
        album = self.album([1] * 12 + [2] * 8, total=20)
        self.assertEqual(self.tags(album.tracks[0])["totaltracks"], 12)
        tags = self.tags(album.tracks[-1])
        self.assertEqual((tags["totaltracks"], tags["totaldiscs"]), (8, 2))
        self.assertEqual(album.total_tracks, 20)

    def test_unsorted_tracks_do_not_change_the_disc_total(self):
        """Disc counts do not depend on which disc appears last."""
        album = self.album([2, 1, 2, 1], total=4)
        self.assertEqual(album.total_discs, 2)
        self.assertIsInstance(album.total_discs, int)

    def test_pagination_defers_totals_until_all_pages_are_parsed(self):
        """A partial first page cannot supply either tag denominator."""
        album = self.album([1, 1], total=3, next_page="more")
        self.assertIsNone(album.total_discs)
        self.assertIsNone(album.track_total_for_disc(1))
        with patch.object(Album, "fetch_items", return_value=[self.track_response(3, 2)]):
            album.grab_more_children()
        self.assertEqual(album.total_discs, 2)
        self.assertEqual(album.track_total_for_disc(1), 2)
        self.assertEqual(album.track_total_for_disc(2), 1)

    def test_librespot_disc_lists_supply_missing_album_total(self):
        """Nested disc lists can supply the missing album-wide track total."""
        album = Album("album:librespot")
        album.parse_metadata(None, {"name": "Album", "type": "album", "disc": [
            {"number": 1, "track": [self.track_response(1), self.track_response(2)]},
            {"number": 2, "track": [self.track_response(3)]}]})
        self.assertEqual((album.total_tracks, album.total_discs), (3, 2))
        self.assertEqual(album.tracks[-1].track_number, 1)
        self.assertEqual(self.tags(album.tracks[-1])["totaltracks"], 1)

    def test_declared_total_mismatch_does_not_publish_partial_counts(self):
        """A short listing cannot replace the declared complete album count."""
        album = self.album([1], total=20)
        tags = self.tags(album.tracks[0])
        self.assertNotIn("totaltracks", tags)
        self.assertNotIn("totaldiscs", tags)
        self.assertEqual(album.total_tracks, 20)

    def test_empty_album_does_not_raise_or_guess_totals(self):
        """Empty listings are accepted without indexing their last element."""
        album = self.album([])
        self.assertIsNone(album.total_discs)
        self.assertIsNone(album.track_total_for_disc(1))

    def test_missing_invalid_or_gapped_disc_numbers_remain_unknown(self):
        """Unusable numbering does not invent disc or track denominators."""
        for discs in ([None], [0], [-1], ["1"], [1, 3]):
            with self.subTest(discs=discs):
                Content.ALL_NODES.clear()
                album = Album("album:invalid")
                for index, disc in enumerate(discs):
                    track = Track(f"track:invalid{index}")
                    track.disc_number = disc
                    album.tracks.append(track)
                album.update_track_totals()
                self.assertIsNone(album.total_discs)
                self.assertIsNone(album.track_total_for_disc(1))

    def test_unavailable_entries_do_not_raise_or_guess_totals(self):
        """Null track responses keep derived totals unknown."""
        album = Album("album:unavailable")
        with patch("zotify.metadata.Printer.hashtaged"):
            album.parse_metadata(None, {"total_tracks": 1,
                                       "tracks": {"items": [None], "next": None}})
        self.assertIsNone(album.total_discs)

    def test_disabled_totals_setting_is_respected(self):
        """Explicitly disabled total tags are not written."""
        album = self.album([1, 1], total=2)
        with Config.temporary_config(MD_DISC_TRACK_TOTALS, False):
            tags = self.tags(album.tracks[0])
        self.assertNotIn("totaltracks", tags)
        self.assertNotIn("totaldiscs", tags)

    def test_restored_metadata_recomputes_stale_disc_totals(self):
        """Tagging restored objects recalculates old or cached disc counts."""
        album = self.album([2, 1], total=2)
        album.total_discs = "1"
        self.assertEqual(self.tags(album.tracks[0])["totaldiscs"], 2)


if __name__ == "__main__":
    unittest.main()
