"""Helpers for validating the payload exposed by a Spotify audio stream."""


def mark_stream_prefix_skipped(stream, byte_count: int) -> None:
    """Record how many leading bytes the caller already consumed from a stream."""
    if byte_count < 0 or byte_count > stream.size:
        raise ValueError("Skipped stream prefix is outside the declared stream size")
    stream._zotify_expected_size = stream.size - byte_count


def expected_stream_size(stream) -> int:
    """Return the bytes still available to download from a stream."""
    return getattr(stream, "_zotify_expected_size", stream.size)
