# Metadata regression tests

With the project dependencies installed, run from the checkout:

```sh
python -m unittest discover -s tests -v
```

The tests initialize configuration in a disposable directory, never load user
credentials, and reject metadata network requests. File-tag writes use in-memory
storage. No login, audio download or existing-library write is performed.

The album-wide `total_tracks` value remains available for metadata and templates.
Audio `totaltracks` tags count the tracks on the current disc, matching Spotify's
per-disc track numbering. Disc totals are derived from all album tracks, with
pagination completed first. Missing album-wide totals are filled from a complete
track listing; incomplete, unavailable or inconsistently numbered listings do
not produce guessed track/disc tag totals.
