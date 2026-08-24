# Bing Daily Wallpaper: Requirements and High-Level Design

## Summary

Add a built-in **Bing Daily Wallpaper** collection to the Slideshow tab. Unlike
Git-backed collections, this collection is managed by the application and
contains the current Bing homepage image.

The application checks once per main-loop iteration whether today's image has
already been downloaded. The persistent cache prevents repeated API and image
requests during the same day. When Bing Daily Wallpaper is active, the normal
slideshow rotation interval is ignored and the TV is synchronized to exactly
one image: the current Bing daily wallpaper.

## Requirements

### Collection picker

- Show `Bing Daily Wallpaper` as the first collection entry in
  `.ftv-dropdown-wrap`.
- Treat it as a built-in collection; it must not depend on
  `SAMSUNG_TV_ART_COLLECTIONS`, `collections.list`, or a Git repository.
- Make its selection mutually exclusive:
  - Selecting Bing Daily Wallpaper clears every other selected collection.
  - Selecting any regular collection clears Bing Daily Wallpaper.
  - `Select All` applies only to regular collections and clears Bing Daily
    Wallpaper.
- Continue to support multi-select behavior among regular collections.
- Use these stable values:
  - Display label: `Bing Daily Wallpaper`
  - Directory/collection ID: `Bing_DailyWallpaper`

### Daily download

- Request Bing metadata from:

  ```text
  https://www.bing.com/HPImageArchive.aspx?format=js&idx=0&n=1&mkt=en-US
  ```

- Read the first object in the response's `images` array.
- Derive the image identifier from `urlbase`. For example:

  ```text
  /th?id=OHR.BKBridge_EN-US2923468858
  ```

- Download the UHD image from the fixed Bing host by appending `_UHD.jpg`:

  ```text
  https://www.bing.com/th?id=OHR.BKBridge_EN-US2923468858_UHD.jpg
  ```

- Save the image under:

  ```text
  <media_root>\Bing_DailyWallpaper\
  ```

- Keep only the current daily image in that directory. Remove the previous
  image only after the replacement has downloaded and validated successfully.
- Store enough metadata for the UI and MQTT state, including:
  - Bing `startdate`
  - local filename and relative path
  - title
  - copyright text
  - copyright link
  - source image identifier or `urlbase`
  - successful download timestamp

### Daily cache and scheduling

- Persist Bing state separately from the existing TV upload cache, for example:

  ```text
  /data/bing_daily_wallpaper.json
  ```

- On each normal main-loop iteration:
  1. Read the cached successful Bing date.
  2. If today's image has already been downloaded and the cached file exists,
     perform no network request.
  3. Otherwise, fetch metadata and download the image.
  4. Atomically replace the image and cache only after the complete operation
     succeeds.
- A missing, malformed, or stale cache must cause a refresh attempt.
- If the calendar date has changed but Bing still returns the previously cached
  `startdate`, keep the previous image and retry later. Do not mark the new day
  complete until Bing publishes a different daily image.
- A failed API or image request must not be recorded as a successful daily
  download. Keep the previous valid image and retry later.
- The daily check runs regardless of whether the Bing collection is currently
  selected, so the current image is ready when the user selects it.
- Network operations must not block the asyncio event loop; run the synchronous
  manager operation in an executor/thread or use an asynchronous HTTP client
  already present in the project.
- Use finite connection/read timeouts and a bounded response size.

### Daily TV mode

Daily mode is active only when:

```text
selected_collections == ["Bing_DailyWallpaper"]
```

and the user applies an image from that collection.

While daily mode is active:

- Ignore `SAMSUNG_TV_ART_UPDATE_MINUTES`.
- Resolve the expected image from the Bing daily cache rather than from a
  random or sequential slideshow selection.
- Verify that the expected image is represented by a valid cached upload on the
  TV.
- If the current daily image is not on the TV:
  1. Select or upload standby as required by the existing safe replacement
     workflow.
  2. Delete all other user-uploaded slideshow images.
  3. Upload the one current Bing image.
  4. Select that image once the upload completes.
  5. Persist its TV content ID and file signature in the normal upload cache.
- If the expected image is already on the TV, do not upload it again.
- Do not repeatedly re-select the image on every loop iteration.
- When a new Bing image is downloaded on a later date and daily mode remains
  active, mark synchronization pending and replace the previous TV image when
  the TV is next available in Art Mode.
- If the TV is off or outside Art Mode, retain the pending daily synchronization
  and retry when Art Mode becomes available.
- Selecting regular collections exits daily mode and restores existing
  random/sequential rotation behavior.

## Proposed Architecture

### 1. Bing utility service

Add a focused module such as:

```text
loop\BingDailyWallpaperManager.py
```

Use a `BingDailyWallpaperManager` service as the single owner of all
Bing-specific behavior:

- determining whether Bing daily mode is active;
- deciding whether today's fetch is required;
- requesting and validating Bing metadata;
- deriving a safe image URL from a validated Bing image identifier;
- downloading to a temporary file;
- validating the downloaded image;
- atomically replacing the current local image;
- writing and loading the dedicated cache;
- coordinating the one-image TV synchronization through the uploader's existing
  TV primitives;
- tracking whether a daily TV synchronization is pending;
- bypassing normal slideshow rotation while daily mode is active;
- returning a typed result describing `unchanged`, `downloaded`, `refreshed`,
  or `failed`.

The manager receives the uploader host through constructor injection. It may
call narrow, existing uploader operations for Art Mode checks, standby
selection, cleanup, upload, selection, cache persistence, and MQTT publication.
It must not duplicate Samsung TV protocol logic.

Suggested result model:

```python
@dataclass(frozen=True)
class BingDailyResult:
    status: str
    date: str | None
    relative_path: str | None
    metadata: dict[str, str]
```

### 2. Uploader orchestration

`monitor_and_display` must remain free of Bing implementation details. It only:

- constructs `BingDailyWallpaperManager`, passing itself as the host;
- calls one manager entry point from the main loop;
- delegates collection-selection normalization and applied-selection handling
  to the manager where needed.

The manager exposes the Bing-specific operations:

```python
class BingDailyWallpaperManager:
    def is_daily_mode(self) -> bool: ...
    async def tick(self) -> None: ...
    async def sync_to_tv(self) -> None: ...
```

`tick()` is the facade used by the uploader loop. It performs the cached daily
check and, when required, stages synchronization through the existing pending
override workflow. This keeps
`uploader.py` limited to a single generic delegation point instead of adding
`is_bing_daily_mode()`, `check_bing_daily()`, or
`sync_bing_daily_to_tv()` methods there.

Network I/O inside the manager must run without blocking asyncio. TV operations
must continue to use the uploader's existing locks and methods rather than
opening a second TV connection.

### 3. Collection discovery

The built-in collection should be injected by collection discovery rather than
pretending it is a Git source:

- ensure `<media_root>\Bing_DailyWallpaper` exists;
- include `Bing_DailyWallpaper` even before the first successful download;
- publish `Bing Daily Wallpaper` as the first display option;
- continue scanning flat and grouped disk collections as currently implemented;
- exclude the Bing directory from normal alphabetical duplication.

Centralize the collection ID and display label in backend constants. Mirror the
same stable ID in the browser code because the UI is currently a standalone
HTML/JavaScript client without a shared module system.

### 4. UI selection policy

Add one collection-selection helper that normalizes every staged update:

```javascript
function normalizeExclusiveCollectionSelection(next, changedValue) { ... }
```

All picker actions should go through it, including:

- clicking Bing Daily Wallpaper;
- clicking a regular collection;
- Select All;
- retained MQTT selection updates;
- restored browser state.

This avoids scattering special-case checkbox behavior across event handlers.
The backend must enforce the same invariant because MQTT commands can originate
outside the web UI.

### 5. Metadata integration

The manager should write a one-row `artwork_data.csv` in the Bing collection
directory using the existing schema:

```csv
artwork_file,artwork_dir,collection_name,artist_name,artwork_title,artwork_description
<filename>,Bing_DailyWallpaper,Bing Daily Wallpaper,Bing,<title>,<copyright>
```

After a successful download, update or reload the runtime metadata index so the
new image immediately appears with its title and copyright information. A
container restart must not be required.

## State Model

Example dedicated cache:

```json
{
  "version": 1,
  "startdate": "20260824",
  "downloaded_at": "2026-08-24T18:05:00Z",
  "filename": "OHR.BKBridge_EN-US2923468858_UHD.jpg",
  "relative_path": "Bing_DailyWallpaper/OHR.BKBridge_EN-US2923468858_UHD.jpg",
  "urlbase": "/th?id=OHR.BKBridge_EN-US2923468858",
  "title": "Crossing into history",
  "copyright": "Brooklyn Bridge, New York City (© shayes17/Getty Images)",
  "copyrightlink": "https://www.bing.com/search?q=Brooklyn+Bridge"
}
```

The dedicated cache answers which Bing image should exist locally. The existing
uploaded-files cache remains authoritative for the relationship between that
local file and its Samsung TV content ID.

## Validation and Safety

- Accept only an `images` array with a valid first object.
- Extract only a constrained Bing `OHR.*` identifier from `urlbase`; never
  download an arbitrary host supplied by API data.
- Reject path separators and traversal components in generated filenames.
- Require a successful HTTP status and an image response.
- Validate the completed temporary file before replacing the current image.
- Use atomic writes for both image and JSON cache updates.
- Serialize download attempts so the main loop and manual actions cannot fetch
  concurrently.
- Preserve the last known-good image and cache on any failure.
- Log failures with enough context to diagnose them, but do not treat failure as
  a successful daily check.

## Acceptance Criteria

1. Bing Daily Wallpaper is always the first collection option.
2. Bing and regular collections cannot be selected together in either the UI or
   backend state.
3. The Bing API and image are fetched no more than once after a successful
   download for a given day.
4. The image is saved under `media\Bing_DailyWallpaper\` and appears in the
   Slideshow image grid.
5. Applying the Bing image activates daily mode and leaves exactly that daily
   image as the active slideshow image on the TV.
6. Daily mode does not rotate according to
   `SAMSUNG_TV_ART_UPDATE_MINUTES`.
7. A newly downloaded image replaces the previous TV image automatically when
   daily mode is active and the TV is available in Art Mode.
8. Switching to any regular collection restores the existing slideshow
   behavior without affecting those collections.
9. API, download, cache, or TV failures preserve the previous usable state and
   remain retryable.
