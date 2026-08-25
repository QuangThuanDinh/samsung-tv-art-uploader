"""Built-in Bing Daily Wallpaper collection and synchronization service."""

import asyncio
import csv
import datetime
import json
import os
import re
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, Optional

from PIL import Image


BING_COLLECTION_ID = 'Bing_DailyWallpaper'
BING_COLLECTION_LABEL = 'Bing Daily Wallpaper'
BING_METADATA_URL = (
    'https://www.bing.com/HPImageArchive.aspx?format=js&idx=0&n=1&mkt=en-US'
)
BING_CACHE_VERSION = 3
BING_HISTORY_LIMIT = 30
_BING_ID_PATTERN = re.compile(r'^/?th\?id=(OHR\.[A-Za-z0-9_-]+)$')


@dataclass(frozen=True)
class BingDailyResult:
    status: str
    date: Optional[str] = None
    relative_path: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)


class BingDailyWallpaperManager:
    """Own Bing download, cache, selection, and daily TV synchronization."""

    def __init__(self, host):
        self.host = host
        self.log = host.log.getChild('BingDailyWallpaper')
        self.collection_dir = os.path.join(host.media_root, BING_COLLECTION_ID)
        self.cache_path = os.environ.get(
            'SAMSUNG_TV_ART_BING_CACHE_FILE',
            '/data/bing_daily_wallpaper.json',
        )
        self.request_timeout = max(
            1,
            int(os.environ.get('SAMSUNG_TV_ART_BING_TIMEOUT_SECONDS', '20')),
        )
        self._download_lock = threading.Lock()
        self._last_failed_attempt = 0.0
        self._failure_count = 0
        try:
            os.makedirs(self.collection_dir, exist_ok=True)
        except OSError as exc:
            self.log.warning('Unable to create Bing collection directory: %s', exc)

    def normalize_collections(self, collections):
        """Enforce Bing as an exclusive collection for all command sources."""
        normalized = []
        bing_selected = False
        for value in collections or []:
            item = str(value or '').strip()
            if not item:
                continue
            if self.is_bing_collection(item):
                bing_selected = True
                continue
            if item not in normalized:
                normalized.append(item)
        return [BING_COLLECTION_ID] if bing_selected else normalized

    def display_collection(self, collection):
        return (
            BING_COLLECTION_LABEL
            if collection == BING_COLLECTION_ID
            else collection
        )

    def is_daily_collection_selection(self, collections):
        return self.normalize_collections(collections) == [BING_COLLECTION_ID]

    def add_collection(self, collections, collection):
        if self.is_bing_collection(collection):
            return [BING_COLLECTION_ID]
        regular = [
            value
            for value in collections or []
            if not self.is_bing_collection(value)
        ]
        regular.append(collection)
        return self.normalize_collections(regular)

    @staticmethod
    def is_bing_collection(value):
        key = re.sub(r'[^a-z0-9]', '', str(value or '').lower())
        return key == 'bingdailywallpaper'

    def order_collection_options(self, options):
        regular = [
            option
            for option in options
            if not self.is_bing_collection(option)
        ]
        return [BING_COLLECTION_LABEL] + sorted(
            set(regular),
            key=lambda value: value.lower(),
        )

    def is_daily_mode(self):
        selected = self.normalize_collections(self.host.selected_collections)
        override = self.host.slideshow_override or []
        return (
            selected == [BING_COLLECTION_ID]
            and bool(override)
            and all(self._is_bing_path(path) for path in override)
        )

    def register_cached_metadata(self):
        state = self._load_cache()
        if state:
            self._register_history_metadata(state)

    async def tick(self):
        """Ensure today's image exists and stage active daily mode for TV sync."""
        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, self.ensure_today)

            if result.metadata:
                if result.status in ('downloaded', 'refreshed'):
                    self._register_history_metadata(self._load_cache())
                else:
                    self._register_metadata(result.metadata)

            if result.status in ('downloaded', 'refreshed'):
                if result.status == 'downloaded':
                    self.log.info(
                        'Downloaded Bing Daily Wallpaper for %s',
                        result.date,
                    )
                if hasattr(self.host, '_collection_file_cache'):
                    self.host._collection_file_cache.pop(self.collection_dir, None)
                self.host._publish_collections_state()
                self.host._publish_slideshow_available()
                if (
                    self.is_daily_mode()
                    and self.host.slideshow_override != [result.relative_path]
                ):
                    self._mark_sync_pending(
                        result.relative_path,
                        force_reupload=self._requires_full_replace(
                            result.relative_path
                        ),
                    )
        except Exception as exc:
            self.log.warning('Bing Daily Wallpaper loop check failed: %s', exc)

    async def apply_selection(
        self,
        req_id=None,
        ack_cmd='slideshow/override/set',
        force_reupload=False,
    ):
        """Activate daily mode using the current cached Bing image."""
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self.ensure_today, True)
        if result.metadata:
            if result.status in ('downloaded', 'refreshed'):
                self._register_history_metadata(self._load_cache())
            else:
                self._register_metadata(result.metadata)
        if result.status == 'failed' or not result.relative_path:
            self.host._publish_ack(
                ack_cmd,
                'error',
                'Bing Daily Wallpaper is unavailable',
                req_id,
            )
            return
        await self.sync_to_tv(
            req_id=req_id,
            ack_cmd=ack_cmd,
            expected_path=result.relative_path,
            force_reupload=(
                force_reupload
                or self._requires_full_replace(result.relative_path)
            ),
        )

    async def sync_to_tv(
        self,
        req_id=None,
        ack_cmd='slideshow/override/set',
        expected_path=None,
        force_reupload=False,
    ):
        """Make the current Bing image the sole daily slideshow upload."""
        state = self._load_cache()
        current = self._current_entry(state)
        path = expected_path or current.get('relative_path')
        if not path or not os.path.isfile(os.path.join(self.host.media_root, path)):
            self.log.warning('Bing daily sync deferred: cached image is unavailable')
            if req_id is not None:
                self.host._publish_ack(
                    ack_cmd,
                    'error',
                    'Bing Daily Wallpaper image is unavailable',
                    req_id,
                )
            return
        if self.host._refresh_in_progress or self.host._collections_sync_running:
            if req_id is not None:
                self.host._publish_ack(
                    ack_cmd,
                    'error',
                    'Another upload or refresh is already running',
                    req_id,
                )
            return

        needs_sync = (
            force_reupload
            or self.host.slideshow_override_pending
            or self.host.slideshow_override != [path]
            or bool(self.host._slideshow_paths_requiring_upload([path]))
        )
        if not needs_sync:
            self._commit_daily_selection()
            if req_id is not None:
                self.host._publish_ack(
                    ack_cmd,
                    'ok',
                    'Bing Daily Wallpaper is already active',
                    req_id,
                )
            return

        await self.host._apply_slideshow_override(
            [path],
            req_id=req_id,
            new_collections=[BING_COLLECTION_ID],
            max_uploads=None,
            force_reupload=force_reupload or self._requires_full_replace(path),
            ack_cmd=ack_cmd,
        )

    def _commit_daily_selection(self):
        self.host.selected_collections = [BING_COLLECTION_ID]
        desired = self.host.get_selected_folder()
        if os.path.isdir(desired):
            self.host.folder = desired
        self.host._pending_selection_change = False
        self.host._publish_selected_collections_state()
        self.host._cache_selected_collections()

    def record_regenerated_derivative(self, generated):
        """Persist the signature/path for a manually regenerated Bing image."""
        self.record_regenerated_derivatives([generated])

    def record_regenerated_derivatives(self, generated_items):
        """Persist manually regenerated Bing derivatives in one cache update."""
        state = self._load_cache()
        if not state or not generated_items:
            return
        by_source = {
            item.get('source_relative_path'): item
            for item in state.get('history', [])
            if item.get('source_relative_path')
        }
        changed = False
        for generated in generated_items:
            entry = by_source.get(generated.get('source_path'))
            if entry is None:
                continue
            entry['filename'] = os.path.basename(generated['path'])
            entry['relative_path'] = generated['path']
            entry['museum_label_signature'] = generated['signature']
            changed = True
        if not changed:
            return
        self._write_collection_csv(state)
        self._write_json_atomic(self.cache_path, state)
        self._register_history_metadata(state)

    def ensure_today(self, force_retry=False):
        """Fetch and persist today's Bing image unless a valid cache already exists."""
        with self._download_lock:
            today = datetime.date.today().strftime('%Y%m%d')
            cached = {}
            current = {}
            try:
                cached = self._load_cache()
                cached = self._prepare_cached_history(cached)
                current = self._current_entry(cached)
                if self._cache_is_current(cached, today):
                    return self._result('unchanged', current)
                if not force_retry and not self._failure_retry_due():
                    return self._result('failed', current)

                os.makedirs(self.collection_dir, exist_ok=True)
                metadata = self._fetch_metadata()
                source_date = str(metadata.get('startdate') or '')
                if not source_date:
                    raise ValueError('Bing metadata contains no startdate')
                if (
                    current
                    and current.get('startdate') == source_date
                    and self._cached_file_exists(current)
                ):
                    self._record_retry_delay()
                    self.log.info(
                        'Bing has not published a new daily image yet; '
                        'keeping %s and retrying later',
                        current.get('filename'),
                    )
                    return self._result('not_available', current)
                image_id = self._extract_image_id(metadata.get('urlbase'))
                source_filename = f'{image_id}_UHD.jpg'
                source_relative_path = (
                    f'{BING_COLLECTION_ID}/{source_filename}'
                )
                destination = os.path.join(
                    self.collection_dir,
                    source_filename,
                )

                image_downloaded = not os.path.isfile(destination)
                if image_downloaded:
                    self._download_image(image_id, destination)

                entry = {
                    'startdate': source_date,
                    'downloaded_at': datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    'filename': source_filename,
                    'relative_path': source_relative_path,
                    'source_filename': source_filename,
                    'source_relative_path': source_relative_path,
                    'urlbase': str(metadata.get('urlbase') or ''),
                    'title': str(metadata.get('title') or ''),
                    'copyright': str(metadata.get('copyright') or ''),
                    'copyrightlink': str(metadata.get('copyrightlink') or ''),
                }
                entry = self._prepare_cached_variant(entry)
                replaced = [
                    item for item in cached.get('history', [])
                    if item.get('startdate') == source_date
                ]
                history = [
                    item for item in cached.get('history', [])
                    if item.get('startdate') != source_date
                ]
                history.append(entry)
                history = self._sort_history(history)
                retained = history[:BING_HISTORY_LIMIT]
                pruned = history[BING_HISTORY_LIMIT:]
                state = {
                    'version': BING_CACHE_VERSION,
                    'checked_date': today,
                    'current_startdate': source_date,
                    'history': retained,
                }
                self._write_collection_csv(state)
                self._write_json_atomic(self.cache_path, state)
                self._remove_history_files(pruned + replaced, retained)
                self._remove_untracked_images(retained)
                self._last_failed_attempt = 0.0
                self._failure_count = 0
                return self._result(
                    'downloaded' if image_downloaded else 'refreshed',
                    entry,
                )
            except Exception as exc:
                self._record_retry_delay()
                self.log.warning('Bing Daily Wallpaper refresh failed: %s', exc)
                return self._result('failed', current)

    def _prepare_cached_history(self, state):
        if not state:
            return state
        original = json.dumps(state, sort_keys=True)
        history = self._sort_history([
            entry for entry in state.get('history', [])
            if isinstance(entry, dict)
        ])
        retained_history = history[:BING_HISTORY_LIMIT]
        pruned = history[BING_HISTORY_LIMIT:]
        prepared = []
        for entry in retained_history:
            prepared.append(self._prepare_cached_variant(entry))
        state = dict(state)
        state['version'] = BING_CACHE_VERSION
        state['history'] = self._sort_history(prepared)
        current = state['history'][0] if state['history'] else {}
        if current:
            state['current_startdate'] = current.get('startdate')
        else:
            state['current_startdate'] = ''
        if json.dumps(state, sort_keys=True) != original:
            self._write_collection_csv(state)
            self._write_json_atomic(self.cache_path, state)
        if pruned:
            self._remove_history_files(pruned, state['history'])
        return state

    def _prepare_cached_variant(self, state):
        """Select or generate the active source/derivative for cached metadata."""
        if not state:
            return state
        state = dict(state)
        active_filename = state.get('filename')
        source_filename = state.get('source_filename')
        if not source_filename and active_filename:
            source_filename = (
                self.host.museum_labels.source_filename(active_filename)
                if self.host.museum_labels.is_derivative(active_filename)
                else active_filename
            )
        if not source_filename:
            return state
        source_path = os.path.join(self.collection_dir, source_filename)
        if not os.path.isfile(source_path):
            return state
        if (
            not self.host.museum_labels.enabled
            and active_filename == source_filename
            and not state.get('source_filename')
        ):
            return state

        source_relative_path = f'{BING_COLLECTION_ID}/{source_filename}'
        state['source_filename'] = source_filename
        state['source_relative_path'] = source_relative_path
        if self.host.museum_labels.enabled:
            destination = os.path.join(
                self.collection_dir,
                self.host.museum_labels.derivative_filename(source_filename),
            )
            if not os.path.isfile(destination):
                destination = self.host.museum_labels.process_image(
                    source_path,
                    state,
                    destination,
                )
                state['museum_label_signature'] = (
                    self.host.museum_labels.image_signature(
                        source_path,
                        state,
                    )
                )
            filename = os.path.basename(destination)
            relative_path = f'{BING_COLLECTION_ID}/{filename}'
        else:
            state.pop('museum_label_signature', None)
            filename = source_filename
            relative_path = source_relative_path
        state['filename'] = filename
        state['relative_path'] = relative_path
        return state

    @staticmethod
    def _sort_history(history):
        return sorted(
            history,
            key=lambda item: (
                str(item.get('startdate') or ''),
                str(item.get('downloaded_at') or ''),
            ),
            reverse=True,
        )

    @staticmethod
    def _current_entry(state):
        if not state:
            return {}
        history = state.get('history') or []
        current_startdate = state.get('current_startdate')
        if current_startdate:
            for entry in history:
                if entry.get('startdate') == current_startdate:
                    return entry
        return history[0] if history else {}

    def preview_filenames(self, filenames):
        """Order Bing preview files by cached history, newest first."""
        available = set(filenames)
        ordered = []
        for entry in self._load_cache().get('history', []):
            for filename in (
                entry.get('filename'),
                entry.get('source_filename'),
            ):
                if filename in available and filename not in ordered:
                    ordered.append(filename)
                    break
        ordered.extend(sorted(available - set(ordered)))
        return ordered

    def current_relative_path(self):
        return self._current_entry(self._load_cache()).get('relative_path')

    def _record_retry_delay(self):
        self._last_failed_attempt = time.monotonic()
        self._failure_count += 1

    def _failure_retry_due(self):
        if not self._last_failed_attempt:
            return True
        delay = min(3600, 900 * (2 ** min(self._failure_count - 1, 2)))
        return time.monotonic() - self._last_failed_attempt >= delay

    def _requires_full_replace(self, expected_path):
        uploaded_paths = {
            record.get('path_rel', key)
            for key, record in self.host.uploaded_files.items()
        }
        return uploaded_paths != {expected_path}

    def _mark_sync_pending(self, path, force_reupload):
        if not path:
            return
        self.host.selected_collections = [BING_COLLECTION_ID]
        self.host.slideshow_override = [path]
        self.host.slideshow_override_pending = True
        self.host.slideshow_override_force_reupload = force_reupload
        self.host._save_slideshow_override()
        self.host._cache_selected_collections()
        self.host._publish_slideshow_state()

    def _register_metadata(self, state):
        filename = state.get('filename')
        relative_path = state.get('relative_path')
        if not filename or not relative_path:
            return
        if not hasattr(self.host, '_csv_by_file'):
            self.host._csv_by_file = {}
        if not hasattr(self.host, '_csv_by_path'):
            self.host._csv_by_path = {}
        if not hasattr(self.host, '_csv_headers'):
            self.host._csv_headers = []
        if not hasattr(self.host, '_dir_to_artist'):
            self.host._dir_to_artist = {}
        if not hasattr(self.host, '_artist_to_dir'):
            self.host._artist_to_dir = {}
        row = {
            'artwork_file': filename,
            'artwork_dir': BING_COLLECTION_ID,
            'collection_name': BING_COLLECTION_LABEL,
            'artist_name': 'Bing',
            'artist_lifespan': '',
            'artwork_title': state.get('title', ''),
            'artwork_year': (state.get('startdate') or '')[:4],
            'artwork_medium': 'Photography',
            'artwork_description': state.get('copyright', ''),
            'copyrightlink': state.get('copyrightlink', ''),
        }
        self.host._csv_by_file[filename] = row
        self.host._csv_by_path[relative_path] = row
        self.host._dir_to_artist[BING_COLLECTION_ID] = BING_COLLECTION_LABEL
        self.host._artist_to_dir[BING_COLLECTION_LABEL] = BING_COLLECTION_ID
        for header in row:
            if header not in self.host._csv_headers:
                self.host._csv_headers.append(header)

    def _register_history_metadata(self, state):
        if not state:
            return
        for key, row in list(getattr(self.host, '_csv_by_path', {}).items()):
            if row.get('artwork_dir') == BING_COLLECTION_ID:
                self.host._csv_by_path.pop(key, None)
        for key, row in list(getattr(self.host, '_csv_by_file', {}).items()):
            if row.get('artwork_dir') == BING_COLLECTION_ID:
                self.host._csv_by_file.pop(key, None)
        for entry in state.get('history', []):
            self._register_metadata(entry)

    def _fetch_metadata(self):
        payload = self._request_bytes(BING_METADATA_URL, max_bytes=1024 * 1024)
        data = json.loads(payload.decode('utf-8'))
        images = data.get('images')
        if not isinstance(images, list) or not images or not isinstance(images[0], dict):
            raise ValueError('Bing metadata response has no image')
        return images[0]

    def _download_image(self, image_id, destination):
        url = f'https://www.bing.com/th?id={image_id}_UHD.jpg'
        payload = self._request_bytes(url, max_bytes=50 * 1024 * 1024)
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        fd, temp_path = tempfile.mkstemp(
            prefix='.bing-',
            suffix='.jpg',
            dir=os.path.dirname(destination),
        )
        try:
            with os.fdopen(fd, 'wb') as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            with Image.open(temp_path) as image:
                image.verify()
                if image.format != 'JPEG':
                    raise ValueError(f'Unexpected Bing image format: {image.format}')
            os.replace(temp_path, destination)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _request_bytes(self, url, max_bytes):
        request = urllib.request.Request(
            url,
            headers={'User-Agent': 'samsung-tv-art-uploader/1.0'},
        )
        with urllib.request.urlopen(request, timeout=self.request_timeout) as response:
            content_type = response.headers.get_content_type()
            if url == BING_METADATA_URL and content_type != 'application/json':
                raise ValueError(f'Unexpected Bing metadata content type: {content_type}')
            if url != BING_METADATA_URL and not content_type.startswith('image/'):
                raise ValueError(f'Unexpected Bing image content type: {content_type}')
            payload = response.read(max_bytes + 1)
            if len(payload) > max_bytes:
                raise ValueError('Bing response exceeds the configured safety limit')
            return payload

    def _write_collection_csv(self, state):
        path = os.path.join(self.collection_dir, 'artwork_data.csv')
        fields = [
            'artwork_file',
            'artwork_dir',
            'collection_name',
            'artist_name',
            'artist_lifespan',
            'artwork_title',
            'artwork_year',
            'artwork_medium',
            'artwork_description',
            'copyrightlink',
        ]
        fd, temp_path = tempfile.mkstemp(
            prefix='.artwork-data-',
            suffix='.csv',
            dir=self.collection_dir,
            text=True,
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8', newline='') as output:
                writer = csv.DictWriter(output, fieldnames=fields)
                writer.writeheader()
                for entry in state.get('history', []):
                    if not entry.get('filename'):
                        continue
                    writer.writerow({
                        'artwork_file': entry['filename'],
                        'artwork_dir': BING_COLLECTION_ID,
                        'collection_name': BING_COLLECTION_LABEL,
                        'artist_name': 'Bing',
                        'artist_lifespan': '',
                        'artwork_title': entry.get('title', ''),
                        'artwork_year': (entry.get('startdate') or '')[:4],
                        'artwork_medium': 'Photography',
                        'artwork_description': entry.get('copyright', ''),
                        'copyrightlink': entry.get('copyrightlink', ''),
                    })
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _remove_history_files(self, pruned, retained):
        retained_files = {
            filename
            for entry in retained
            for filename in self._entry_owned_filenames(entry)
        }
        for entry in pruned:
            for filename in self._entry_owned_filenames(entry):
                if filename in retained_files:
                    continue
                path = os.path.join(self.collection_dir, filename)
                if os.path.isfile(path):
                    try:
                        os.remove(path)
                    except OSError as exc:
                        self.log.warning(
                            'Unable to prune Bing history image %s: %s',
                            filename,
                            exc,
                        )

    def _entry_owned_filenames(self, entry):
        filename = entry.get('filename')
        source = entry.get('source_filename')
        if not source and filename:
            source = (
                self.host.museum_labels.source_filename(filename)
                if self.host.museum_labels.is_derivative(filename)
                else filename
            )
        names = {
            filename,
            source,
        } - {None, ''}
        if source:
            names.add(self.host.museum_labels.derivative_filename(source))
        return names

    def _remove_untracked_images(self, retained):
        if not retained:
            return
        keep = {
            filename
            for entry in retained
            for filename in self._entry_owned_filenames(entry)
        }
        for filename in os.listdir(self.collection_dir):
            path = os.path.join(self.collection_dir, filename)
            if (
                filename not in keep
                and os.path.isfile(path)
                and os.path.splitext(filename)[1].lower()
                in {'.jpg', '.jpeg', '.png', '.bmp', '.tif', '.tiff'}
            ):
                try:
                    os.remove(path)
                except OSError as exc:
                    self.log.warning(
                        'Unable to remove untracked Bing image %s: %s',
                        filename,
                        exc,
                    )

    def _cache_is_current(self, state, today):
        if not state or state.get('checked_date') != today:
            return False
        return self._cached_file_exists(self._current_entry(state))

    def _cached_file_exists(self, state):
        relative_path = (state or {}).get('relative_path')
        return bool(
            relative_path
            and self._is_bing_path(relative_path)
            and os.path.isfile(os.path.join(self.host.media_root, relative_path))
        )

    def _load_cache(self):
        try:
            with open(self.cache_path, 'r', encoding='utf-8') as source:
                state = json.load(source)
            if not isinstance(state, dict):
                return {}
            if state.get('version') == BING_CACHE_VERSION:
                history = [
                    entry for entry in state.get('history', [])
                    if isinstance(entry, dict)
                ]
                normalized = dict(state)
                normalized['history'] = self._sort_history(history)
                return normalized
            if not state.get('startdate') and not state.get('relative_path'):
                return {}
            entry = {
                key: value for key, value in state.items()
                if key not in {
                    'version',
                    'checked_date',
                    'current_startdate',
                    'history',
                }
            }
            migrated = {
                'version': BING_CACHE_VERSION,
                'checked_date': state.get('checked_date', ''),
                'current_startdate': entry.get('startdate', ''),
                'history': [entry],
            }
            self._write_json_atomic(self.cache_path, migrated)
            return migrated
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            self.log.warning('Unable to read Bing daily cache: %s', exc)
            return {}

    def _write_json_atomic(self, path, value):
        directory = os.path.dirname(path) or '.'
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(
            prefix='.bing-cache-',
            suffix='.json',
            dir=directory,
            text=True,
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as output:
                json.dump(value, output, ensure_ascii=False, indent=2)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    @staticmethod
    def _extract_image_id(urlbase):
        match = _BING_ID_PATTERN.fullmatch(str(urlbase or ''))
        if not match:
            raise ValueError('Bing metadata contains an invalid urlbase')
        return match.group(1)

    @staticmethod
    def _is_bing_path(path):
        normalized = str(path or '').replace('\\', '/')
        return normalized.startswith(f'{BING_COLLECTION_ID}/')

    @staticmethod
    def _result(status, state):
        state = state or {}
        return BingDailyResult(
            status=status,
            date=state.get('startdate'),
            relative_path=state.get('relative_path'),
            metadata=state,
        )
