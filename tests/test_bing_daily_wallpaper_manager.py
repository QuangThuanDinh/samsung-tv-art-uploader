import asyncio
import csv
import datetime
import io
import json
import logging
import os
import tempfile
import unittest
from unittest import mock

from PIL import Image

from loop.BingDailyWallpaperManager import (
    BING_COLLECTION_ID,
    BING_COLLECTION_LABEL,
    BING_METADATA_URL,
    BingDailyResult,
    BingDailyWallpaperManager,
)
from loop.MuseumLabelManager import MuseumLabelManager


class _Host:
    def __init__(self, media_root):
        self.media_root = media_root
        self.log = logging.getLogger('test')
        self.selected_collections = []
        self.slideshow_override = None
        self.slideshow_override_pending = False
        self.slideshow_override_force_reupload = False
        self.uploaded_files = {}
        self._refresh_in_progress = False
        self._collections_sync_running = False
        self._pending_selection_change = False
        self._csv_by_file = {}
        self._csv_by_path = {}
        self._csv_headers = []
        self._dir_to_artist = {}
        self._artist_to_dir = {}
        self.folder = media_root
        self._collection_file_cache = {}
        self.acks = []
        self.museum_labels = MuseumLabelManager(self)

    def _publish_ack(self, cmd, status, message, req_id):
        self.acks.append((cmd, status, message, req_id))

    def _publish_collections_state(self):
        pass

    def _publish_slideshow_available(self):
        pass

    def _publish_slideshow_state(self):
        pass

    def _publish_selected_collections_state(self):
        pass

    def _cache_selected_collections(self):
        pass

    def _save_slideshow_override(self):
        pass

    def _slideshow_paths_requiring_upload(self, paths):
        return []

    def get_selected_folder(self):
        return os.path.join(self.media_root, self.selected_collections[0])


class BingDailyWallpaperManagerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.media_root = os.path.join(self.temp_dir.name, 'media')
        os.makedirs(self.media_root)
        self.cache_path = os.path.join(self.temp_dir.name, 'bing-cache.json')
        self.env = mock.patch.dict(
            os.environ,
            {'SAMSUNG_TV_ART_BING_CACHE_FILE': self.cache_path},
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.host = _Host(self.media_root)
        self.manager = BingDailyWallpaperManager(self.host)

    def test_bing_collection_is_exclusive(self):
        self.assertEqual(
            self.manager.normalize_collections(
                ['Monet', BING_COLLECTION_LABEL, 'Renoir']
            ),
            [BING_COLLECTION_ID],
        )
        self.assertEqual(
            self.manager.add_collection([BING_COLLECTION_ID], 'Monet'),
            ['Monet'],
        )
        self.assertEqual(
            self.manager.add_collection(['Monet'], BING_COLLECTION_LABEL),
            [BING_COLLECTION_ID],
        )

    def test_downloads_once_and_reuses_current_daily_cache(self):
        today = datetime.date.today().strftime('%Y%m%d')
        metadata = {
            'images': [{
                'startdate': today,
                'urlbase': '/th?id=OHR.BKBridge_EN-US2923468858',
                'title': 'Crossing into history',
                'copyright': 'Brooklyn Bridge',
                'copyrightlink': 'https://www.bing.com/search?q=Brooklyn+Bridge',
            }],
        }
        image = io.BytesIO()
        Image.new('RGB', (4, 4), color='blue').save(image, format='JPEG')
        responses = {
            BING_METADATA_URL: json.dumps(metadata).encode('utf-8'),
            (
                'https://www.bing.com/th?'
                'id=OHR.BKBridge_EN-US2923468858_UHD.jpg'
            ): image.getvalue(),
        }

        with mock.patch.object(
            self.manager,
            '_request_bytes',
            side_effect=lambda url, max_bytes: responses[url],
        ) as request:
            first = self.manager.ensure_today()
            second = self.manager.ensure_today()

        self.assertEqual(first.status, 'downloaded')
        self.assertEqual(second.status, 'unchanged')
        self.assertEqual(request.call_count, 2)
        self.assertTrue(
            os.path.isfile(os.path.join(self.media_root, first.relative_path))
        )
        self.assertTrue(os.path.isfile(self.cache_path))
        with open(self.cache_path, 'r', encoding='utf-8') as source:
            cached = json.load(source)
        self.assertEqual(cached['version'], 3)
        self.assertEqual(len(cached['history']), 1)
        self.assertEqual(cached['history'][0]['relative_path'], first.relative_path)
        self.assertTrue(
            os.path.isfile(
                os.path.join(
                    self.media_root,
                    BING_COLLECTION_ID,
                    'artwork_data.csv',
                )
            )
        )

    def test_same_check_date_retries_when_bing_image_is_from_previous_day(self):
        today_date = datetime.date.today()
        today = today_date.strftime('%Y%m%d')
        previous_date = (today_date - datetime.timedelta(days=1)).strftime(
            '%Y%m%d'
        )
        filename = 'OHR.Previous_UHD.jpg'
        relative_path = f'{BING_COLLECTION_ID}/{filename}'
        Image.new('RGB', (4, 4), color='green').save(
            os.path.join(self.manager.collection_dir, filename),
            format='JPEG',
        )
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump({
                'version': 3,
                'checked_date': today,
                'current_startdate': previous_date,
                'history': [{
                    'startdate': previous_date,
                    'filename': filename,
                    'relative_path': relative_path,
                    'source_filename': filename,
                    'source_relative_path': relative_path,
                }],
            }, output)

        with mock.patch.object(
            self.manager,
            '_fetch_metadata',
            return_value={
                'startdate': previous_date,
                'urlbase': '/th?id=OHR.Previous',
            },
        ) as fetch:
            result = self.manager.ensure_today()

        self.assertEqual(result.status, 'not_available')
        fetch.assert_called_once_with()

    def test_invalid_metadata_preserves_previous_image(self):
        previous = os.path.join(
            self.manager.collection_dir,
            'OHR.Previous_UHD.jpg',
        )
        Image.new('RGB', (4, 4), color='green').save(previous, format='JPEG')

        with mock.patch.object(
            self.manager,
            '_request_bytes',
            return_value=json.dumps({
                'images': [{'urlbase': 'https://untrusted.example/image'}],
            }).encode('utf-8'),
        ):
            result = self.manager.ensure_today()

        self.assertEqual(result.status, 'failed')
        self.assertTrue(os.path.isfile(previous))
        self.assertFalse(os.path.exists(self.cache_path))

    def test_waits_when_bing_still_returns_previous_daily_image(self):
        filename = 'OHR.Previous_UHD.jpg'
        relative_path = f'{BING_COLLECTION_ID}/{filename}'
        previous = os.path.join(self.manager.collection_dir, filename)
        Image.new('RGB', (4, 4), color='green').save(previous, format='JPEG')
        cached = {
            'version': 1,
            'checked_date': '19000101',
            'startdate': '20260823',
            'filename': filename,
            'relative_path': relative_path,
        }
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump(cached, output)
        metadata = {
            'images': [{
                'startdate': '20260823',
                'urlbase': '/th?id=OHR.Previous',
            }],
        }

        with mock.patch.object(
            self.manager,
            '_request_bytes',
            return_value=json.dumps(metadata).encode('utf-8'),
        ):
            result = self.manager.ensure_today()

        self.assertEqual(result.status, 'not_available')
        self.assertEqual(result.relative_path, relative_path)
        with open(self.cache_path, 'r', encoding='utf-8') as source:
            migrated = json.load(source)
        self.assertEqual(migrated['version'], 3)
        self.assertEqual(migrated['checked_date'], cached['checked_date'])
        self.assertEqual(migrated['history'][0]['relative_path'], relative_path)

    def test_retains_newest_thirty_images_and_prunes_oldest(self):
        history = []
        for day in range(1, 31):
            filename = f'OHR.History{day:02d}_UHD.jpg'
            Image.new('RGB', (4, 4), color='green').save(
                os.path.join(self.manager.collection_dir, filename),
                format='JPEG',
            )
            history.append({
                'startdate': f'202607{day:02d}',
                'downloaded_at': f'2026-07-{day:02d}T12:00:00+00:00',
                'filename': filename,
                'relative_path': f'{BING_COLLECTION_ID}/{filename}',
                'source_filename': filename,
                'source_relative_path': f'{BING_COLLECTION_ID}/{filename}',
            })
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump({
                'version': 3,
                'checked_date': '19000101',
                'current_startdate': '20260730',
                'history': list(reversed(history)),
            }, output)

        metadata = {
            'startdate': '20260825',
            'urlbase': '/th?id=OHR.Newest',
            'title': 'Newest',
            'copyright': 'Newest image',
            'copyrightlink': 'https://www.bing.com/',
        }

        def download(_image_id, destination):
            Image.new('RGB', (4, 4), color='blue').save(
                destination,
                format='JPEG',
            )

        with mock.patch.object(
            self.manager,
            '_fetch_metadata',
            return_value=metadata,
        ), mock.patch.object(
            self.manager,
            '_download_image',
            side_effect=download,
        ):
            result = self.manager.ensure_today()

        self.assertEqual(result.status, 'downloaded')
        with open(self.cache_path, 'r', encoding='utf-8') as source:
            cached = json.load(source)
        self.assertEqual(len(cached['history']), 30)
        self.assertEqual(cached['history'][0]['startdate'], '20260825')
        self.assertEqual(cached['history'][-1]['startdate'], '20260702')
        self.assertFalse(os.path.exists(os.path.join(
            self.manager.collection_dir,
            'OHR.History01_UHD.jpg',
        )))
        self.assertTrue(os.path.exists(os.path.join(
            self.manager.collection_dir,
            'OHR.History02_UHD.jpg',
        )))
        with open(
            os.path.join(self.manager.collection_dir, 'artwork_data.csv'),
            'r',
            encoding='utf-8',
            newline='',
        ) as source:
            rows = list(csv.DictReader(source))
        self.assertEqual(len(rows), 30)
        self.assertEqual(rows[0]['artwork_title'], 'Newest')

    def test_preview_filenames_follow_newest_first_cache_order(self):
        names = [
            'OHR.Newest_UHD.jpg',
            'OHR.Middle_UHD.jpg',
            'OHR.Oldest_UHD.jpg',
        ]
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump({
                'version': 3,
                'checked_date': '20260825',
                'current_startdate': '20260825',
                'history': [
                    {
                        'startdate': f'202608{day:02d}',
                        'filename': filename,
                        'relative_path': f'{BING_COLLECTION_ID}/{filename}',
                    }
                    for day, filename in zip((25, 24, 23), names)
                ],
            }, output)

        ordered = self.manager.preview_filenames(list(reversed(names)))

        self.assertEqual(ordered, names)

    def test_preview_order_uses_source_when_derivative_is_missing(self):
        newest_source = 'OHR.Newest_UHD.jpg'
        newest_derivative = self.host.museum_labels.derivative_filename(
            newest_source
        )
        oldest = 'OHR.Oldest_UHD.jpg'
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump({
                'version': 3,
                'checked_date': '20260825',
                'current_startdate': '20260825',
                'history': [
                    {
                        'startdate': '20260825',
                        'filename': newest_derivative,
                        'relative_path': (
                            f'{BING_COLLECTION_ID}/{newest_derivative}'
                        ),
                        'source_filename': newest_source,
                        'source_relative_path': (
                            f'{BING_COLLECTION_ID}/{newest_source}'
                        ),
                    },
                    {
                        'startdate': '20260824',
                        'filename': oldest,
                        'relative_path': f'{BING_COLLECTION_ID}/{oldest}',
                    },
                ],
            }, output)

        ordered = self.manager.preview_filenames([oldest, newest_source])

        self.assertEqual(ordered, [newest_source, oldest])

    def test_pruning_removes_source_and_museum_label_derivative(self):
        source = 'OHR.Oldest_UHD.jpg'
        derivative = self.host.museum_labels.derivative_filename(source)
        for filename in (source, derivative):
            Image.new('RGB', (4, 4), color='green').save(
                os.path.join(self.manager.collection_dir, filename),
                format='JPEG',
            )

        self.manager._remove_history_files(
            [{
                'startdate': '20260701',
                'filename': derivative,
                'relative_path': f'{BING_COLLECTION_ID}/{derivative}',
            }],
            [],
        )

        self.assertFalse(os.path.exists(os.path.join(
            self.manager.collection_dir,
            source,
        )))
        self.assertFalse(os.path.exists(os.path.join(
            self.manager.collection_dir,
            derivative,
        )))

    def test_registers_metadata_for_every_retained_history_entry(self):
        history = [
            {
                'startdate': '20260825',
                'filename': 'OHR.New_UHD.jpg',
                'relative_path': f'{BING_COLLECTION_ID}/OHR.New_UHD.jpg',
                'title': 'New',
            },
            {
                'startdate': '20260824',
                'filename': 'OHR.Old_UHD.jpg',
                'relative_path': f'{BING_COLLECTION_ID}/OHR.Old_UHD.jpg',
                'title': 'Old',
            },
        ]
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump({
                'version': 3,
                'checked_date': '20260825',
                'current_startdate': '20260825',
                'history': history,
            }, output)

        self.manager.register_cached_metadata()

        self.assertEqual(
            self.host._csv_by_file['OHR.New_UHD.jpg']['artwork_title'],
            'New',
        )
        self.assertEqual(
            self.host._csv_by_file['OHR.Old_UHD.jpg']['artwork_title'],
            'Old',
        )

    def test_noop_sync_acknowledges_and_commits_daily_selection(self):
        filename = 'OHR.Today_UHD.jpg'
        relative_path = f'{BING_COLLECTION_ID}/{filename}'
        Image.new('RGB', (4, 4), color='blue').save(
            os.path.join(self.manager.collection_dir, filename),
            format='JPEG',
        )
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump({
                'filename': filename,
                'relative_path': relative_path,
            }, output)
        self.host.selected_collections = ['Monet']
        self.host.slideshow_override = [relative_path]
        self.host.uploaded_files = {
            relative_path: {'path_rel': relative_path, 'content_id': '1'}
        }

        asyncio.run(self.manager.sync_to_tv(req_id='request-1'))

        self.assertEqual(self.host.selected_collections, [BING_COLLECTION_ID])
        self.assertEqual(self.host.folder, self.manager.collection_dir)
        self.assertEqual(
            self.host.acks[-1],
            (
                'slideshow/override/set',
                'ok',
                'Bing Daily Wallpaper is already active',
                'request-1',
            ),
        )

    def test_busy_sync_does_not_clobber_another_selection(self):
        filename = 'OHR.Today_UHD.jpg'
        relative_path = f'{BING_COLLECTION_ID}/{filename}'
        Image.new('RGB', (4, 4), color='blue').save(
            os.path.join(self.manager.collection_dir, filename),
            format='JPEG',
        )
        with open(self.cache_path, 'w', encoding='utf-8') as output:
            json.dump({
                'filename': filename,
                'relative_path': relative_path,
            }, output)
        self.host.selected_collections = ['Monet']
        self.host.slideshow_override = ['Monet/Water_Lilies.jpg']
        self.host._refresh_in_progress = True

        asyncio.run(self.manager.sync_to_tv(req_id='request-2'))

        self.assertEqual(self.host.selected_collections, ['Monet'])
        self.assertEqual(
            self.host.slideshow_override,
            ['Monet/Water_Lilies.jpg'],
        )
        self.assertEqual(self.host.acks[-1][1], 'error')

    def _write_media(self, relative_path):
        full = os.path.join(self.media_root, relative_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, 'wb') as handle:
            handle.write(b'image-bytes')
        return relative_path

    def test_daily_rollover_marks_existing_daily_mode_pending(self):
        old_path = f'{BING_COLLECTION_ID}/OHR.Old_UHD.jpg'
        new_path = self._write_media(f'{BING_COLLECTION_ID}/OHR.New_UHD.jpg')
        self.host.selected_collections = [BING_COLLECTION_ID]
        self.host.slideshow_override = [old_path]
        self.host.uploaded_files = {
            old_path: {'path_rel': old_path, 'content_id': '1'}
        }
        result = BingDailyResult(
            status='downloaded',
            date='20260824',
            relative_path=new_path,
            metadata={
                'filename': 'OHR.New_UHD.jpg',
                'relative_path': new_path,
                'startdate': '20260824',
            },
        )

        with mock.patch.object(self.manager, 'ensure_today', return_value=result):
            asyncio.run(self.manager.tick())

        self.assertEqual(self.host.slideshow_override, [new_path])
        self.assertTrue(self.host.slideshow_override_pending)
        self.assertTrue(self.host.slideshow_override_force_reupload)

    def _daily_result(self, path, status='unchanged'):
        return BingDailyResult(
            status=status,
            date='20260901',
            relative_path=path,
            metadata={
                'filename': os.path.basename(path),
                'relative_path': path,
                'startdate': '20260901',
            },
        )

    def test_stale_selection_is_corrected_on_a_later_tick(self):
        # ensure_today reports 'unchanged' for the rest of the day once the
        # image is cached. If the download landed while daily mode was briefly
        # inactive, the selection has to be corrected on a later tick or it
        # stays on yesterday's image until the next rollover.
        old_path = f'{BING_COLLECTION_ID}/OHR.Old_UHD.jpg'
        new_path = self._write_media(f'{BING_COLLECTION_ID}/OHR.New_UHD.jpg')
        self.host.selected_collections = [BING_COLLECTION_ID]
        self.host.slideshow_override = [old_path]
        self.host.uploaded_files = {
            old_path: {'path_rel': old_path, 'content_id': '1'}
        }

        with mock.patch.object(
            self.manager,
            'ensure_today',
            return_value=self._daily_result(new_path),
        ):
            asyncio.run(self.manager.tick())

        self.assertEqual(self.host.slideshow_override, [new_path])
        self.assertTrue(self.host.slideshow_override_pending)

    def test_matching_selection_is_left_alone(self):
        path = self._write_media(f'{BING_COLLECTION_ID}/OHR.Today_UHD.jpg')
        self.host.selected_collections = [BING_COLLECTION_ID]
        self.host.slideshow_override = [path]
        self.host.uploaded_files = {path: {'path_rel': path, 'content_id': '1'}}
        self.host.slideshow_override_pending = False

        with mock.patch.object(
            self.manager,
            'ensure_today',
            return_value=self._daily_result(path),
        ):
            asyncio.run(self.manager.tick())

        self.assertEqual(self.host.slideshow_override, [path])
        self.assertFalse(self.host.slideshow_override_pending)

    def test_selection_is_untouched_outside_daily_mode(self):
        path = self._write_media(f'{BING_COLLECTION_ID}/OHR.Today_UHD.jpg')
        self.host.selected_collections = ['Monet']
        self.host.slideshow_override = ['Monet/Water_Lilies.jpg']

        with mock.patch.object(
            self.manager,
            'ensure_today',
            return_value=self._daily_result(path),
        ):
            asyncio.run(self.manager.tick())

        self.assertEqual(
            self.host.slideshow_override,
            ['Monet/Water_Lilies.jpg'],
        )
        self.assertFalse(self.host.slideshow_override_pending)

    def test_missing_image_does_not_become_the_selection(self):
        # Selecting a path that was pruned from disk would only fail later in
        # _apply_slideshow_override.
        old_path = f'{BING_COLLECTION_ID}/OHR.Old_UHD.jpg'
        missing = f'{BING_COLLECTION_ID}/OHR.Gone_UHD.jpg'
        self.host.selected_collections = [BING_COLLECTION_ID]
        self.host.slideshow_override = [old_path]

        with mock.patch.object(
            self.manager,
            'ensure_today',
            return_value=self._daily_result(missing),
        ):
            asyncio.run(self.manager.tick())

        self.assertEqual(self.host.slideshow_override, [old_path])
        self.assertFalse(self.host.slideshow_override_pending)

    def test_failed_lookup_does_not_change_the_selection(self):
        old_path = f'{BING_COLLECTION_ID}/OHR.Old_UHD.jpg'
        new_path = self._write_media(f'{BING_COLLECTION_ID}/OHR.New_UHD.jpg')
        self.host.selected_collections = [BING_COLLECTION_ID]
        self.host.slideshow_override = [old_path]

        with mock.patch.object(
            self.manager,
            'ensure_today',
            return_value=self._daily_result(new_path, status='failed'),
        ):
            asyncio.run(self.manager.tick())

        self.assertEqual(self.host.slideshow_override, [old_path])
        self.assertFalse(self.host.slideshow_override_pending)


if __name__ == '__main__':
    unittest.main()
