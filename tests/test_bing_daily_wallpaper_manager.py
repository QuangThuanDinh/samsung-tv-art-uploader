import asyncio
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
        metadata = {
            'images': [{
                'startdate': '20260824',
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
        self.assertTrue(
            os.path.isfile(
                os.path.join(
                    self.media_root,
                    BING_COLLECTION_ID,
                    'artwork_data.csv',
                )
            )
        )

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
            self.assertEqual(json.load(source), cached)

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

    def test_daily_rollover_marks_existing_daily_mode_pending(self):
        old_path = f'{BING_COLLECTION_ID}/OHR.Old_UHD.jpg'
        new_path = f'{BING_COLLECTION_ID}/OHR.New_UHD.jpg'
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


if __name__ == '__main__':
    unittest.main()
