import json
import os
import tempfile
import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class UploadReplacementTests(unittest.IsolatedAsyncioTestCase):
    def make_host(self, data_dir):
        host = monitor_and_display.__new__(monitor_and_display)
        host.pending_delete_path = os.path.join(
            data_dir,
            'pending_tv_delete_ids.json',
        )
        host.delete_delay_seconds = 0
        host.post_delete_recovery_seconds = 0
        host.current_content_id = 'old-current'
        host.shown_content_ids = set()
        host.slideshow_override = None
        host.fav = set()
        host.exclude = []
        host.exclude_content_ids = []
        host.uploaded_files = {
            'new.jpg': {
                'content_id': 'new-content',
                'path_rel': 'new.jpg',
            },
        }
        host.tv = mock.Mock()
        host.tv.select_image = mock.AsyncMock()
        host.tv.delete_list = mock.AsyncMock()
        host.update_ha_selected_artwork = mock.AsyncMock()
        host.log = mock.Mock()
        return host

    async def test_selects_replacement_before_old_content_can_be_deleted(self):
        with tempfile.TemporaryDirectory() as data_dir:
            host = self.make_host(data_dir)

            selected = await host._select_replacement({'new-content'})
            await host._delete_tv_upload_ids({'old-current'})

            self.assertEqual(selected, 'new-content')
            self.assertEqual(host.current_content_id, 'new-content')
            host.tv.select_image.assert_awaited_once_with('new-content')
            host.tv.delete_list.assert_awaited_once_with(['old-current'])

    async def test_current_content_is_retained_for_later_cleanup(self):
        with tempfile.TemporaryDirectory() as data_dir:
            host = self.make_host(data_dir)
            host._queue_pending_delete_ids({'old-current', 'other-old'})

            await host._drain_pending_delete_ids()

            host.tv.delete_list.assert_awaited_once_with(['other-old'])
            with open(
                host.pending_delete_path,
                'r',
                encoding='utf-8',
            ) as source:
                self.assertEqual(json.load(source), ['old-current'])


if __name__ == '__main__':
    unittest.main()
