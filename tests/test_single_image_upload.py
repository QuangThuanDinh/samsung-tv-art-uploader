import asyncio
import os
import tempfile
import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class SingleImageUploadTests(unittest.IsolatedAsyncioTestCase):
    """The ad-hoc Upload button must put one image on the TV and change
    nothing else: no deletions, no slideshow selection edits and no
    rotation or daily-mode rescheduling."""

    def write_image(self, root, name='pic.jpg'):
        with open(os.path.join(root, name), 'wb') as handle:
            handle.write(b'data')

    def make_host(self, media_root, uploaded=None, live_entries=None):
        host = monitor_and_display.__new__(monitor_and_display)
        host.media_root = media_root
        host.log = mock.Mock()
        host._tv_state_lock = asyncio.Lock()
        host._refresh_in_progress = False
        host._collections_sync_running = False
        host._publish_ack = mock.Mock()
        host.tv = mock.Mock()
        host.tv.select_image = mock.AsyncMock()
        host.tv.delete_list = mock.AsyncMock()
        host.safe_in_artmode = mock.AsyncMock(return_value=True)
        host.get_tv_content_entries = mock.AsyncMock(
            return_value=live_entries if live_entries is not None else [],
        )
        host.uploaded_files = dict(uploaded or {})
        host._slideshow_paths_requiring_upload = mock.Mock(
            side_effect=lambda paths: [
                p for p in paths
                if not any(
                    r.get('path_rel', k) == p
                    for k, r in host.uploaded_files.items()
                )
            ],
        )
        host._cached_upload_matches_tv = mock.AsyncMock(return_value=True)
        host.write_program_data = mock.Mock()
        host.update_ha_selected_artwork = mock.AsyncMock()
        host._publish_slideshow_state = mock.Mock()
        host._publish_slideshow_available = mock.Mock()
        host._publish_current_artwork_state = mock.AsyncMock()
        host.current_content_id = 'old-current'
        host.shown_content_ids = set()

        # Rotation and slideshow state that must survive untouched.
        host.slideshow_override = ['other.jpg']
        host.max_uploads = 3
        host.start = 12345.0
        host._last_slideshow_paths = ['other.jpg']

        async def _upload(paths, *args, **kwargs):
            for path in paths:
                host.uploaded_files[os.path.basename(path)] = {
                    'content_id': 'new-content',
                    'path_rel': path,
                }
            return len(paths)

        host.upload_files = mock.AsyncMock(side_effect=_upload)
        return host

    def ack_calls(self, host):
        return [(c.args[1], c.args[2]) for c in host._publish_ack.call_args_list]

    def assert_rotation_untouched(self, host):
        self.assertEqual(host.slideshow_override, ['other.jpg'])
        self.assertEqual(host.max_uploads, 3)
        self.assertEqual(host.start, 12345.0)
        self.assertEqual(host._last_slideshow_paths, ['other.jpg'])
        host.tv.delete_list.assert_not_awaited()

    async def test_uploads_and_displays_without_touching_rotation(self):
        with tempfile.TemporaryDirectory() as root:
            self.write_image(root)
            host = self.make_host(root)

            await host._upload_single_image('pic.jpg', req_id='r1')

            host.upload_files.assert_awaited_once_with(['pic.jpg'])
            host.tv.select_image.assert_awaited_once_with('new-content')
            self.assertEqual(host.current_content_id, 'new-content')
            self.assertIn('new-content', host.shown_content_ids)
            host.write_program_data.assert_called_once()
            self.assertIn('ok', [status for status, _ in self.ack_calls(host)])
            self.assert_rotation_untouched(host)

    async def test_existing_upload_is_reused_instead_of_re_uploaded(self):
        with tempfile.TemporaryDirectory() as root:
            self.write_image(root)
            host = self.make_host(
                root,
                uploaded={'pic.jpg': {
                    'content_id': 'cached-content',
                    'path_rel': 'pic.jpg',
                }},
                live_entries=[{'content_id': 'cached-content'}],
            )

            await host._upload_single_image('pic.jpg')

            host.upload_files.assert_not_awaited()
            host.tv.select_image.assert_awaited_once_with('cached-content')
            self.assert_rotation_untouched(host)

    async def test_stale_cache_entry_forces_a_fresh_upload(self):
        with tempfile.TemporaryDirectory() as root:
            self.write_image(root)
            host = self.make_host(
                root,
                uploaded={'pic.jpg': {
                    'content_id': 'cached-content',
                    'path_rel': 'pic.jpg',
                }},
                live_entries=[],
            )
            # The TV no longer holds the cached content_id.
            host._cached_upload_matches_tv = mock.AsyncMock(return_value=False)

            await host._upload_single_image('pic.jpg')

            host.upload_files.assert_awaited_once_with(['pic.jpg'])
            host.tv.select_image.assert_awaited_once_with('new-content')

    async def test_tv_not_in_art_mode_is_reported_and_uploads_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            self.write_image(root)
            host = self.make_host(root)
            host.safe_in_artmode = mock.AsyncMock(return_value=False)

            await host._upload_single_image('pic.jpg')

            host.upload_files.assert_not_awaited()
            host.tv.select_image.assert_not_awaited()
            self.assertEqual(self.ack_calls(host)[-1][0], 'error')
            self.assertFalse(host._refresh_in_progress)
            self.assert_rotation_untouched(host)

    async def test_missing_file_is_reported_without_locking_the_tv(self):
        with tempfile.TemporaryDirectory() as root:
            host = self.make_host(root)

            await host._upload_single_image('gone.jpg')

            host.get_tv_content_entries.assert_not_awaited()
            self.assertEqual(self.ack_calls(host)[-1][0], 'error')
            self.assertFalse(host._refresh_in_progress)

    async def test_concurrent_refresh_is_refused(self):
        with tempfile.TemporaryDirectory() as root:
            self.write_image(root)
            host = self.make_host(root)
            host._refresh_in_progress = True

            await host._upload_single_image('pic.jpg')

            host.upload_files.assert_not_awaited()
            self.assertEqual(self.ack_calls(host)[-1][0], 'error')
            # The in-flight refresh must not be cleared by the refusal.
            self.assertTrue(host._refresh_in_progress)

    async def test_refresh_flag_is_released_after_a_failure(self):
        with tempfile.TemporaryDirectory() as root:
            self.write_image(root)
            host = self.make_host(root)
            host.get_tv_content_entries = mock.AsyncMock(return_value=None)

            await host._upload_single_image('pic.jpg')

            self.assertFalse(host._refresh_in_progress)
            self.assertEqual(self.ack_calls(host)[-1][0], 'error')
            host._publish_slideshow_state.assert_called_once()
            host._publish_slideshow_available.assert_called_once()


if __name__ == '__main__':
    unittest.main()
