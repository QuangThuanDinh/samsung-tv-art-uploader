import os
import tempfile
import unittest
from unittest import mock

from PIL import Image

from loop.pil_methods import PIL_methods


class _Monitor:
    def __init__(self, folder, uploaded_files=None):
        self.folder = folder
        self.uploaded_files = uploaded_files or {}
        self.get_tv_content = mock.AsyncMock()
        self.write_program_data = mock.Mock()
        self.update_uploaded_files = mock.Mock(
            side_effect=lambda filename, content_id: self.uploaded_files.__setitem__(
                filename, {'content_id': content_id, 'path_rel': filename},
            )
        )

    def get_folder_files(self):
        return list(self._files)

    @staticmethod
    def get_file_type(_path, image):
        return image.format.lower()


class PILSyncSkipTests(unittest.IsolatedAsyncioTestCase):
    """The persisted uploaded_files cache must actually be consulted: once a
    restart restores it (as loop.uploader.set_current_cache does), initialize()
    must not re-run the expensive TV thumbnail download/compare for files it
    already has a content_id for."""

    def make_collection(self, root, name, filenames):
        collection = os.path.join(root, name)
        os.makedirs(collection)
        for fn in filenames:
            Image.new('RGB', (20, 10), color='blue').save(os.path.join(collection, fn))
        return collection

    async def test_skips_tv_sync_when_all_files_already_cached(self):
        with tempfile.TemporaryDirectory() as root:
            collection = self.make_collection(
                root, 'Bing_DailyWallpaper', ['OHR.Olvera_UHD.museum-label.jpg'],
            )
            # Cache restored the way loop.uploader.set_current_cache does:
            # keyed by the media_root-relative path, not the bare filename.
            uploaded_files = {
                'Bing_DailyWallpaper/OHR.Olvera_UHD.museum-label.jpg': {
                    'content_id': 'MY_F0273',
                    'path_rel': 'Bing_DailyWallpaper/OHR.Olvera_UHD.museum-label.jpg',
                },
            }
            monitor = _Monitor(collection, uploaded_files)
            monitor._files = ['OHR.Olvera_UHD.museum-label.jpg']
            helper = PIL_methods(monitor)

            completed = await helper.initialize()

            self.assertTrue(completed)
            monitor.get_tv_content.assert_not_called()

    async def test_only_syncs_unresolved_files(self):
        with tempfile.TemporaryDirectory() as root:
            collection = self.make_collection(
                root, 'Bing_DailyWallpaper',
                ['known.jpg', 'new.jpg'],
            )
            uploaded_files = {
                'Bing_DailyWallpaper/known.jpg': {
                    'content_id': 'MY_F0001',
                    'path_rel': 'Bing_DailyWallpaper/known.jpg',
                },
            }
            monitor = _Monitor(collection, uploaded_files)
            monitor._files = ['known.jpg', 'new.jpg']
            monitor.get_tv_content.return_value = ['MY_F0002']
            helper = PIL_methods(monitor)
            helper.get_thumbnails = mock.AsyncMock(return_value={})

            completed = await helper.initialize()

            self.assertTrue(completed)
            monitor.get_tv_content.assert_called_once()
            # Only the unresolved file is handed to the thumbnail comparison.
            helper.get_thumbnails.assert_called_once()


if __name__ == '__main__':
    unittest.main()
