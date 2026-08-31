import os
import tempfile
import unittest

from PIL import Image

from loop.pil_methods import PIL_methods


class _Monitor:
    def __init__(self, folder):
        self.folder = folder
        self.uploaded_files = {}

    def get_folder_files(self):
        return ['painting.jpg']

    @staticmethod
    def get_file_type(_path, image):
        return image.format.lower()


class PILMethodsTests(unittest.TestCase):
    def test_load_files_uses_current_monitor_folder(self):
        with tempfile.TemporaryDirectory() as root:
            collection = os.path.join(root, 'Bing_DailyWallpaper')
            os.makedirs(collection)
            image_path = os.path.join(collection, 'painting.jpg')
            Image.new('RGB', (20, 10), color='blue').save(image_path)

            monitor = _Monitor(root)
            helper = PIL_methods(monitor)
            monitor.folder = collection

            loaded = helper.load_files()
            try:
                self.assertEqual(list(loaded), ['painting.jpg'])
            finally:
                # load_files returns lazily-opened PIL images. They must be
                # closed before the temp directory is removed, or Windows
                # refuses to unlink the still-open file.
                for image in loaded.values():
                    image.close()


if __name__ == '__main__':
    unittest.main()
