import json
import os
import tempfile
import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class DynamicStandbyReuseTests(unittest.TestCase):
    def test_reuses_matching_dynamic_standby_without_uploading_duplicate(self):
        with tempfile.TemporaryDirectory() as media_root:
            source = os.path.join('Collection', 'art.jpg')
            source_path = os.path.join(media_root, source)
            os.makedirs(os.path.dirname(source_path))
            with open(source_path, 'wb') as image:
                image.write(b'image')

            state_path = os.path.join(media_root, 'standby-state.json')
            with open(state_path, 'w', encoding='utf-8') as state:
                json.dump({'source': source}, state)

            host = monitor_and_display.__new__(monitor_and_display)
            host.dynamic_standby = True
            host.standby_content_id = 'MY_F0262'
            host.standby_image_date = '2026:08:29 05:00:00'
            host.dynamic_standby_state_path = state_path
            host.media_root = media_root
            host.matte = 'none'
            host.uploaded_files = {}
            host._resolve_matte_for = mock.Mock(return_value='none')
            host.write_program_data = mock.Mock()
            host.log = mock.Mock()

            remaining = host.reuse_dynamic_standby_upload([source])

            self.assertEqual(remaining, [])
            self.assertEqual(
                host.uploaded_files[source]['content_id'],
                'MY_F0262',
            )
            self.assertEqual(
                host.uploaded_files[source]['image_date'],
                '2026:08:29 05:00:00',
            )
            host.write_program_data.assert_called_once_with()

    def test_does_not_reuse_standby_with_different_matte(self):
        with tempfile.TemporaryDirectory() as media_root:
            source = 'art.jpg'
            source_path = os.path.join(media_root, source)
            with open(source_path, 'wb') as image:
                image.write(b'image')

            state_path = os.path.join(media_root, 'standby-state.json')
            with open(state_path, 'w', encoding='utf-8') as state:
                json.dump({'source': source}, state)

            host = monitor_and_display.__new__(monitor_and_display)
            host.dynamic_standby = True
            host.standby_content_id = 'MY_F0262'
            host.dynamic_standby_state_path = state_path
            host.media_root = media_root
            host.matte = 'none'
            host.uploaded_files = {}
            host._resolve_matte_for = mock.Mock(return_value='shadowbox_polar')

            self.assertEqual(
                host.reuse_dynamic_standby_upload([source]),
                [source],
            )
            self.assertEqual(host.uploaded_files, {})


if __name__ == '__main__':
    unittest.main()
