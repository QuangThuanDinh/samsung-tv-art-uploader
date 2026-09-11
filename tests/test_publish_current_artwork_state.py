import asyncio
import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class PublishCurrentArtworkStateTests(unittest.IsolatedAsyncioTestCase):
    """The "file" attribute published to MQTT/the web UI must always be a
    bare basename. For subfolder collections (e.g. Bing_DailyWallpaper/...)
    the uploaded_files cache key is the full media_root-relative path, and
    leaking that whole path into "file" made the web UI build a doubled-up
    image URL: "<origin>/.../<folder>/<folder>%2F<name>.jpg" (a 404)."""

    def make_host(self, cache_key, content_id='cid-1'):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host._tv_state_lock = asyncio.Lock()
        host._refresh_in_progress = False
        host.mqtt_enabled = True
        host._mqtt = mock.Mock()
        host.get_current_artwork = mock.AsyncMock(return_value=content_id)
        host.current_content_id = None
        host.uploaded_files = {cache_key: {'content_id': content_id, 'path_rel': cache_key}}
        host._publish_mqtt_discovery = mock.Mock()
        host._publish_mqtt_state = mock.Mock()
        return host

    async def test_subfolder_cache_key_publishes_basename_only(self):
        host = self.make_host('Bing_DailyWallpaper/OHR.Olvera_UHD.museum-label.jpg')

        await host._publish_current_artwork_state(force=True)

        host._publish_mqtt_state.assert_called_once()
        display, file_attr, collection = host._publish_mqtt_state.call_args[0]
        self.assertEqual(file_attr, 'OHR.Olvera_UHD.museum-label.jpg')
        self.assertEqual(collection, 'Bing_DailyWallpaper')
        self.assertEqual(display, 'OHR.Olvera_UHD.museum-label')

    async def test_flat_collection_cache_key_still_publishes_basename(self):
        host = self.make_host('flat.jpg')

        await host._publish_current_artwork_state(force=True)

        host._publish_mqtt_state.assert_called_once()
        display, file_attr, collection = host._publish_mqtt_state.call_args[0]
        self.assertEqual(file_attr, 'flat.jpg')
        self.assertEqual(display, 'flat')

    async def test_skip_live_poll_does_not_call_get_current_artwork(self):
        """skip_live_poll must trust self.current_content_id (e.g. derived
        from cache during a do-and-forget startup) instead of calling
        get_current_artwork(), which talks to the TV client directly and
        would auto-open a WebSocket outside of any SAFE MODE session."""
        host = self.make_host('Bing_DailyWallpaper/OHR.Olvera_UHD.museum-label.jpg')
        host.current_content_id = 'cid-1'

        await host._publish_current_artwork_state(force=True, skip_live_poll=True)

        host.get_current_artwork.assert_not_called()
        host._publish_mqtt_state.assert_called_once()
        display, file_attr, collection = host._publish_mqtt_state.call_args[0]
        self.assertEqual(file_attr, 'OHR.Olvera_UHD.museum-label.jpg')


if __name__ == '__main__':
    unittest.main()
