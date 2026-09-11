import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class InitializeSkipsStartupSessionTests(unittest.IsolatedAsyncioTestCase):
    """initialize() must not open the startup Art session when Bing Daily
    Wallpaper mode already has today's image uploaded and active — that
    session (and the PIL sync it triggers) exists to reconcile state that,
    in this case, is already known good from the persisted cache."""

    def make_host(self, already_applied):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.tv = None
        host._in_art_mode = None
        host.selected_collections = ['Bing_DailyWallpaper']
        host.current_content_id = None
        host._tv_init_pending = True
        host._startup_in_progress = True
        host.get_api_version = mock.AsyncMock()
        host.get_current_artwork = mock.AsyncMock(return_value='MY_F0273')
        host.safe_in_artmode = mock.AsyncMock(return_value=True)
        host._publish_current_artwork_state = mock.AsyncMock()
        host._publish_slideshow_state = mock.Mock()
        host.load_program_data = mock.Mock()
        host.get_folder_files = mock.Mock(return_value=[])
        host.folder = '/media/Bing_DailyWallpaper'
        host.bing_daily = mock.Mock()
        host.bing_daily.already_applied.return_value = already_applied
        host._initialize_tv_state = mock.AsyncMock()
        host.get_content_ids = mock.Mock(return_value=[])
        host.tv_session = mock.Mock(side_effect=RuntimeError('tv_session must not be entered'))
        return host

    async def test_skips_tv_session_when_already_applied(self):
        host = self.make_host(already_applied=True)

        await monitor_and_display.initialize(host)

        host.tv_session.assert_not_called()
        host._initialize_tv_state.assert_not_called()
        self.assertFalse(host._tv_init_pending)
        self.assertFalse(host._startup_in_progress)
        host._publish_slideshow_state.assert_called_once()

    async def test_opens_tv_session_when_not_already_applied(self):
        host = self.make_host(already_applied=False)
        host.tv_session = mock.MagicMock()
        cm = mock.AsyncMock()
        cm.__aenter__.return_value = None
        cm.__aexit__.return_value = False
        host.tv_session.return_value = cm

        await monitor_and_display.initialize(host)

        host.tv_session.assert_called_once_with('startup', require_artmode=False)
        host._initialize_tv_state.assert_called_once()


if __name__ == '__main__':
    unittest.main()
