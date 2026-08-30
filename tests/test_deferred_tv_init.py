import unittest
from unittest import mock

from loop.pil_methods import PIL_methods
from loop.uploader import monitor_and_display


class ArtChannelLivenessTests(unittest.TestCase):
    """_art_channel_is_live gates every TV-dependent initialization step."""

    def make_host(self):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.tv = None
        return host

    def make_tv(self, retired=False, alive=True, channel_ready=True):
        tv = mock.Mock()
        tv.retired = retired
        tv.is_alive = mock.Mock(return_value=alive)
        tv.channel_ready = channel_ready
        return tv

    def test_no_connection_is_not_live(self):
        host = self.make_host()

        self.assertFalse(host._art_channel_is_live())

    def test_retired_connection_is_not_live(self):
        host = self.make_host()
        host.tv = self.make_tv(retired=True)

        self.assertFalse(host._art_channel_is_live())

    def test_open_socket_without_ready_is_not_live(self):
        # The socket reaching OPEN is not enough; on HDMI it opens but the
        # art-app channel never completes its handshake.
        host = self.make_host()
        host.tv = self.make_tv(channel_ready=False)

        self.assertFalse(host._art_channel_is_live())

    def test_dead_socket_is_not_live(self):
        host = self.make_host()
        host.tv = self.make_tv(alive=False)

        self.assertFalse(host._art_channel_is_live())

    def test_ready_channel_is_live(self):
        host = self.make_host()
        host.tv = self.make_tv()

        self.assertTrue(host._art_channel_is_live())

    def test_a_raising_client_is_not_live(self):
        host = self.make_host()
        tv = self.make_tv()
        tv.is_alive = mock.Mock(side_effect=RuntimeError('boom'))
        host.tv = tv

        self.assertFalse(host._art_channel_is_live())


class DeferredTvInitializationTests(unittest.IsolatedAsyncioTestCase):
    """Initialization must retry instead of silently degrading for the session."""

    def make_host(self, live=True, sync=True):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.sync = sync
        host.api_version_str = '4.3.4.0'
        host._in_art_mode = False
        host._tv_init_pending = False
        host.get_api_version = mock.AsyncMock()
        host._drain_pending_delete_ids = mock.AsyncMock()
        host.pil = mock.Mock()
        host.pil.initialize = mock.AsyncMock(return_value=True)
        host._art_channel_is_live = mock.Mock(return_value=live)
        return host

    async def test_dead_channel_defers_instead_of_running(self):
        host = self.make_host(live=False)

        await host._initialize_tv_state()

        self.assertTrue(host._tv_init_pending)
        host.get_api_version.assert_not_awaited()
        host.pil.initialize.assert_not_awaited()

    async def test_deferral_is_announced_only_once(self):
        host = self.make_host(live=False)

        await host._initialize_tv_state()
        await host._initialize_tv_state()

        self.assertEqual(host.log.info.call_count, 1)

    async def test_live_channel_runs_initialization(self):
        host = self.make_host()

        await host._initialize_tv_state()

        self.assertFalse(host._tv_init_pending)
        host.get_api_version.assert_awaited_once()
        host.pil.initialize.assert_awaited_once()

    async def test_pending_delete_drain_only_runs_in_art_mode(self):
        host = self.make_host()
        await host._initialize_tv_state()
        host._drain_pending_delete_ids.assert_not_awaited()

        host._in_art_mode = True
        await host._initialize_tv_state()
        host._drain_pending_delete_ids.assert_awaited_once()

    async def test_failed_thumbnail_sync_stays_pending(self):
        host = self.make_host()
        host.pil.initialize = mock.AsyncMock(return_value=False)

        await host._initialize_tv_state()

        self.assertTrue(host._tv_init_pending)

    async def test_a_later_success_clears_the_pending_flag(self):
        host = self.make_host(live=False)
        await host._initialize_tv_state()
        self.assertTrue(host._tv_init_pending)

        host._art_channel_is_live = mock.Mock(return_value=True)
        await host._initialize_tv_state()

        self.assertFalse(host._tv_init_pending)
        host.pil.initialize.assert_awaited_once()

    async def test_legacy_firmware_skips_thumbnail_sync(self):
        host = self.make_host()
        host.api_version_str = '0.97'

        await host._initialize_tv_state()

        self.assertFalse(host._tv_init_pending)
        host.pil.initialize.assert_not_awaited()

    async def test_sync_disabled_completes_without_thumbnails(self):
        host = self.make_host(sync=False)

        await host._initialize_tv_state()

        self.assertFalse(host._tv_init_pending)
        host.pil.initialize.assert_not_awaited()


class PILInitializeResultTests(unittest.IsolatedAsyncioTestCase):
    """A failed TV read must never be reported as an empty TV."""

    def make_helper(self, my_photos):
        monitor = mock.Mock()
        monitor.uploaded_files = {}
        monitor.get_tv_content = mock.AsyncMock(return_value=my_photos)
        helper = PIL_methods(monitor)
        helper.log = mock.Mock()
        helper.load_files = mock.Mock(return_value={'a.jpg': object()})
        helper.check_thumbnails = mock.AsyncMock()
        return helper

    async def test_failed_read_reports_failure_and_warns(self):
        helper = self.make_helper(None)

        self.assertFalse(await helper.initialize())
        helper.check_thumbnails.assert_not_awaited()
        helper.log.warning.assert_called_once()
        for call in helper.log.info.call_args_list:
            self.assertNotIn('no photos found on tv', call.args[0])

    async def test_genuinely_empty_tv_is_reported_as_empty(self):
        helper = self.make_helper([])

        self.assertTrue(await helper.initialize())
        helper.check_thumbnails.assert_not_awaited()
        helper.log.info.assert_any_call('no photos found on tv')

    async def test_populated_tv_runs_the_comparison(self):
        helper = self.make_helper(['content-1'])

        self.assertTrue(await helper.initialize())
        helper.check_thumbnails.assert_awaited_once()

    async def test_no_local_files_still_counts_as_complete(self):
        helper = self.make_helper(['content-1'])
        helper.load_files = mock.Mock(return_value={})

        self.assertTrue(await helper.initialize())
        helper.check_thumbnails.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
