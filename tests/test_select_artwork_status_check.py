import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class SelectArtworkStatusCheckTests(unittest.IsolatedAsyncioTestCase):
    """After a do-and-forget startup (nothing to reconcile), the main loop
    must not immediately force an art-mode/power probe — that would reopen
    the Art WebSocket we deliberately avoided during initialize(). Non-daily
    (rotation) startups still need the usual immediate check."""

    def make_host(self, already_applied):
        host = monitor_and_display.__new__(monitor_and_display)
        host.initialize = mock.AsyncMock()
        host.bing_daily = mock.Mock()
        host.bing_daily.already_applied.return_value = already_applied
        host.art_status_probe_seconds = 0
        host.safe_mode = True
        host._select_artwork_loop = mock.AsyncMock()
        return host

    async def test_no_forced_check_when_already_applied(self):
        host = self.make_host(already_applied=True)

        await monitor_and_display.select_artwork(host)

        self.assertFalse(host._status_check_needed)

    async def test_forced_check_when_not_already_applied(self):
        host = self.make_host(already_applied=False)

        await monitor_and_display.select_artwork(host)

        self.assertTrue(host._status_check_needed)


if __name__ == '__main__':
    unittest.main()
