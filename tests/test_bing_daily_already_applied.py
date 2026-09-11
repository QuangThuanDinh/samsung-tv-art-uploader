import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class BingDailyAlreadyAppliedTests(unittest.TestCase):
    """Do-and-forget: once Bing Daily Wallpaper has today's image uploaded and
    active, startup must not open an Art WebSocket purely to re-verify state
    already known from the persisted cache. Rotation-mode collections still
    need a live socket to discover/rotate art, so this must stay scoped to
    daily mode only."""

    def make_host(self, is_daily_mode=True, pending=False, override=None, requires_upload=None):
        host = monitor_and_display.__new__(monitor_and_display)
        host.bing_daily = mock.Mock()
        host.bing_daily.is_daily_mode.return_value = is_daily_mode
        host.slideshow_override_pending = pending
        host.slideshow_override = override if override is not None else ['Bing_DailyWallpaper/today.jpg']
        host._slideshow_paths_requiring_upload = mock.Mock(
            return_value=requires_upload if requires_upload is not None else [],
        )
        return host

    def test_true_when_daily_mode_applied_and_nothing_to_upload(self):
        host = self.make_host()

        self.assertTrue(host._bing_daily_already_applied())

    def test_false_when_not_daily_mode(self):
        host = self.make_host(is_daily_mode=False)

        self.assertFalse(host._bing_daily_already_applied())
        host._slideshow_paths_requiring_upload.assert_not_called()

    def test_false_when_override_pending(self):
        host = self.make_host(pending=True)

        self.assertFalse(host._bing_daily_already_applied())

    def test_false_when_no_override(self):
        host = self.make_host(override=[])

        self.assertFalse(host._bing_daily_already_applied())

    def test_false_when_upload_still_required(self):
        host = self.make_host(requires_upload=['Bing_DailyWallpaper/today.jpg'])

        self.assertFalse(host._bing_daily_already_applied())

    def test_false_on_unexpected_error(self):
        host = self.make_host()
        host.bing_daily.is_daily_mode.side_effect = RuntimeError('boom')

        self.assertFalse(host._bing_daily_already_applied())


if __name__ == '__main__':
    unittest.main()
