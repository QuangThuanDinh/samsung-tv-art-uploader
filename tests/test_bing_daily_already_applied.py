import unittest
from unittest import mock

from loop.BingDailyWallpaperManager import BingDailyWallpaperManager


class BingDailyAlreadyAppliedTests(unittest.TestCase):
    """Do-and-forget: once Bing Daily Wallpaper has today's image uploaded and
    active, startup must not open an Art WebSocket purely to re-verify state
    already known from the persisted cache. Rotation-mode collections still
    need a live socket to discover/rotate art, so this must stay scoped to
    daily mode only."""

    def make_manager(self, is_daily_mode=True, pending=False, override=None, requires_upload=None):
        host = mock.Mock()
        host.log = mock.Mock()
        host.media_root = '/media'
        manager = BingDailyWallpaperManager.__new__(BingDailyWallpaperManager)
        manager.host = host
        manager.is_daily_mode = mock.Mock(return_value=is_daily_mode)
        host.slideshow_override_pending = pending
        host.slideshow_override = override if override is not None else ['Bing_DailyWallpaper/today.jpg']
        host._slideshow_paths_requiring_upload = mock.Mock(
            return_value=requires_upload if requires_upload is not None else [],
        )
        return manager

    def test_true_when_daily_mode_applied_and_nothing_to_upload(self):
        manager = self.make_manager()

        self.assertTrue(manager.already_applied())

    def test_false_when_not_daily_mode(self):
        manager = self.make_manager(is_daily_mode=False)

        self.assertFalse(manager.already_applied())
        manager.host._slideshow_paths_requiring_upload.assert_not_called()

    def test_false_when_override_pending(self):
        manager = self.make_manager(pending=True)

        self.assertFalse(manager.already_applied())

    def test_false_when_no_override(self):
        manager = self.make_manager(override=[])

        self.assertFalse(manager.already_applied())

    def test_false_when_upload_still_required(self):
        manager = self.make_manager(requires_upload=['Bing_DailyWallpaper/today.jpg'])

        self.assertFalse(manager.already_applied())

    def test_false_on_unexpected_error(self):
        manager = self.make_manager()
        manager.is_daily_mode.side_effect = RuntimeError('boom')

        self.assertFalse(manager.already_applied())


if __name__ == '__main__':
    unittest.main()
