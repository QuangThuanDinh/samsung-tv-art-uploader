import asyncio
import unittest
from unittest import mock

from loop.uploader import monitor_and_display


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def make_host(safe_mode=True, powered_on=True, in_artmode=True, connects=True):
    """Build a monitor_and_display with just enough state for tv_session().

    reconnect_tv() is what actually opens a WebSocket, so its await count is the
    number of sockets a scenario cost the TV.
    """
    host = monitor_and_display.__new__(monitor_and_display)
    host.log = mock.Mock()
    host.safe_mode = safe_mode
    host._tv_session_depth = 0
    host._tv_powered_on = None
    host._in_art_mode = None
    host._status_check_needed = False

    def make_tv(alive):
        tv = mock.Mock()
        tv.retired = False
        tv.is_alive = mock.Mock(return_value=alive)
        tv.is_powered_on = mock.AsyncMock(return_value=powered_on)
        tv.query_artmode = mock.AsyncMock(return_value=in_artmode)
        tv.close = mock.AsyncMock()

        def retire():
            tv.retired = True

        tv.retire = mock.Mock(side_effect=retire)
        return tv

    # Steady state between SAFE MODE flows: the object survives, its socket does
    # not, so every flow has to ask reconnect_tv for a new one.
    host.tv = make_tv(alive=False)
    host.tv.retired = True
    host._create_tv_connection = mock.Mock()

    async def reconnect(power_verified=False):
        if not connects:
            return False
        host.tv = make_tv(alive=True)
        return True

    host.reconnect_tv = mock.AsyncMock(side_effect=reconnect)
    return host


class SafeModeSessionTests(unittest.TestCase):
    def test_session_opens_and_closes_one_socket(self):
        host = make_host()

        async def scenario():
            async with host.tv_session('flow') as ready:
                self.assertTrue(ready)
                self.assertFalse(host.tv.retired)

        run(scenario())
        self.assertEqual(host.reconnect_tv.await_count, 1)
        self.assertTrue(host.tv.retired)
        host.tv.close.assert_awaited()

    def test_nested_sessions_reuse_one_socket(self):
        host = make_host()

        async def scenario():
            async with host.tv_session('outer') as outer:
                self.assertTrue(outer)
                async with host.tv_session('inner') as inner:
                    self.assertTrue(inner)
                    self.assertEqual(host._tv_session_depth, 2)
                # The inner scope must not close the shared socket.
                self.assertFalse(host.tv.retired)
                self.assertEqual(host._tv_session_depth, 1)

        run(scenario())
        self.assertEqual(host.reconnect_tv.await_count, 1)
        self.assertTrue(host.tv.retired)

    def test_socket_is_closed_when_the_flow_raises(self):
        host = make_host()

        async def scenario():
            async with host.tv_session('flow'):
                raise RuntimeError('boom')

        with self.assertRaises(RuntimeError):
            run(scenario())
        self.assertTrue(host.tv.retired)
        self.assertEqual(host._tv_session_depth, 0)

    def test_powered_off_tv_costs_no_websocket(self):
        host = make_host(powered_on=False)

        async def scenario():
            async with host.tv_session('flow') as ready:
                self.assertFalse(ready)

        run(scenario())
        host.reconnect_tv.assert_not_awaited()
        self.assertIs(host._tv_powered_on, False)

    def test_not_in_art_mode_yields_false_and_closes(self):
        host = make_host(in_artmode=False)

        async def scenario():
            async with host.tv_session('flow') as ready:
                self.assertFalse(ready)

        run(scenario())
        self.assertTrue(host.tv.retired)
        self.assertIs(host._in_art_mode, False)

    def test_require_artmode_false_skips_the_artmode_query(self):
        host = make_host(in_artmode=False)

        async def scenario():
            async with host.tv_session('flow', require_artmode=False) as ready:
                self.assertTrue(ready)

        run(scenario())
        host.tv.query_artmode.assert_not_awaited()

    def test_failed_handshake_yields_false(self):
        host = make_host(connects=False)

        async def scenario():
            async with host.tv_session('flow') as ready:
                self.assertFalse(ready)

        run(scenario())
        self.assertEqual(host.reconnect_tv.await_count, 1)

    def test_normal_mode_never_touches_the_connection(self):
        host = make_host(safe_mode=False)
        original = host.tv

        async def scenario():
            async with host.tv_session('flow') as ready:
                self.assertTrue(ready)

        run(scenario())
        # Nothing is probed, opened or closed: behaviour must be unchanged.
        host.reconnect_tv.assert_not_awaited()
        original.is_powered_on.assert_not_awaited()
        original.close.assert_not_awaited()
        self.assertIs(host.tv, original)
        self.assertEqual(host._tv_session_depth, 0)


class SafeModeBackgroundWorkTests(unittest.TestCase):
    def test_watchdog_is_disabled_in_safe_mode(self):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.safe_mode = True
        host.connect_watchdog_seconds = 1
        host._first_connect_failure_time = 0.0
        host._connect_failures = 99

        with mock.patch('os._exit') as exit_mock:
            host._check_connect_watchdog(10_000_000.0)

        exit_mock.assert_not_called()

    def test_artmode_check_outside_a_session_stays_rest_only(self):
        host = make_host()
        host._tv_shutdown_signaled = False
        host._tv_off_confirmed = False
        host._refresh_in_progress = False
        host.consecutive_failures = 3

        result = run(host._safe_in_artmode_unlocked())

        self.assertTrue(result)
        host.tv.query_artmode.assert_not_awaited()
        host.reconnect_tv.assert_not_awaited()
        self.assertEqual(host.consecutive_failures, 0)


if __name__ == '__main__':
    unittest.main()
