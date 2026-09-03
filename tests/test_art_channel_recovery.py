import asyncio
import time
import unittest
from unittest import mock

from loop.tv_connection import ArtChannelNotReadyError, FrameTVConnection
from loop.uploader import monitor_and_display


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class ReadyTimeoutRetryTests(unittest.TestCase):
    """A missing ms.channel.ready must not be retried inside one query.

    The second attempt re-enters start_listening and waits the same timeout for
    the same event on the same socket, so it doubles the cost of every probe
    against a TV whose art-app channel never comes up.
    """

    def make_conn(self):
        conn = FrameTVConnection.__new__(FrameTVConnection)
        conn._logger = mock.Mock()
        conn._retired = False
        conn._artmode_failures = 0
        conn._artmode_failure_limit = 3
        conn._client = mock.Mock()
        conn.retire = mock.Mock()
        return conn

    def test_not_ready_is_attempted_once(self):
        conn = self.make_conn()
        conn._client.get_artmode = mock.AsyncMock(
            side_effect=ArtChannelNotReadyError('no ready'),
        )

        with self.assertRaises(ArtChannelNotReadyError):
            run(conn.query_artmode(power_verified=True))

        self.assertEqual(conn._client.get_artmode.await_count, 1)
        self.assertEqual(conn._artmode_failures, 1)
        conn.retire.assert_not_called()

    def test_other_errors_still_get_a_second_attempt(self):
        conn = self.make_conn()
        conn._client.get_artmode = mock.AsyncMock(side_effect=TimeoutError('quiet'))

        with self.assertRaises(TimeoutError):
            run(conn.query_artmode(power_verified=True))

        self.assertEqual(conn._client.get_artmode.await_count, 2)

    def test_not_ready_still_retires_at_the_limit(self):
        conn = self.make_conn()
        conn._client.get_artmode = mock.AsyncMock(
            side_effect=ArtChannelNotReadyError('no ready'),
        )

        for _ in range(3):
            with self.assertRaises(ArtChannelNotReadyError):
                run(conn.query_artmode(power_verified=True))

        conn.retire.assert_called_once()


class ReconnectRetirePolicyTests(unittest.TestCase):
    """A handshake failure that left the socket open must keep that socket.

    start_listening() deliberately leaves the transport open when
    ms.channel.ready never arrives so a late ready can still complete it.
    Retiring here forced a brand new session on the TV every cooldown expiry.
    """

    def make_host(self, alive):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host._status_check_needed = False
        host._next_connect_attempt = 0.0
        host._connect_failures = 0
        host._first_connect_failure_time = None
        host.reconnect_delay = 5
        host.connect_retry_max_seconds = 60
        host.connect_watchdog_seconds = 0
        host._in_art_mode = None

        def make_client():
            client = mock.Mock()
            client.retired = False
            client.is_alive = mock.Mock(return_value=alive)
            client.retire = mock.Mock()
            client.close = mock.AsyncMock()
            client.is_powered_on = mock.AsyncMock(return_value=True)
            client.start_listening = mock.AsyncMock(
                side_effect=ArtChannelNotReadyError('no ready'),
            )
            return client

        host.tv = make_client()
        # The real _create_tv_connection installs a fresh client on self.tv.
        host.replacement = None

        def create():
            host.replacement = make_client()
            host.tv = host.replacement

        host._create_tv_connection = mock.Mock(side_effect=create)
        return host

    def test_open_socket_survives_a_ready_timeout(self):
        host = self.make_host(alive=True)
        outgoing = host.tv

        self.assertFalse(run(host.reconnect_tv(power_verified=True)))

        outgoing.retire.assert_called_once()
        self.assertIs(host.tv, host.replacement)
        host.replacement.retire.assert_not_called()

    def test_dead_socket_is_retired(self):
        host = self.make_host(alive=False)
        outgoing = host.tv

        self.assertFalse(run(host.reconnect_tv(power_verified=True)))

        outgoing.retire.assert_called_once()
        host.replacement.retire.assert_called_once()

    def test_cooldown_blocks_without_touching_a_socket(self):
        host = self.make_host(alive=True)
        host._next_connect_attempt = time.time() + 60

        self.assertFalse(run(host.reconnect_tv(power_verified=True)))

        host._create_tv_connection.assert_not_called()
        host.tv.retire.assert_not_called()
        self.assertTrue(host._status_check_needed)


class ConnectFailureClassificationTests(unittest.TestCase):
    """An unknown art mode must not be reported as an expected failure.

    _in_art_mode is None precisely because the channel is down, so treating
    None as "expected" made every genuine wedge reset the watchdog timer.
    """

    def make_host(self, in_art_mode):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host._connect_failures = 0
        host._next_connect_attempt = 0.0
        host._first_connect_failure_time = None
        host.reconnect_delay = 5
        host.connect_retry_max_seconds = 60
        host.connect_watchdog_seconds = 0
        host._in_art_mode = in_art_mode
        return host

    def test_confirmed_art_mode_off_is_expected(self):
        host = self.make_host(False)

        host._note_connect_failure('no ready')

        self.assertIsNone(host._first_connect_failure_time)

    def test_unknown_art_mode_arms_the_watchdog(self):
        host = self.make_host(None)

        host._note_connect_failure('no ready')

        self.assertIsNotNone(host._first_connect_failure_time)

    def test_watchdog_start_time_is_not_reset_by_later_failures(self):
        host = self.make_host(None)

        host._note_connect_failure('no ready')
        first = host._first_connect_failure_time
        host._note_connect_failure('no ready')

        self.assertEqual(host._first_connect_failure_time, first)


class LivenessProbeCooldownSkipTests(unittest.IsolatedAsyncioTestCase):
    """A probe that cannot reconnect must not run or log at INFO."""

    def make_host(self, cooldown_active, channel_live):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.art_status_probe_seconds = 0
        host._refresh_in_progress = False
        host._tv_shutdown_signaled = False
        host._tv_powered_on = True
        host._in_art_mode = None
        host._connect_failures = 1
        host._next_connect_attempt = (
            time.time() + 60 if cooldown_active else 0.0
        )
        host.tv = mock.Mock()
        host.tv.retired = not channel_live
        host.tv.is_alive = mock.Mock(return_value=channel_live)
        host.tv.channel_ready = channel_live
        host.safe_in_artmode = mock.AsyncMock(return_value=False)
        host._artmode_event = mock.Mock()
        return host

    async def pump(self, host):
        task = asyncio.ensure_future(host._art_liveness_loop())
        for _ in range(6):
            await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_probe_is_skipped_while_the_cooldown_is_active(self):
        host = self.make_host(cooldown_active=True, channel_live=False)

        await self.pump(host)

        host.safe_in_artmode.assert_not_awaited()
        host.log.info.assert_not_called()
        self.assertTrue(host.log.debug.called)

    async def test_probe_runs_once_the_cooldown_expires(self):
        host = self.make_host(cooldown_active=False, channel_live=False)

        await self.pump(host)

        host.safe_in_artmode.assert_awaited()

    async def test_live_channel_is_always_probed(self):
        host = self.make_host(cooldown_active=True, channel_live=True)

        await self.pump(host)

        host.safe_in_artmode.assert_awaited()


if __name__ == '__main__':
    unittest.main()
