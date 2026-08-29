import asyncio
import time
import unittest
from unittest import mock

from loop.uploader import monitor_and_display


class ReconnectBackoffTests(unittest.IsolatedAsyncioTestCase):
    def make_host(self, watchdog_seconds=0):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.tv = None
        host.reconnect_delay = 5
        host.connect_retry_max_seconds = 300
        host.connect_watchdog_seconds = watchdog_seconds
        host._connect_failures = 0
        host._next_connect_attempt = 0.0
        host._first_connect_failure_time = None
        host._status_check_needed = False
        return host

    def attach_failing_tv(self, host, error=None):
        """Make _create_tv_connection produce a client that never completes."""
        def create():
            host.tv = mock.Mock()
            host.tv.is_powered_on = mock.AsyncMock(return_value=True)
            host.tv.start_listening = mock.AsyncMock(
                side_effect=error or ConnectionError(
                    'TV Art WebSocket did not emit ms.channel.ready within 10s'
                )
            )
            host.tv.is_alive = mock.Mock(return_value=False)
            host.tv.retire = mock.Mock()
            host.tv.close = mock.AsyncMock()

        host._create_tv_connection = mock.Mock(side_effect=create)

    async def test_handshake_failure_is_logged_with_real_reason(self):
        host = self.make_host()
        self.attach_failing_tv(host)

        self.assertFalse(await host.reconnect_tv(power_verified=True))

        host.log.warning.assert_called_once()
        message = host.log.warning.call_args[0][0] % host.log.warning.call_args[0][1:]
        self.assertIn('ms.channel.ready', message)
        self.assertIn('attempt 1', message)

    async def test_cooldown_blocks_immediate_retry(self):
        host = self.make_host()
        self.attach_failing_tv(host)

        await host.reconnect_tv(power_verified=True)
        create_calls = host._create_tv_connection.call_count

        # A second attempt inside the cooldown window must not touch the TV.
        self.assertFalse(await host.reconnect_tv(power_verified=True))
        self.assertEqual(host._create_tv_connection.call_count, create_calls)
        self.assertEqual(host._connect_failures, 1)

    async def test_cooldown_escalates_and_is_capped(self):
        host = self.make_host()
        host.connect_retry_max_seconds = 40

        for expected in (5, 10, 20, 40, 40):
            host._next_connect_attempt = 0.0
            before = time.time()
            host._note_connect_failure('boom')
            self.assertAlmostEqual(
                host._next_connect_attempt - before,
                expected,
                delta=1.0,
            )

    async def test_success_clears_backoff(self):
        host = self.make_host()
        host._connect_failures = 4
        host._next_connect_attempt = 0.0
        host._first_connect_failure_time = time.time() - 60

        def create():
            host.tv = mock.Mock()
            host.tv.is_powered_on = mock.AsyncMock(return_value=True)
            host.tv.start_listening = mock.AsyncMock()
            host.tv.is_alive = mock.Mock(return_value=True)
            host.tv.retire = mock.Mock()
            host.tv.close = mock.AsyncMock()

        host._create_tv_connection = mock.Mock(side_effect=create)

        self.assertTrue(await host.reconnect_tv(power_verified=True))
        self.assertEqual(host._connect_failures, 0)
        self.assertEqual(host._next_connect_attempt, 0.0)
        self.assertIsNone(host._first_connect_failure_time)

    async def test_powered_off_tv_does_not_count_as_handshake_failure(self):
        host = self.make_host()
        host._connect_failures = 3
        host._first_connect_failure_time = time.time() - 60

        def create():
            host.tv = mock.Mock()
            host.tv.is_powered_on = mock.AsyncMock(return_value=False)
            host.tv.retire = mock.Mock()
            host.tv.close = mock.AsyncMock()

        host._create_tv_connection = mock.Mock(side_effect=create)

        self.assertFalse(await host.reconnect_tv())
        self.assertEqual(host._connect_failures, 0)
        self.assertEqual(host._next_connect_attempt, 0.0)

    def test_watchdog_exits_after_prolonged_failure(self):
        host = self.make_host(watchdog_seconds=1800)
        host._connect_failures = 25
        host._first_connect_failure_time = time.time() - 1801

        with mock.patch('os._exit') as fake_exit, mock.patch('logging.shutdown'):
            host._check_connect_watchdog(time.time())

        fake_exit.assert_called_once_with(1)
        host.log.error.assert_called_once()

    def test_watchdog_does_not_exit_before_threshold(self):
        host = self.make_host(watchdog_seconds=1800)
        host._first_connect_failure_time = time.time() - 60

        with mock.patch('os._exit') as fake_exit:
            host._check_connect_watchdog(time.time())

        fake_exit.assert_not_called()

    def test_watchdog_can_be_disabled(self):
        host = self.make_host(watchdog_seconds=0)
        host._first_connect_failure_time = time.time() - 100000

        with mock.patch('os._exit') as fake_exit:
            host._check_connect_watchdog(time.time())

        fake_exit.assert_not_called()


class LivenessProbeLoggingTests(unittest.TestCase):
    def make_host(self):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.tv = None
        return host

    def test_probe_logs_socket_state_and_readiness(self):
        host = self.make_host()
        host.tv = mock.Mock(
            retired=False,
            socket_state='OPEN',
            channel_ready=True,
        )

        host._log_art_liveness_probe()

        call = host.log.info.call_args[0]
        self.assertEqual(call[1], 'OPEN')
        self.assertTrue(call[2])
        self.assertIn('get_artmode_status', call[0])
    def test_retired_connection_is_flagged_in_socket_state(self):
        host = self.make_host()
        host.tv = mock.Mock(retired=True, socket_state='CLOSED')

        self.assertEqual(host._describe_socket_state(), 'CLOSED (retired)')

    def test_missing_connection_is_reported(self):
        host = self.make_host()

        self.assertEqual(host._describe_socket_state(), 'no-connection')

    def test_socket_state_errors_do_not_break_logging(self):
        host = self.make_host()
        broken = mock.Mock(retired=False)
        type(broken).socket_state = mock.PropertyMock(
            side_effect=RuntimeError('boom')
        )
        host.tv = broken

        self.assertEqual(host._describe_socket_state(), 'unavailable')
        host._log_art_liveness_probe()
        host.log.info.assert_called_once()


class LivenessProbeLoopTests(unittest.IsolatedAsyncioTestCase):
    def make_host(self, in_art_mode=True):
        host = monitor_and_display.__new__(monitor_and_display)
        host.log = mock.Mock()
        host.art_status_probe_seconds = 0
        host._refresh_in_progress = False
        host._tv_shutdown_signaled = False
        host._status_check_needed = False
        host._last_art_status_probe = 0
        host._in_art_mode = in_art_mode
        host._artmode_event = asyncio.Event()
        host.tv = mock.Mock(retired=False, socket_state='OPEN', channel_ready=True)
        return host

    async def run_one_pass(self, host):
        """Run exactly one probe iteration, then stop the loop."""
        calls = []

        async def probe():
            calls.append(True)
            if len(calls) > 1:
                raise asyncio.CancelledError()
            return host._in_art_mode

        host.safe_in_artmode = probe
        task = asyncio.create_task(host._art_liveness_loop())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return calls

    async def test_probe_runs_while_tv_is_not_in_art_mode(self):
        # A TV sitting on HDMI is exactly when a dead socket must be detected.
        host = self.make_host(in_art_mode=False)

        calls = await self.run_one_pass(host)

        self.assertTrue(calls)
        host.log.info.assert_any_call(
            'Art liveness probe starting: REST power probe + '
            'get_artmode_status (socket=%s, channel_ready=%s)',
            'OPEN',
            True,
        )

    async def test_probe_skips_while_refresh_in_progress(self):
        host = self.make_host()
        host._refresh_in_progress = True

        calls = await self.run_one_pass(host)

        self.assertEqual(calls, [])

    async def test_probe_skips_without_a_connection(self):
        host = self.make_host()
        host.tv = None

        calls = await self.run_one_pass(host)

        self.assertEqual(calls, [])

    async def test_art_mode_transition_wakes_main_loop(self):
        host = self.make_host(in_art_mode=False)

        async def probe():
            host._in_art_mode = True
            return True

        host.safe_in_artmode = probe
        task = asyncio.create_task(host._art_liveness_loop())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        self.assertTrue(host._artmode_event.is_set())
        self.assertTrue(host._status_check_needed)

    async def test_probe_survives_a_failing_check(self):
        host = self.make_host()
        attempts = []

        async def probe():
            attempts.append(True)
            raise ConnectionError('dead channel')

        host.safe_in_artmode = probe
        task = asyncio.create_task(host._art_liveness_loop())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        self.assertTrue(attempts)
        host.log.debug.assert_called()


if __name__ == '__main__':
    unittest.main()
