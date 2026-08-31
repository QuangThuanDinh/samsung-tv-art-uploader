import asyncio
import unittest
from unittest import mock

from loop.tv_connection import FrameTVConnection


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class QueryArtmodeFailureThresholdTests(unittest.TestCase):
    """A slow art channel must not cost the TV a brand new socket.

    Every replacement socket consumes memory in the TV's connection manager,
    and once that is exhausted the TV silently stops completing handshakes
    until it is power-cycled. So retirement is only allowed after the channel
    has failed repeatedly, never on a single unanswered request.
    """

    def make_conn(self, limit=3, status='on'):
        conn = FrameTVConnection.__new__(FrameTVConnection)
        conn._logger = mock.Mock()
        conn._retired = False
        conn._artmode_failures = 0
        conn._artmode_failure_limit = limit
        conn._client = mock.Mock()
        conn._client.test_mode = 0
        conn._client.get_artmode = mock.AsyncMock(return_value=status)
        conn.retire = mock.Mock()
        return conn

    def test_success_returns_true_and_resets_counter(self):
        conn = self.make_conn()
        conn._artmode_failures = 2

        self.assertTrue(run(conn.query_artmode(power_verified=True)))
        self.assertEqual(conn._artmode_failures, 0)
        conn.retire.assert_not_called()

    def test_off_status_is_false(self):
        conn = self.make_conn(status='off')

        self.assertFalse(run(conn.query_artmode(power_verified=True)))

    def test_single_failure_keeps_the_socket(self):
        conn = self.make_conn()
        conn._client.get_artmode = mock.AsyncMock(side_effect=TimeoutError('quiet'))

        with self.assertRaises(TimeoutError):
            run(conn.query_artmode(power_verified=True))

        self.assertEqual(conn._artmode_failures, 1)
        conn.retire.assert_not_called()

    def test_socket_is_retired_only_at_the_limit(self):
        conn = self.make_conn(limit=3)
        conn._client.get_artmode = mock.AsyncMock(side_effect=TimeoutError('quiet'))

        for _ in range(3):
            with self.assertRaises(TimeoutError):
                run(conn.query_artmode(power_verified=True))

        self.assertEqual(conn._artmode_failures, 3)
        conn.retire.assert_called_once()

    def test_recovery_before_the_limit_clears_the_counter(self):
        conn = self.make_conn(limit=3)
        conn._client.get_artmode = mock.AsyncMock(side_effect=TimeoutError('quiet'))

        for _ in range(2):
            with self.assertRaises(TimeoutError):
                run(conn.query_artmode(power_verified=True))

        conn._client.get_artmode = mock.AsyncMock(return_value='on')
        self.assertTrue(run(conn.query_artmode(power_verified=True)))

        self.assertEqual(conn._artmode_failures, 0)
        conn.retire.assert_not_called()

    def test_empty_response_counts_as_a_failure(self):
        conn = self.make_conn(status=None)

        with self.assertRaises(TimeoutError):
            run(conn.query_artmode(power_verified=True))

        self.assertEqual(conn._artmode_failures, 1)


class RetireClosesSocketTests(unittest.TestCase):
    """retire() must keep a strong reference to its close task.

    asyncio holds only weak references to tasks, so an unreferenced close task
    can be garbage collected before the socket is torn down, leaking the
    session on the TV.
    """

    def test_close_task_is_awaited_to_completion(self):
        closed = []

        async def scenario():
            conn = FrameTVConnection.__new__(FrameTVConnection)
            conn._retired = False
            conn._client = mock.Mock()

            async def close():
                await asyncio.sleep(0)
                closed.append(True)

            conn._client.close = close
            conn.retire()
            # Drop the only user reference; the module-level set must keep the
            # close task alive.
            del conn
            await asyncio.sleep(0.05)

        run(scenario())
        self.assertEqual(closed, [True])

    def test_retire_is_idempotent(self):
        conn = FrameTVConnection.__new__(FrameTVConnection)
        conn._retired = True
        conn._client = mock.Mock()

        conn.retire()

        conn._client.close.assert_not_called()


if __name__ == '__main__':
    unittest.main()
