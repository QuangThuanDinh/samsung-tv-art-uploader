import asyncio
import json
import unittest
from unittest import mock

from samsungtvws.async_connection import SamsungTVWSAsyncConnection

from loop.tv_connection import FrameTVConnection, _LoggingSamsungTVAsyncArt


class LoggingSamsungTVAsyncArtTests(unittest.IsolatedAsyncioTestCase):
    def make_client(self):
        client = _LoggingSamsungTVAsyncArt.__new__(
            _LoggingSamsungTVAsyncArt
        )
        client._response_logger = mock.Mock()
        client._retired = False
        client._channel_id = 'tv-connection-id'
        client._channel_ready = asyncio.Event()
        client._channel_ready.set()
        client._start_lock = asyncio.Lock()
        client._request_lock = asyncio.Lock()
        client._heartbeat_task = None
        client._suppress_disconnect_log = False
        client._ready_timeout = 1.0
        client._recv_loop = mock.Mock(done=mock.Mock(return_value=False))
        client._active_listener = client._recv_loop
        client.is_alive = mock.Mock(return_value=True)
        client.pending_requests = {}
        client.art_uuid = 'initial-uuid'
        return client

    def test_port_8001_skips_token_pairing(self):
        client = self.make_client()
        client.port = 8001
        client.host = '192.0.2.1'

        with mock.patch(
            'samsungtvws.async_art.SamsungTVAsyncArt.get_token'
        ) as parent_get_token:
            self.assertIsNone(client.get_token())

        parent_get_token.assert_not_called()

    def test_pairing_is_only_required_for_secure_port_without_token(self):
        connection = FrameTVConnection.__new__(FrameTVConnection)
        connection._client = mock.Mock(port=8001)
        self.assertFalse(connection.requires_pairing)

        connection._client.port = 8002
        connection._client._get_token.return_value = 'None'
        self.assertTrue(connection.requires_pairing)

        connection._client._get_token.return_value = 'saved-token'
        self.assertFalse(connection.requires_pairing)

    async def test_request_uses_connection_and_request_ids(self):
        client = self.make_client()
        client.get_uuid = mock.Mock(return_value='request-uuid')

        async def assert_start_precedes_request_lock():
            self.assertFalse(client._request_lock.locked())

        client.start_listening = assert_start_precedes_request_lock

        async def respond(command):
            payload = json.loads(command.get_payload())
            request = json.loads(payload['params']['data'])
            client.pending_requests[request['request_id']].set_result({
                'data': json.dumps({
                    'event': 'artmode_status',
                    'value': 'on',
                    'id': request['id'],
                    'request_id': request['request_id'],
                }),
            })

        client.send_command = respond

        response = await client._send_art_request({
            'request': 'get_artmode_status',
        })

        self.assertEqual(response['value'], 'on')
        self.assertEqual(response['id'], 'tv-connection-id')
        self.assertEqual(response['request_id'], 'request-uuid')
        self.assertEqual(client.pending_requests, {})

    async def test_start_waits_for_ready_event(self):
        client = self.make_client()
        client._channel_ready.clear()
        client._heartbeat_task = mock.Mock(done=mock.Mock(return_value=False))
        client.is_alive = mock.Mock(return_value=True)
        client.process_event = mock.AsyncMock()
        client.get_artmode = mock.AsyncMock(return_value='on')

        async def start_listener(instance, callback):
            client._channel_ready.set()
            return True

        with mock.patch.object(
            SamsungTVWSAsyncConnection,
            'start_listening',
            side_effect=start_listener,
        ):
            started = await client.start_listening()

        self.assertTrue(started)

    async def test_legacy_response_matches_connection_id(self):
        client = self.make_client()
        client.start_listening = mock.AsyncMock()
        client.get_uuid = mock.Mock(return_value='request-uuid')

        async def respond(command):
            client.pending_requests['tv-connection-id'].set_result({
                'data': json.dumps({
                    'event': 'artmode_status',
                    'value': 'off',
                    'id': 'tv-connection-id',
                }),
            })

        client.send_command = respond

        response = await client._send_art_request({
            'request': 'get_artmode_status',
        })

        self.assertEqual(response['value'], 'off')
        self.assertEqual(client.pending_requests, {})

    def test_passive_listener_disconnect_is_logged_at_info(self):
        client = self.make_client()
        listener = client._active_listener
        listener.cancelled.return_value = False
        listener.exception.return_value = ConnectionError('socket closed')

        client._on_listener_done(listener)

        client._response_logger.info.assert_called_once_with(
            'TV Art WebSocket disconnected (%s)',
            'ConnectionError: socket closed',
        )
        self.assertIsNone(client._channel_id)
        self.assertFalse(client._channel_ready.is_set())

    def test_intentional_listener_close_is_not_logged(self):
        client = self.make_client()
        client._suppress_disconnect_log = True

        client._on_listener_done(mock.Mock())

        client._response_logger.info.assert_not_called()

    def test_completed_listener_disconnect_is_logged_at_info(self):
        client = self.make_client()
        listener = client._active_listener
        listener.cancelled.return_value = False
        listener.exception.return_value = None

        client._on_listener_done(listener)

        client._response_logger.info.assert_called_once_with(
            'TV Art WebSocket disconnected (%s)',
            'listener stopped',
        )


if __name__ == '__main__':
    unittest.main()
