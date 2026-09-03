import asyncio
import json
import os
import socket
import time

from samsungtvws import exceptions
from samsungtvws.async_art import ArtChannelEmitCommand, SamsungTVAsyncArt
from samsungtvws.async_connection import SamsungTVWSAsyncConnection
from samsungtvws.event import MS_CHANNEL_CONNECT_EVENT, MS_CHANNEL_READY_EVENT


_REDACTED = '[REDACTED]'
_SENSITIVE_KEY_PARTS = ('token', 'password', 'secret', 'credential')

# Strong references to in-flight close tasks. asyncio keeps only weak references
# to tasks, so without this a close can be garbage collected mid-execution and
# leave the socket open on the TV.
_PENDING_CLOSE_TASKS = set()


def _sanitize_websocket_payload(value):
    if isinstance(value, dict):
        return {
            key: (
                _REDACTED
                if any(part in str(key).lower() for part in _SENSITIVE_KEY_PARTS)
                else _sanitize_websocket_payload(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_websocket_payload(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(('{', '[')):
            try:
                return _sanitize_websocket_payload(json.loads(value))
            except (TypeError, ValueError):
                pass
    return value


# The com.samsung.art-app channel completes its handshake independently of Art
# Mode: a healthy TV emits ms.channel.ready even while sitting on HDMI. A ready
# that never arrives therefore means the channel is wedged, not that Art Mode is
# simply off.
class ArtChannelNotReadyError(ConnectionError):
    """The socket is open but ms.channel.ready has not arrived yet.

    Distinct from a general ConnectionError because retrying immediately is
    pointless: a second attempt would just wait the same timeout for the same
    event on the same socket.
    """


class _LoggingSamsungTVAsyncArt(SamsungTVAsyncArt):
    def __init__(
        self,
        *args,
        response_logger,
        log_responses=False,
        max_response_chars=4000,
        **kwargs,
    ):
        self._response_logger = response_logger
        self._log_responses = log_responses
        self._max_response_chars = max_response_chars
        self._retired = False
        self._channel_id = None
        self._channel_ready = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._suppress_disconnect_log = False
        self._active_listener = None
        self._disconnect_callback = None
        self._token_logged = False
        self._ready_timeout = self._read_positive_seconds(
            'SAMSUNG_TV_ART_READY_TIMEOUT_SECONDS',
            10,
        )
        # A 2s budget was long enough to time out on a healthy but busy TV, and
        # each timeout used to discard a working socket.
        self._request_timeout = self._read_positive_seconds(
            'SAMSUNG_TV_ART_REQUEST_TIMEOUT_SECONDS',
            8,
        )
        super().__init__(*args, **kwargs)

    def _read_positive_seconds(self, name, default):
        try:
            return max(1.0, float(os.environ.get(name, str(default))))
        except ValueError:
            self._response_logger.warning(
                'Invalid %s value; using %ss',
                name,
                default,
            )
            return float(default)

    def get_token(self):
        # Logged once per client so repeated reconnect attempts do not bury the
        # actual connection failure under identical endpoint lines.
        should_log = not getattr(self, '_token_logged', False)
        self._token_logged = True
        if self.port == 8001:
            if should_log:
                self._response_logger.info(
                    'Art WebSocket uses ws://%s:8001; token pairing is skipped',
                    self.host,
                )
            return None
        if should_log:
            self._response_logger.info(
                'Art WebSocket uses wss://%s:8002; token authentication is enabled',
                self.host,
            )
        return super().get_token()

    async def open(self):
        if self._retired:
            raise ConnectionError('retired TV WebSocket cannot be reopened')
        self._suppress_disconnect_log = False
        self._active_listener = None
        self._channel_id = None
        self._channel_ready.clear()
        return await SamsungTVWSAsyncConnection.open(self)

    async def start_listening(self, *args, **kwargs):
        started = False
        async with self._start_lock:
            if self._retired:
                raise ConnectionError(
                    'retired TV WebSocket cannot be reopened'
                )
            if self._recv_loop is not None and self._recv_loop.done():
                try:
                    self._recv_loop.result()
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    self._response_logger.debug(
                        'Previous TV Art listener failed (%s): %s',
                        type(exc).__name__,
                        exc,
                    )
                self._recv_loop = None
            listener_running = (
                self._recv_loop is not None
                and not self._recv_loop.done()
            )
            if (
                self.is_alive()
                and listener_running
                and self._channel_ready.is_set()
            ):
                return False
            if not self.is_alive():
                try:
                    powered_on = await super().on()
                except Exception as exc:
                    await self.close()
                    raise ConnectionError(
                        'TV REST power probe failed'
                    ) from exc
                if not powered_on:
                    # The TV being off is the one case where the socket is
                    # genuinely gone; retiring here is correct.
                    self._retired = True
                    await self.close()
                    raise ConnectionError('TV is powered off')
                await self.open()
                listener_running = False

            if listener_running:
                # A live listener is reused. Re-entering the library's
                # start_listening would spawn a second receive loop on the same
                # socket, and every extra socket costs memory on the TV.
                started = True
            else:
                started = await SamsungTVWSAsyncConnection.start_listening(
                    self,
                    self.process_event,
                )
                if started and self._recv_loop is not None:
                    self._active_listener = self._recv_loop
                    self._recv_loop.add_done_callback(self._on_listener_done)
            try:
                await asyncio.wait_for(
                    self._channel_ready.wait(),
                    timeout=self._ready_timeout,
                )
            except asyncio.TimeoutError as exc:
                # Deliberately leave the socket open. Tearing it down and
                # opening a replacement every retry is what accumulates
                # sessions on the TV until it stops accepting new ones.
                # The listener stays live, so a late ms.channel.ready is
                # still picked up and the next call reuses this socket.
                raise ArtChannelNotReadyError(
                    'TV Art WebSocket did not emit ms.channel.ready within '
                    f'{self._ready_timeout:g}s'
                ) from exc
            if not self._channel_id:
                await self.close()
                raise ConnectionError(
                    'TV Art WebSocket did not provide a connection ID'
                )
        if started:
            try:
                await self.get_artmode()
            except AssertionError:
                pass
        return started

    def _on_listener_done(self, listener):
        if (
            self._suppress_disconnect_log
            or self._retired
            or listener is not self._active_listener
            or not self._channel_ready.is_set()
        ):
            return
        if listener.cancelled():
            reason = 'listener cancelled'
        else:
            exception = listener.exception()
            reason = (
                f'{type(exception).__name__}: {exception}'
                if exception
                else 'listener stopped'
            )
        self._response_logger.info(
            'TV Art WebSocket disconnected (%s)',
            reason,
        )
        self._channel_id = None
        self._channel_ready.clear()
        self._active_listener = None
        self._notify_disconnect()

    def _notify_disconnect(self):
        if self._disconnect_callback is None:
            return
        try:
            self._disconnect_callback()
        except Exception as exc:
            self._response_logger.warning(
                'TV Art WebSocket disconnect callback failed: %s',
                exc,
            )

    @property
    def socket_state(self):
        """WebSocket transport state, the Python analogue of readyState."""
        connection = getattr(self, 'connection', None)
        if connection is None:
            return 'no-socket'
        state = getattr(connection, 'state', None)
        if state is not None:
            return getattr(state, 'name', str(state))
        if getattr(connection, 'open', False):
            return 'OPEN'
        if getattr(connection, 'closed', False):
            return 'CLOSED'
        return 'unknown'

    @property
    def channel_ready(self):
        """True once ms.channel.ready has been observed for the live socket."""
        return self._channel_ready.is_set()

    async def close(self):
        self._suppress_disconnect_log = True
        try:
            await super().close()
        finally:
            self._channel_id = None
            self._channel_ready.clear()
            self._active_listener = None

    async def _send_art_request(
        self,
        request_data,
        wait_for_event=None,
        timeout=None,
    ):
        if timeout is None:
            timeout = self._request_timeout
        await self.start_listening()
        async with self._request_lock:
            listener_running = (
                self._recv_loop is not None
                and not self._recv_loop.done()
            )
            if (
                not self.is_alive()
                or not listener_running
                or not self._channel_ready.is_set()
                or not self._channel_id
            ):
                raise ArtChannelNotReadyError('TV Art WebSocket channel is not ready')

            request_data = dict(request_data)
            request_id = request_data.get('request_id') or self.get_uuid()
            request_data['id'] = self._channel_id
            request_data['request_id'] = request_id

            response_key = wait_for_event or request_id
            future = asyncio.get_running_loop().create_future()
            response_keys = {response_key, request_id}
            if wait_for_event is None:
                response_keys.add(self._channel_id)
            for key in response_keys:
                self.pending_requests[key] = future

            try:
                await self.send_command(
                    ArtChannelEmitCommand.art_app_request(request_data)
                )
                response = await asyncio.wait_for(future, timeout)
                data = json.loads(response['data'])
            except asyncio.TimeoutError:
                data = None
            finally:
                for key in response_keys:
                    if self.pending_requests.get(key) is future:
                        self.pending_requests.pop(key, None)

            if data and data.get('event', '*') == 'error':
                request = json.loads(data['request_data'])['request']
                raise exceptions.ResponseError(
                    f"{request} request failed with error number "
                    f"{data['error_code']}"
                )
            return data

    def _websocket_event(self, event, response):
        if event == MS_CHANNEL_CONNECT_EVENT:
            self._channel_id = response.get('data', {}).get('id')
        elif event == MS_CHANNEL_READY_EVENT:
            self._channel_ready.set()
        if self._log_responses:
            sanitized = _sanitize_websocket_payload(response)
            rendered = json.dumps(
                sanitized,
                ensure_ascii=False,
                default=repr,
                separators=(',', ':'),
            )
            if len(rendered) > self._max_response_chars:
                rendered = (
                    rendered[:self._max_response_chars]
                    + f'... [truncated {len(rendered) - self._max_response_chars} chars]'
                )
            self._response_logger.info(
                'TV WebSocket response event=%s payload=%s',
                event,
                rendered,
            )
        return super()._websocket_event(event, response)


class FrameTVConnection:
    """Own the Samsung TV client and its connection lifecycle."""

    def __init__(
        self,
        host,
        token_file,
        artmode_event,
        logger,
        reconnect_delay=5,
        power_state_callback=None,
    ):
        device_name = (
            os.environ.get('SAMSUNG_TV_ART_DEVICE_NAME')
            or socket.gethostname()
            or 'SamsungTvRemote'
        )
        # Samsung's Art channel is most consistently exposed over plain
        # WebSocket on 8001. Port 8002 remains available as an explicit override.
        port = int(os.environ.get('SAMSUNG_TV_ART_PORT', '8001'))
        log_responses = os.environ.get(
            'SAMSUNG_TV_ART_LOG_WEBSOCKET_RESPONSES',
            'false',
        ).lower() in ('1', 'true', 'yes')
        max_response_chars = int(os.environ.get(
            'SAMSUNG_TV_ART_WEBSOCKET_LOG_MAX_CHARS',
            '4000',
        ))
        self._client = _LoggingSamsungTVAsyncArt(
            host=host,
            port=port,
            token_file=token_file,
            name=device_name,
            response_logger=logger,
            log_responses=log_responses,
            max_response_chars=max(256, max_response_chars),
        )
        self._artmode_event = artmode_event
        self._logger = logger
        self._reconnect_delay = reconnect_delay
        self._power_state_callback = power_state_callback
        self._retired = False
        self._client._disconnect_callback = self._handle_art_disconnect
        self._artmode_failures = 0
        self._artmode_failure_limit = max(
            1,
            int(os.environ.get('SAMSUNG_TV_ART_REQUEST_FAILURE_LIMIT', '3')),
        )
        self._register_artmode_callbacks()

    def __getattr__(self, name):
        return getattr(self._client, name)

    def _signal_artmode_change(self):
        if self._artmode_event:
            self._artmode_event.set()

    def _handle_art_disconnect(self):
        if self._retired:
            return
        self._retired = True
        self._client._retired = True
        self._signal_artmode_change()

    def _notify_power_state(self, state):
        if self._power_state_callback:
            try:
                self._power_state_callback(state)
            except Exception as exc:
                self._logger.warning('TV power-state callback failed: %s', exc)

    def retire(self):
        """Permanently prevent this connection object from reopening its socket."""
        if self._retired:
            return
        self._retired = True
        self._client._retired = True
        try:
            task = asyncio.create_task(self._client.close())
        except RuntimeError:
            return
        # The event loop holds only weak references to tasks, so an unreferenced
        # close task can be garbage collected before the socket is torn down.
        # That leaks the session on the TV, which is what eventually makes it
        # refuse new connections until it is power-cycled.
        _PENDING_CLOSE_TASKS.add(task)
        task.add_done_callback(_PENDING_CLOSE_TASKS.discard)

    @property
    def retired(self):
        return self._retired or self._client._retired

    @property
    def port(self):
        return self._client.port

    @property
    def requires_pairing(self):
        if self.port != 8002:
            return False
        token = self._client._get_token()
        return not token or token.strip().lower() in ('none', 'null')

    def _register_artmode_callbacks(self):
        async def on_go_to_standby(event, response):
            self._logger.debug('TV WebSocket event: go_to_standby')
            self._notify_power_state('standby')
            self.retire()
            self._signal_artmode_change()

        async def on_art_mode_changed(event, response):
            try:
                data = json.loads(response['data'])
                new_state = data.get('status') == 'on'
            except Exception:
                new_state = None
            self._logger.debug(
                'TV WebSocket event: art_mode_changed (status=%s)',
                new_state,
            )
            self._signal_artmode_change()

        async def on_wakeup(event, response):
            self._logger.debug('TV WebSocket event: wakeup')
            self._notify_power_state('wakeup')
            self._signal_artmode_change()

        try:
            self._client.callbacks['go_to_standby'] = on_go_to_standby
            self._client.callbacks['art_mode_changed'] = on_art_mode_changed
            self._client.callbacks['wakeup'] = on_wakeup
        except Exception as exc:
            self._logger.warning(
                'Failed to register TV artmode callbacks: %s',
                exc,
            )

    async def is_powered_on(self):
        """Check TV power through HTTPS REST without opening an Art WebSocket."""
        started = time.monotonic()
        try:
            return bool(await self._client.on())
        finally:
            elapsed = time.monotonic() - started
            log_fn = self._logger.info if elapsed >= 1 else self._logger.debug
            log_fn('TV REST power probe took %.2fs', elapsed)

    async def query_artmode(self, power_verified=False):
        """Query Art Mode only after REST confirms that the TV is powered on."""
        if self._retired:
            raise ConnectionError('retired TV WebSocket cannot be queried')
        if not power_verified and not await self.is_powered_on():
            return False
        last_error = None
        for attempt in range(2):
            try:
                started = time.monotonic()
                status = await self._client.get_artmode()
                elapsed = time.monotonic() - started
                if status is None:
                    raise TimeoutError('empty Art Mode response')
                # A slow Art reply is the early warning that the channel is
                # degrading, so surface it instead of hiding it in debug.
                log_fn = (
                    self._logger.info
                    if elapsed >= 1
                    else self._logger.debug
                )
                log_fn(
                    'Art request get_artmode_status answered in %.2fs',
                    elapsed,
                )
                self._artmode_failures = 0
                return str(status).lower() == 'on'
            except ArtChannelNotReadyError as exc:
                # A second attempt would wait the same ready timeout for the
                # same event on the same socket, doubling the cost of every
                # probe against a TV whose art-app channel never comes up.
                last_error = exc
                self._logger.debug(
                    'get_artmode() attempt %d aborted, channel not ready: %s',
                    attempt + 1,
                    exc,
                )
                break
            except Exception as exc:
                last_error = exc
                self._logger.debug(
                    'get_artmode() attempt %d failed: %s',
                    attempt + 1,
                    exc,
                )
        self._artmode_failures += 1
        # Retiring on the first unanswered request threw away sockets that were
        # merely slow, and every replacement socket costs memory on the TV. Only
        # give up once the channel has failed repeatedly.
        if self._artmode_failures >= self._artmode_failure_limit:
            self._logger.info(
                'Retiring Art WebSocket after %d consecutive unanswered '
                'get_artmode_status requests: %s',
                self._artmode_failures,
                last_error,
            )
            self.retire()
            raise last_error if last_error else AssertionError('artmode query failed')
        self._logger.debug(
            'get_artmode_status unanswered (%d/%d); keeping the socket: %s',
            self._artmode_failures,
            self._artmode_failure_limit,
            last_error,
        )
        raise last_error if last_error else AssertionError('artmode query failed')
