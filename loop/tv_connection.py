import asyncio
import json
import os
import socket

from samsungtvws.async_art import SamsungTVAsyncArt


_REDACTED = '[REDACTED]'
_SENSITIVE_KEY_PARTS = ('token', 'password', 'secret', 'credential')


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
        super().__init__(*args, **kwargs)

    async def start_listening(self, *args, **kwargs):
        if self._retired:
            raise ConnectionError('retired TV WebSocket cannot be reopened')
        try:
            powered_on = await super().on()
        except Exception as exc:
            self._retired = True
            await super().close()
            raise ConnectionError('TV REST power probe failed') from exc
        if not powered_on:
            self._retired = True
            await super().close()
            raise ConnectionError('TV is powered off')
        return await super().start_listening(*args, **kwargs)

    def _websocket_event(self, event, response):
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
        # Port 8002 is the SSL WebSocket default. Some Tizen 9.0 firmware
        # filters 8002 and requires the plain WebSocket port 8001.
        port = int(os.environ.get('SAMSUNG_TV_ART_PORT', '8002'))
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
        self._register_artmode_callbacks()

    def __getattr__(self, name):
        return getattr(self._client, name)

    def _signal_artmode_change(self):
        if self._artmode_event:
            self._artmode_event.set()

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
            asyncio.create_task(self._client.close())
        except RuntimeError:
            pass

    @property
    def retired(self):
        return self._retired or self._client._retired

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
        return bool(await self._client.on())

    async def query_artmode(self, power_verified=False):
        """Query Art Mode only after REST confirms that the TV is powered on."""
        if self._retired:
            raise ConnectionError('retired TV WebSocket cannot be queried')
        if not power_verified and not await self.is_powered_on():
            return False
        last_error = None
        for attempt in range(2):
            try:
                return str(await self._client.get_artmode()).lower() == 'on'
            except Exception as exc:
                last_error = exc
                self._logger.debug(
                    'get_artmode() attempt %d failed: %s',
                    attempt + 1,
                    exc,
                )
        raise last_error if last_error else AssertionError('artmode query failed')
