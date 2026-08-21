import asyncio
import json
import os
import socket


class FrameTVConnection:
    """Own the Samsung TV client and its connection lifecycle."""

    def __init__(self, host, token_file, artmode_event, logger, reconnect_delay=5):
        from samsungtvws.async_art import SamsungTVAsyncArt

        device_name = (
            os.environ.get('SAMSUNG_TV_ART_DEVICE_NAME')
            or socket.gethostname()
            or 'SamsungTvRemote'
        )
        # Port 8002 is the SSL WebSocket default. Some Tizen 9.0 firmware
        # filters 8002 and requires the plain WebSocket port 8001.
        port = int(os.environ.get('SAMSUNG_TV_ART_PORT', '8002'))
        self._client = SamsungTVAsyncArt(
            host=host,
            port=port,
            token_file=token_file,
            name=device_name,
        )
        self._artmode_event = artmode_event
        self._logger = logger
        self._reconnect_delay = reconnect_delay
        self._register_artmode_callbacks()

    def __getattr__(self, name):
        return getattr(self._client, name)

    def _signal_artmode_change(self):
        if self._artmode_event:
            self._artmode_event.set()

    def _register_artmode_callbacks(self):
        async def on_go_to_standby(event, response):
            self._logger.debug('TV WebSocket event: go_to_standby')
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

    async def query_artmode(self):
        """Query Art Mode with a direct WebSocket fallback for legacy TVs."""
        try:
            if await self._client.in_artmode():
                return True
        except Exception as exc:
            self._logger.debug(
                'in_artmode() failed, falling back to get_artmode(): %s',
                exc,
            )

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

    async def reconnect(self, attempts=5):
        """Close the current socket and retry the Samsung listener."""
        try:
            await self._client.close()
        except Exception:
            pass

        for attempt in range(1, attempts + 1):
            try:
                await asyncio.sleep(self._reconnect_delay * attempt)
                await self._client.start_listening()
                if self._client.is_alive():
                    self._logger.info(
                        'Reconnected to TV on attempt %d',
                        attempt,
                    )
                    return True
            except Exception as exc:
                self._logger.warning(
                    'Reconnect attempt %d failed: %s',
                    attempt,
                    exc,
                )
        return False
