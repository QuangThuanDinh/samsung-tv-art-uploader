"""MQTT, settings, collection, and slideshow integration for the uploader."""

import asyncio
import json
import logging
import os
import re
import socket
import threading
import time
import uuid

from .BingDailyWallpaperManager import BING_COLLECTION_ID

try:
    import paho.mqtt.client as mqtt  # type: ignore
except Exception:
    mqtt = None


class MQTTLogHandler(logging.Handler):
    """Logging handler that forwards records to frame_tv/log (non-retained, QoS 0)."""
    TOPIC = 'frame_tv/log'

    def __init__(self, mqtt_client):
        super().__init__(level=logging.INFO)
        self.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))
        self._mqtt = mqtt_client
        self._publishing = False  # re-entrancy guard

    def emit(self, record):
        if self._publishing:
            return
        try:
            self._publishing = True
            msg = self.format(record)
            self._mqtt.publish(self.TOPIC, msg, qos=0, retain=False)
        except Exception:
            pass
        finally:
            self._publishing = False


class _MatteRejectedError(Exception):
    '''Raised when the TV rejects a matte for an image during apply (error -7).
    Carries the rejected matte id so the caller can surface a friendly message.'''
    def __init__(self, matte):
        super().__init__(f'TV rejected matte {matte!r} (-7)')
        self.matte = matte


class MQTTIntegrationMixin:
    """MQTT integration for a TV uploader host.

    The host supplies TV, upload, media scanning, and reseed methods; this
    mixin owns broker lifecycle, retained state, and command dispatch.
    """

    def _init_mqtt(self):
        if not self.mqtt_enabled or mqtt is None:
            return
        try:
            # paho-mqtt 2.x introduced CallbackAPIVersion. Prefer VERSION2 to avoid
            # deprecation warnings, but gracefully fall back to VERSION1 (paho 2.x)
            # or omit entirely (paho 1.x) when not available.
            _cb_cls = getattr(mqtt, 'CallbackAPIVersion', None)
            _cb_api = None
            if _cb_cls is not None:
                _cb_api = getattr(_cb_cls, 'VERSION2', None) or getattr(_cb_cls, 'VERSION1', None)
            _client_kwargs = dict(
                client_id=self._resolve_mqtt_client_id(),
                clean_session=True,
                protocol=getattr(mqtt, 'MQTTv311', 4),
            )
            if _cb_api is not None:
                _client_kwargs['callback_api_version'] = _cb_api
            self._mqtt = mqtt.Client(**_client_kwargs)
            if self.mqtt_username:
                self._mqtt.username_pw_set(self.mqtt_username, self.mqtt_password)
            # Setup callbacks and logging
            self._mqtt.on_connect = self._on_mqtt_connect
            self._mqtt.on_disconnect = self._on_mqtt_disconnect_compat
            self._mqtt.on_message = self._on_mqtt_message
            try:
                # Use a compatibility wrapper so both paho v1 and v2 callback
                # signatures are supported without warnings or crashes.
                self._mqtt.on_publish = self._on_mqtt_publish_compat
            except Exception:
                pass
            try:
                self._mqtt.enable_logger()
            except Exception:
                pass
            try:
                self._mqtt.reconnect_delay_set(min_delay=5, max_delay=60)
            except Exception:
                pass
            # Connect and start network loop.
            # keepalive=30 keeps the connection alive through typical NAT session
            # timeouts (many routers close idle TCP sessions after 30-60 s).
            self._mqtt.connect(self.mqtt_host, self.mqtt_port, keepalive=30)
            self._mqtt.loop_start()
            # Give it a brief moment to receive CONNACK
            for _ in range(20):
                if getattr(self, '_mqtt_is_connected', False):
                    break
                time.sleep(0.2)
            if not getattr(self, '_mqtt_is_connected', False):
                self.log.warning('MQTT: did not receive CONNACK yet; publishes may be dropped until connected')
            # Subscribe to topics once connected (also repeated in on_connect)
            if self.selection_from_mqtt:
                try:
                    self._mqtt.subscribe(self.selection_mqtt_topic, qos=1)
                except Exception:
                    pass
            try:
                self._mqtt.subscribe(f"{self.mqtt_cmd_prefix}/#", qos=1)
            except Exception:
                pass
            try:
                self._mqtt.subscribe(self.mqtt_slideshow_presets_topic, qos=1)
            except Exception:
                pass
            # Publish a diagnostic heartbeat to verify publish path
            try:
                self._mqtt.publish('frame_tv/diag/online', 'online', qos=0, retain=False)
            except Exception:
                pass
            self.log.info('MQTT connect initiated to %s:%d', self.mqtt_host, self.mqtt_port)
        except Exception as e:
            self.log.warning('MQTT init failed: %s', e)
            self._mqtt = None

    def _resolve_mqtt_client_id(self) -> str:
        """Return a stable, unique MQTT client_id for this container instance.
        Priority:
          1) Explicit override via SAMSUNG_TV_ART_MQTT_CLIENT_ID
          2) Persisted UUID in /data/client_id.txt (created on first run)
          3) HOSTNAME + short UUID suffix

        The final ID is sanitized to [A-Za-z0-9_-] and trimmed to <= 64 chars
        for broad broker compatibility.
        """
        try:
            # 1) Explicit override
            override = os.environ.get('SAMSUNG_TV_ART_MQTT_CLIENT_ID')
            if override:
                cid = override.strip()
            else:
                # 2) Persisted UUID in /data
                data_dir = '/data'
                cid_file = os.path.join(data_dir, 'client_id.txt')
                persisted = None
                try:
                    if os.path.isfile(cid_file):
                        with open(cid_file, 'r') as f:
                            persisted = f.read().strip()
                    else:
                        os.makedirs(data_dir, exist_ok=True)
                        persisted = str(uuid.uuid4())
                        # Write atomically
                        tmp_path = cid_file + '.tmp'
                        with open(tmp_path, 'w') as f:
                            f.write(persisted)
                        os.replace(tmp_path, cid_file)
                except Exception:
                    # Fall through to ephemeral if persistence fails
                    persisted = None

                host = os.environ.get('HOSTNAME') or socket.gethostname() or 'container'
                # 3) Compose ID
                suffix = (persisted or str(uuid.uuid4()))[:8]
                cid = f"frame-tv-art-{host}-{suffix}"

            # Sanitize and trim
            cid = re.sub(r'[^A-Za-z0-9_-]', '-', cid)
            if len(cid) > 64:
                cid = cid[:64]
            return cid
        except Exception:
            # Absolute fallback to a random UUID-based id
            return f"frame-tv-art-{str(uuid.uuid4())[:8]}"

    def _generate_default_presets(self):
        """Build default saved-selection presets from installed collections."""
        csv_data = getattr(self, '_csv_by_path', {})
        if not csv_data:
            # Fall back to scanning media_root directories
            csv_data = {}
            try:
                for d in os.listdir(self.media_root):
                    dp = os.path.join(self.media_root, d)
                    if not os.path.isdir(dp):
                        continue
                    for f in os.listdir(dp):
                        if f.lower().endswith(('.jpg','.jpeg','.png')):
                            csv_data[f'{d}/{f}'] = {'artwork_title': f, 'artwork_dir': d}
            except Exception:
                pass

        def _m(path, title, *kw):
            t = (path + ' ' + title).lower()
            return any(re.search(r'\b' + k + r'\b', t) for k in kw)

        marine_kw = ['sea','ocean','coast','coastal','beach','wave','waves','marine',
                     'harbour','harbor','port','sail','sailing','sailboat','boat','boats',
                     'ship','ships','fishing','steamer','pier','dock','bay','fleet',
                     'canal','barge','schooner','yacht','regatta','rowboat','whaling',
                     'seascape','fisherman','fishermen']
        land_kw   = ['landscape','valley','mountain','mountains','hill','hills','forest',
                     'woodland','meadow','field','countryside','rural','farm','pastoral',
                     'garden','park','river','lake','pond','waterfall','cliff','canyon',
                     'desert','moor','fjord','wilderness','trees']
        marine_excl = ['sea','ocean','harbour','harbor','port','sail','boat','ship',
                       'vessel','pier','dock','fleet','marine','coastal','beach','schooner']

        landscape_artists = {'Caspar_David_Friedrich','Albert_Bierstadt','Adalbert_Stifter',
                              'Antoine_Chintreuil','Arthur_Streeton','Frederick_McCubbin'}
        marine_artists    = {'Eugene_Boudin','Jacob_Maris','George_Wesley_Bellows'}
        imp_artists       = {'Claude_Monet','Pierre-Auguste_Renoir','Alfred_Sisley',
                              'Camille_Pissarro','Berthe_Morisot','Mary_Cassatt','Childe_Hassam'}
        abstract_artists  = {'Jackson_Pollock','Mark_Rothko','Paul_Klee','Max_Ernst',
                              'Franz_Marc','Andy_Warhol','Keith_Haring','Banksy',
                              'Pablo_Picasso','Marc_Chagall','Henri_Matisse','Gerhard Richter'}
        west_artists      = {'Frederic_Remington','Charles_Marion_Russell'}

        def artist_of(path):
            return path.split('/')[0] if '/' in path else ''

        landscapes, marine, impressionism, abstract_mod, west, portraits = [], [], [], [], [], []
        for path, row in csv_data.items():
            title = (row.get('artwork_title') or '').strip()
            art = artist_of(path)
            is_marine = _m(path, title, *marine_kw)
            is_land   = _m(path, title, *land_kw) and not _m(path, title, *marine_excl)
            if (art in landscape_artists and not is_marine) or \
               (is_land and art not in abstract_artists and art not in {'Banksy','Andy_Warhol',
                'Keith_Haring','Jackson_Pollock','Mark_Rothko','Pablo_Picasso','Henri_Matisse',
                'Marc_Chagall','Paul_Klee','Max_Ernst','Alphonse_Mucha','El_Greco',
                'Diego_Velazquez','Sandro_Botticelli','Francois_Boucher','Gustav_Klimt',
                'Rembrandt_Harmenszoon_van_Rijn','Leonardo_da_Vinci'}):
                landscapes.append(path)
            if art in marine_artists or is_marine:
                marine.append(path)
            if art in imp_artists:
                impressionism.append(path)
            if art in abstract_artists:
                abstract_mod.append(path)
            if art in west_artists:
                west.append(path)
            if _m(path, title, 'portrait', 'self-portrait', 'bust') and \
               not _m(path, title, 'landscape','mountain','forest','field','river','lake','boat','ship'):
                portraits.append(path)

        CHUNK = 30
        presets = []
        for name, paths in [
            ('Landscapes',       landscapes),
            ('Boats & Marine',    marine),
            ('Impressionism',     impressionism),
            ('Abstract & Modern', abstract_mod),
            ('American West',     west),
            ('Portraits',         portraits),
        ]:
            unique = sorted(set(paths))
            if not unique:
                continue
            if len(unique) <= CHUNK:
                presets.append({'name': name, 'paths': unique})
            else:
                # Split into numbered sub-presets of CHUNK images each
                chunks = [unique[i:i+CHUNK] for i in range(0, len(unique), CHUNK)]
                for idx, chunk in enumerate(chunks, 1):
                    presets.append({'name': f'{name} {idx}', 'paths': chunk})
        return presets

    def _bootstrap_default_presets(self):
        """Publish default presets if no retained message arrived after connect."""
        if self._presets_from_broker:
            return
        if not self._mqtt or not self._mqtt_is_connected:
            return
        try:
            defaults = self._generate_default_presets()
            if defaults:
                self._save_slideshow_presets(defaults)
                self._mqtt.publish(
                    self.mqtt_slideshow_presets_topic,
                    json.dumps(defaults),
                    qos=1, retain=True,
                )
                self.log.info('Published %d default preset(s) to %s',
                              len(defaults), self.mqtt_slideshow_presets_topic)
        except Exception as e:
            self.log.warning('Failed to publish default presets: %s', e)

    def _on_mqtt_message(self, client, userdata, msg):
        try:
            topic = getattr(msg, 'topic', '')
            if topic == self.mqtt_slideshow_presets_topic:
                self._presets_from_broker = True
                if self._presets_bootstrap_timer:
                    self._presets_bootstrap_timer.cancel()
                    self._presets_bootstrap_timer = None
            if self.selection_from_mqtt and topic == self.selection_mqtt_topic:
                if (
                    getattr(msg, 'retain', False)
                    and self._ignore_retained_selection_until_reconnect
                ):
                    self.log.info(
                        'Ignoring stale retained collection selection after explicit UI update'
                    )
                    return
                payload = msg.payload.decode('utf-8') if isinstance(msg.payload, (bytes, bytearray)) else str(msg.payload or '')
                raw_cols = [c.strip() for c in (payload or '').split(',') if c.strip()]
                # Map retained selections to artwork_dir folder names when possible
                mapped = []
                try:
                    have = set(self._scan_collections())
                except Exception:
                    have = set()
                for c in raw_cols:
                    mc = self._map_to_artwork_dir(c) or c
                    # Keep only entries that exist as directories under media_root
                    if mc in have and mc not in mapped:
                        mapped.append(mc)
                mapped = self.bing_daily.normalize_collections(mapped)
                if mapped != self.selected_collections:
                    self.selected_collections = mapped
                    self._pending_selection_change = True
                    self._cache_selected_collections()
                    self.log.info('Received MQTT selection update (mapped from %s): %s', raw_cols, self.selected_collections)
                return
            # Command handling
            if topic.startswith(f"{self.mqtt_cmd_prefix}/"):
                payload_raw = msg.payload.decode('utf-8') if isinstance(msg.payload, (bytes, bytearray)) else (msg.payload or '')
                cmd = topic[len(self.mqtt_cmd_prefix)+1:]
                self.log.debug(
                    'Received MQTT command on %s: %s',
                    topic,
                    payload_raw if isinstance(payload_raw, str) else '<binary>',
                )
                self._handle_mqtt_command(cmd, payload_raw)
        except Exception as e:
            self.log.warning('Failed handling MQTT message: %s', e)

    def _schedule_command_coro(self, coro, label='command'):
        """Schedule a coroutine from MQTT callback context onto the main event loop."""
        try:
            loop = self._loop
            if loop and loop.is_running():
                return asyncio.run_coroutine_threadsafe(coro, loop)
        except Exception as e:
            self.log.warning('Failed to schedule %s: %s', label, e)
        try:
            # If scheduling failed, ensure we close the coroutine to avoid warnings
            coro.close()
        except Exception:
            pass
        return None

    # MQTT callbacks (connection lifecycle)
    def _on_mqtt_connect(self, client, userdata, flags, rc, properties=None):  # properties for MQTTv5 compatibility
        try:
            rc_val = getattr(rc, 'value', rc)
            self._mqtt_is_connected = (rc_val == 0)
            if rc_val == 0:
                self._ignore_retained_selection_until_reconnect = False
                self.log.info('MQTT connected (CONNACK rc=0)')
                # Attach MQTT log handler so all INFO+ records stream to frame_tv/log
                if not getattr(self, '_mqtt_log_handler', None):
                    self._mqtt_log_handler = MQTTLogHandler(client)
                    logging.getLogger().addHandler(self._mqtt_log_handler)
                # Ensure subscriptions are in place after reconnects
                try:
                    if self.selection_from_mqtt:
                        client.subscribe(self.selection_mqtt_topic, qos=1)
                    client.subscribe(f"{self.mqtt_cmd_prefix}/#", qos=1)
                    client.subscribe(self.mqtt_slideshow_presets_topic, qos=1)
                except Exception:
                    pass
                # Bootstrap presets:
                #  1) If we have presets persisted on disk, republish them as retained
                #     so the broker (which may have lost retain across its own restart
                #     or a fresh container) is reseeded from our local source of truth.
                #  2) Otherwise, wait briefly for a retained message from the broker;
                #     if none arrives, generate defaults from installed collections.
                if self._slideshow_presets:
                    try:
                        client.publish(
                            self.mqtt_slideshow_presets_topic,
                            json.dumps(self._slideshow_presets),
                            qos=1, retain=True,
                        )
                        self._presets_from_broker = True
                        self.log.info('Republished %d persisted preset(s) to %s',
                                      len(self._slideshow_presets), self.mqtt_slideshow_presets_topic)
                    except Exception as e:
                        self.log.warning('Failed to republish persisted presets: %s', e)
                elif not self._presets_from_broker:
                    if self._presets_bootstrap_timer:
                        self._presets_bootstrap_timer.cancel()
                    t = threading.Timer(3.0, self._bootstrap_default_presets)
                    t.daemon = True
                    t.start()
                    self._presets_bootstrap_timer = t
                # Republish retained state outside Paho's network callback. Several
                # helpers wait for QoS completion, which deadlocks the callback thread
                # and causes keepalive disconnect/reconnect loops if run inline here.
                def republish_retained_state():
                    try:
                        self._publish_collections_state()
                        self._publish_settings_state()
                        self._publish_matte_overrides()
                    except Exception as e:
                        self.log.warning('Failed to republish retained MQTT state: %s', e)

                worker = threading.Thread(
                    target=republish_retained_state,
                    name='mqtt-state-republisher',
                    daemon=True,
                )
                worker.start()
                # Reset artwork state refresh timer so the main loop immediately
                # re-publishes the current artwork state after any (re)connect.
                self._last_state_publish = 0
            else:
                self.log.warning('MQTT connect failed (rc=%s)', str(rc_val))
        except Exception:
            pass

    def _on_mqtt_disconnect(self, client, userdata, rc, properties=None):
        try:
            # Guard against paho firing the callback twice for the same disconnection
            if not self._mqtt_is_connected:
                return
            self._mqtt_is_connected = False
            rc_val = getattr(rc, 'value', rc)
            self.log.warning('MQTT disconnected (rc=%s)', str(rc_val))
            # Detach MQTT log handler on disconnect to avoid publish errors
            if getattr(self, '_mqtt_log_handler', None):
                logging.getLogger().removeHandler(self._mqtt_log_handler)
                self._mqtt_log_handler = None
        except Exception:
            pass

    # paho v2 passes extra positional args (disconnect_flags, reason_code, properties).
    def _on_mqtt_disconnect_compat(self, client, userdata, *args, **kwargs):
        try:
            # Last positional arg before any trailing properties is the rc/reason_code
            rc = args[0] if args else 0
            return self._on_mqtt_disconnect(client, userdata, rc)
        except Exception:
            pass

    def _on_mqtt_publish(self, client, userdata, mid):
        try:
            self.log.debug('MQTT published (mid=%s)', str(mid))
        except Exception:
            pass

    # paho v2 passes extra positional args (properties, reasonCode). Accept and ignore.
    def _on_mqtt_publish_compat(self, client, userdata, mid, *args, **kwargs):
        try:
            return self._on_mqtt_publish(client, userdata, mid)
        except Exception:
            pass

    def _publish_mqtt_discovery(self):
        if not self.mqtt_enabled or not self._mqtt or self._mqtt_config_published:
            return
        if not self.mqtt_discovery:
            # Discovery disabled — just mark availability and skip HA config payload
            try:
                self._publish_and_wait(f"{self.mqtt_state_topic}/availability", "online", qos=1, retain=True)
            except Exception:
                pass
            self._mqtt_config_published = True
            return
        try:
            obj_id = self.mqtt_unique_id
            cfg_topic = f"{self.mqtt_discovery_prefix}/sensor/{obj_id}/config"
            device = {
                "identifiers": [f"frame_tv_art_{self.ip}"],
                "name": "Frame TV Art",
                "manufacturer": "Custom",
                "model": "Art Uploader",
            }
            payload = {
                "name": "Frame TV Selected Artwork",
                "default_entity_id": "sensor.frame_tv_art_selected_artwork",
                "state_topic": self.mqtt_state_topic,
                "json_attributes_topic": self.mqtt_attr_topic,
                "unique_id": obj_id,
                "icon": "mdi:image-text",
                "device": device,
                "availability_topic": f"{self.mqtt_state_topic}/availability",
                # Ensure entity is enabled by default in registry
                "enabled_by_default": True,
                "entity_registry_enabled_default": True,
            }
            try:
                self._publish_and_wait(cfg_topic, json.dumps(payload), qos=1, retain=True)
            except Exception:
                self._mqtt.publish(cfg_topic, json.dumps(payload), qos=1, retain=True)
            # Mark available
            try:
                self._publish_and_wait(f"{self.mqtt_state_topic}/availability", "online", qos=1, retain=True)
            except Exception:
                self._mqtt.publish(f"{self.mqtt_state_topic}/availability", "online", qos=0, retain=True)

            # Also publish discovery for 'Selected Collections' state sensor
            sel_obj_id = 'frame_tv_art_selected_collections'
            sel_cfg_topic = f"{self.mqtt_discovery_prefix}/sensor/{sel_obj_id}/config"
            sel_payload = {
                "name": "Frame TV Selected Collections",
                "default_entity_id": "sensor.frame_tv_art_selected_collections",
                "state_topic": self.mqtt_selected_collections_state_topic,
                "json_attributes_topic": self.mqtt_selected_collections_attr_topic,
                "unique_id": sel_obj_id,
                "icon": "mdi:folder-multiple",
                "device": device,
                # Ensure entity is enabled by default in registry
                "enabled_by_default": True,
                "entity_registry_enabled_default": True,
            }
            try:
                self._publish_and_wait(sel_cfg_topic, json.dumps(sel_payload), qos=1, retain=True)
            except Exception:
                self._mqtt.publish(sel_cfg_topic, json.dumps(sel_payload), qos=1, retain=True)

            self._mqtt_config_published = True
            self.log.info('Published MQTT discovery to %s and %s', cfg_topic, sel_cfg_topic)
        except Exception as e:
            self.log.warning('MQTT discovery publish failed: %s', e)

    def _publish_mqtt_state(self, display, file, collection):
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            # State = display text; attributes carry file and collection
            try:
                self._publish_and_wait(self.mqtt_state_topic, display or "", qos=1, retain=True)
            except Exception:
                self._mqtt.publish(self.mqtt_state_topic, display or "", qos=0, retain=True)
            attrs = {"file": file or "", "collection": collection or ""}
            attrs["art_mode_state"] = (
                "unknown"
                if self._in_art_mode is None
                else ("on" if self._in_art_mode else "off")
            )
            # Only include in_art_mode when we actually know.  Writing False
            # while the state is still None (unknown) retains a stale 'Not in
            # art mode' for the web UI until the next confirmed-True transition.
            if self._in_art_mode is not None:
                attrs["in_art_mode"] = bool(self._in_art_mode)
            tv_powered_on = getattr(self, '_tv_powered_on', None)
            if tv_powered_on is not None:
                attrs["tv_power_on"] = bool(tv_powered_on)
            # Merge CSV columns (ensure every header key exists, even if blank)
            if self._csv_headers:
                path_key = f"{collection}/{file}" if collection and file else None
                row = self.museum_labels.metadata_for_path(
                    path_key or file or '',
                    getattr(self, '_csv_by_path', {}),
                    self._csv_by_file,
                )
                for h in self._csv_headers:
                    # Keep original header key names to match CSV
                    attrs[h] = str(row.get(h, "") or "")
            try:
                self._publish_and_wait(self.mqtt_attr_topic, json.dumps(attrs, separators=(",", ":")), qos=1, retain=True)
            except Exception:
                self._mqtt.publish(self.mqtt_attr_topic, json.dumps(attrs, separators=(",", ":")), qos=0, retain=True)
        except Exception as e:
            self.log.warning('MQTT state publish failed: %s', e)

    def _scan_collections(self):
        """Return list of collection paths relative to media_root.
        A directory is a flat collection if it contains image files directly.
        A directory with no images but with image-containing subdirectories is
        treated as a multi-collection repo; each qualifying subdir is returned
        as 'repo/subdir' (using os.sep-compatible join).
        """
        SKIP = {'@eaDir', '@tmp'}
        IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.bmp', '.tif'}

        def _has_images(path):
            try:
                images = [
                    f
                    for f in os.listdir(path)
                    if (
                    os.path.isfile(os.path.join(path, f))
                    and os.path.splitext(f)[1].lower() in IMAGE_EXT
                    )
                ]
                return bool(self.museum_labels.preferred_filenames(path, images))
            except Exception:
                return False

        try:
            result = [BING_COLLECTION_ID]
            for d in sorted(os.listdir(self.media_root)):
                if d in SKIP or d == BING_COLLECTION_ID:
                    continue
                dir_path = os.path.join(self.media_root, d)
                if not os.path.isdir(dir_path):
                    continue
                if _has_images(dir_path):
                    # Flat collection — images live directly in this directory
                    result.append(d)
                else:
                    # Possibly a multi-collection repo — check one level deeper
                    try:
                        for s in sorted(os.listdir(dir_path)):
                            if s in SKIP:
                                continue
                            sub_path = os.path.join(dir_path, s)
                            if os.path.isdir(sub_path) and _has_images(sub_path):
                                result.append(os.path.join(d, s))
                    except Exception:
                        pass
            return result
        except Exception as e:
            self.log.warning('Failed to scan collections in %s: %s', self.media_root, e)
            return []

    def _publish_collections_discovery(self):
        if not self.mqtt_enabled or not self._mqtt or not self.mqtt_discovery:
            return
        try:
            obj_id = self.mqtt_collections_unique_id
            cfg_topic = f"{self.mqtt_discovery_prefix}/sensor/{obj_id}/config"
            device = {
                "identifiers": [f"frame_tv_art_{self.ip}"],
                "name": "Frame TV Art",
                "manufacturer": "Custom",
                "model": "Art Uploader",
            }
            payload = {
                "name": "Frame TV Art Collections",
                "default_entity_id": "sensor.frame_tv_art_collections",
                "state_topic": self.mqtt_collections_state_topic,
                "json_attributes_topic": self.mqtt_collections_attr_topic,
                "unique_id": obj_id,
                "icon": "mdi:folder-multiple-image",
                "device": device,
                # Ensure entity is enabled by default in registry
                "enabled_by_default": True,
                "entity_registry_enabled_default": True,
            }
            try:
                self._publish_and_wait(cfg_topic, json.dumps(payload), qos=1, retain=True)
            except Exception:
                self._mqtt.publish(cfg_topic, json.dumps(payload), qos=1, retain=True)
        except Exception as e:
            self.log.warning('MQTT collections discovery publish failed: %s', e)

    def _publish_settings_discovery(self):
        if not self.mqtt_enabled or not self._mqtt or not self.mqtt_discovery:
            return
        try:
            obj_id = 'frame_tv_art_settings'
            cfg_topic = f"{self.mqtt_discovery_prefix}/sensor/{obj_id}/config"
            device = {
                "identifiers": [f"frame_tv_art_{self.ip}"],
                "name": "Frame TV Art",
                "manufacturer": "Custom",
                "model": "Art Uploader",
            }
            payload = {
                "name": "Frame TV Settings",
                "default_entity_id": "sensor.frame_tv_art_settings",
                "state_topic": self.mqtt_settings_state_topic,
                "json_attributes_topic": self.mqtt_settings_attr_topic,
                "unique_id": obj_id,
                "icon": "mdi:cog",
                "device": device,
                "enabled_by_default": True,
            }
            try:
                self._publish_and_wait(cfg_topic, json.dumps(payload), qos=1, retain=True)
            except Exception:
                self._mqtt.publish(cfg_topic, json.dumps(payload), qos=1, retain=True)
        except Exception as e:
            self.log.warning('MQTT settings discovery publish failed: %s', e)

    def _publish_settings_state(self):
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            attrs = {
                "SAMSUNG_TV_ART_MAX_UPLOADS": str(os.environ.get('SAMSUNG_TV_ART_MAX_UPLOADS', '30')),
                "SAMSUNG_TV_ART_UPDATE_MINUTES": str(int(max(0, (self.update_time or 0) / 60))),
                "SAMSUNG_TV_ART_TV_IP": str(os.environ.get('SAMSUNG_TV_ART_TV_IP', self.ip or '')),
                "SAMSUNG_TV_ART_SEQUENTIAL": '1' if self.sequential else '0',
                "SAMSUNG_TV_ART_MQTT_HOST": str(os.environ.get('SAMSUNG_TV_ART_MQTT_HOST', self.mqtt_host or '')),
                "SAMSUNG_TV_ART_MQTT_PORT": str(os.environ.get('SAMSUNG_TV_ART_MQTT_PORT', str(self.mqtt_port or 1883))),
                "SAMSUNG_TV_ART_MQTT_WS_HOST": str(os.environ.get('SAMSUNG_TV_ART_MQTT_WS_HOST', '')),
                "SAMSUNG_TV_ART_MQTT_WS_PORT": str(os.environ.get('SAMSUNG_TV_ART_MQTT_WS_PORT', '9001')),
                "SAMSUNG_TV_ART_MQTT_USERNAME": str(os.environ.get('SAMSUNG_TV_ART_MQTT_USERNAME', self.mqtt_username or '')),
            }
            # Settings updates can originate in paho's callback thread, where
            # waiting for a QoS acknowledgment blocks the network loop itself.
            self._mqtt.publish(
                self.mqtt_settings_state_topic,
                "online",
                qos=1,
                retain=True,
            )
            self._mqtt.publish(
                self.mqtt_settings_attr_topic,
                json.dumps(attrs, separators=(",", ":")),
                qos=1,
                retain=True,
            )
        except Exception as e:
            self.log.warning('MQTT settings state publish failed: %s', e)

    # ── Slideshow override persistence ──────────────────────────────────────

    def _load_slideshow_override(self):
        """Load persisted slideshow override from /data/slideshow_override.json."""
        try:
            if os.path.isfile(self.slideshow_override_path):
                with open(self.slideshow_override_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                paths = data.get('paths', [])
                self.slideshow_override = paths if paths else None
                self.slideshow_override_pending = bool(
                    self.slideshow_override and data.get('pending', False)
                )
                self.slideshow_override_force_reupload = bool(
                    self.slideshow_override_pending
                    and data.get('force_reupload', False)
                )
                if self.slideshow_override:
                    self.log.info(
                        'Loaded slideshow override with %d paths (pending=%s)',
                        len(self.slideshow_override),
                        self.slideshow_override_pending,
                    )
        except Exception as e:
            self.log.warning('Failed to load slideshow override: %s', e)
            self.slideshow_override = None
            self.slideshow_override_pending = False
            self.slideshow_override_force_reupload = False

    def _save_slideshow_override(self):
        """Persist slideshow override to /data/slideshow_override.json."""
        try:
            os.makedirs('/data', exist_ok=True)
            data = {
                'paths': list(self.slideshow_override) if self.slideshow_override else [],
                'pending': bool(
                    self.slideshow_override and self.slideshow_override_pending
                ),
                'force_reupload': bool(
                    self.slideshow_override
                    and self.slideshow_override_pending
                    and self.slideshow_override_force_reupload
                ),
            }
            with open(self.slideshow_override_path, 'w', encoding='utf-8') as f:
                json.dump(data, f)
        except Exception as e:
            self.log.warning('Failed to save slideshow override: %s', e)

    # ── Slideshow presets persistence ──────────────────────────────────────

    def _load_slideshow_presets(self):
        """Load persisted slideshow presets from /data/slideshow_presets.json."""
        try:
            if os.path.isfile(self.slideshow_presets_path):
                with open(self.slideshow_presets_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    self._slideshow_presets = data
                    self.log.info('Loaded %d persisted slideshow preset(s)', len(data))
        except Exception as e:
            self.log.warning('Failed to load slideshow presets: %s', e)
            self._slideshow_presets = []

    def _save_slideshow_presets(self, presets):
        """Persist slideshow presets to /data/slideshow_presets.json."""
        try:
            os.makedirs('/data', exist_ok=True)
            with open(self.slideshow_presets_path, 'w', encoding='utf-8') as f:
                json.dump(presets if isinstance(presets, list) else [], f)
            self._slideshow_presets = presets if isinstance(presets, list) else []
        except Exception as e:
            self.log.warning('Failed to save slideshow presets: %s', e)

    # ── Per-image matte overrides ───────────────────────────────────────────

    def _load_matte_overrides(self):
        """Load persisted per-image matte overrides from disk."""
        try:
            if os.path.isfile(self.matte_overrides_path):
                with open(self.matte_overrides_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    # filter to {str: str}
                    self._matte_overrides = {
                        str(k): str(v) for k, v in data.items()
                        if isinstance(k, str) and isinstance(v, str) and v
                    }
                    self.log.info('Loaded %d persisted matte override(s)', len(self._matte_overrides))
        except Exception as e:
            self.log.warning('Failed to load matte overrides: %s', e)
            self._matte_overrides = {}

    def _save_matte_overrides(self):
        """Persist per-image matte overrides to disk."""
        try:
            os.makedirs('/data', exist_ok=True)
            with open(self.matte_overrides_path, 'w', encoding='utf-8') as f:
                json.dump(self._matte_overrides, f)
        except Exception as e:
            self.log.warning('Failed to save matte overrides: %s', e)

    def _resolve_matte_for(self, path_rel, fname=None):
        """Resolve effective matte for an image.
        Priority: per-image override → CSV 'matte' column → global self.matte default.
        """
        try:
            if path_rel and path_rel in self._matte_overrides:
                return self._matte_overrides[path_rel]
            if fname and fname in self._matte_overrides:
                return self._matte_overrides[fname]
            csv_rec = self.museum_labels.metadata_for_path(
                path_rel or fname or '',
                getattr(self, '_csv_by_path', {}),
                self._csv_by_file,
            )
            csv_matte = (csv_rec.get('matte') or '').strip()
            if csv_matte:
                return csv_matte
        except Exception:
            pass
        return self.matte

    def _publish_matte_overrides(self):
        """Publish current per-image matte overrides as a retained MQTT map."""
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            payload = json.dumps(self._matte_overrides, separators=(',', ':'))
            self._mqtt.publish(self.mqtt_slideshow_mattes_topic, payload, qos=1, retain=True)
        except Exception as e:
            self.log.debug('MQTT mattes publish failed: %s', e)

    async def _ensure_matte_options_cache(self):
        """Populate self._matte_options_cache from the TV (or fallback). Safe to
        call from anywhere — no MQTT side effects. Used by both the MQTT
        publisher and the background matte probe worker."""
        if self._matte_options_cache is not None:
            return
        try:
            mattes = await self.tv.get_matte_list(True)
            if isinstance(mattes, (list, tuple)) and len(mattes) >= 2:
                types = [m.get('matte_type') for m in mattes[0] if isinstance(m, dict) and m.get('matte_type')]
                colors = [m.get('color') for m in mattes[1] if isinstance(m, dict) and m.get('color')]
                self._matte_options_cache = {'matte_types': types, 'matte_colors': colors}
            elif isinstance(mattes, dict):
                self._matte_options_cache = {
                    'matte_types': [m.get('matte_type') for m in (mattes.get('matte_types') or []) if isinstance(m, dict) and m.get('matte_type')],
                    'matte_colors': [m.get('color') for m in (mattes.get('matte_colors') or []) if isinstance(m, dict) and m.get('color')],
                }
        except Exception as e:
            self.log.debug('get_matte_list failed, using fallback: %s', e)
            self._matte_options_cache = {
                'matte_types':  ['none', 'shadowbox', 'modern', 'modernthin', 'modernwide',
                                 'flexible', 'panoramic', 'triptych', 'mix', 'squares'],
                'matte_colors': ['polar', 'neutral', 'apricot', 'sand', 'seafoam', 'lavender',
                                 'burgandy', 'navy', 'forest', 'dark', 'warm', 'sage'],
            }

    async def _publish_matte_options(self):
        """Fetch (and cache) the TV's matte_type/matte_color list, publish as retained."""
        if not self.mqtt_enabled or not self._mqtt:
            return
        await self._ensure_matte_options_cache()
        try:
            payload = json.dumps(self._matte_options_cache, separators=(',', ':'))
            self._mqtt.publish(self.mqtt_slideshow_matte_options_topic, payload, qos=1, retain=True)
        except Exception as e:
            self.log.debug('MQTT matte_options publish failed: %s', e)

    @staticmethod
    def _is_matte_minus_7(exc):
        """The Samsung Art API returns error code -7 when it rejects a matte
        combo for an image (or, harmlessly, when 'none' is reapplied to an
        image that already has no matte). We treat -7 specially in two places:
          1. `slideshow/matte/set` handler — surface a red toast and revert.
          2. `_reapply_matte_for` (per-cycle reapply) — drop the override back
             to 'none' so we don't keep retrying a combo the TV won't accept."""
        msg = str(exc)
        return 'error number -7' in msg or "'-7'" in msg or 'errno -7' in msg

    def _publish_slideshow_state(self):
        """Publish current slideshow mode, settings, and active paths to MQTT."""
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            mode = 'override' if self.slideshow_override is not None else 'auto'
            current_paths = [v.get('path_rel', k) for k, v in self.uploaded_files.items()]
            active_paths = (
                list(self.slideshow_override)
                if self.slideshow_override is not None
                else current_paths
            )
            missing_rel = [k for k, v in self.uploaded_files.items() if not v.get('path_rel')]
            if missing_rel:
                self.log.warning('uploaded_files entries missing path_rel (will use basename): %s', missing_rel)
            attrs = {
                'mode': mode,
                'sequential': self.sequential,
                'update_minutes': int(max(0, (self.update_time or 0) / 60)),
                'max_uploads': self.max_uploads,
                'override_paths': list(self.slideshow_override) if self.slideshow_override else [],
                'pending': self.slideshow_override_pending,
                'current_paths': current_paths,
                'matte_mismatch_paths': self._slideshow_paths_requiring_upload(active_paths),
                'uploading': bool(self._refresh_in_progress or getattr(self, '_startup_in_progress', False)),
            }
            # This method is also called from paho's MQTT callback thread.
            # Waiting for QoS completion there deadlocks delivery until the wait
            # times out, so enqueue retained publishes and let paho flush them.
            self._mqtt.publish(
                self.mqtt_slideshow_state_topic,
                mode,
                qos=1,
                retain=True,
            )
            self._mqtt.publish(
                self.mqtt_slideshow_attr_topic,
                json.dumps(attrs, separators=(',', ':')),
                qos=1,
                retain=True,
            )
        except Exception as e:
            self.log.warning('MQTT slideshow state publish failed: %s', e)

    def _slideshow_paths_requiring_upload(self, paths):
        """Return paths missing from the TV cache or cached with another matte."""
        uploaded_by_path = {
            record.get('path_rel', key): record
            for key, record in self.uploaded_files.items()
        }
        requiring_upload = []
        for path in paths:
            uploaded = uploaded_by_path.get(path)
            desired_matte = (
                self._resolve_matte_for(path, os.path.basename(path)) or 'none'
            )
            signature = self._get_file_signature(os.path.join(self.media_root, path))
            modified = None
            try:
                modified = self.get_last_updated(
                    os.path.basename(path),
                    os.path.join(self.media_root, path),
                )
            except OSError:
                pass
            legacy_changed = (
                uploaded
                and not uploaded.get('sha256')
                and (
                    uploaded.get('modified') is None
                    or modified is None
                    or uploaded.get('modified') != modified
                )
            )
            if (
                not uploaded
                or uploaded.get('matte') != desired_matte
                or legacy_changed
                or (
                    uploaded.get('sha256')
                    and signature.get('sha256')
                    and uploaded['sha256'] != signature['sha256']
                )
            ):
                requiring_upload.append(path)
        return requiring_upload

    async def _cached_upload_matches_tv(self, path, record, live_by_id):
        """Confirm a cached ID still refers to the same TV upload."""
        content_id = record.get('content_id')
        live_entry = live_by_id.get(content_id)
        if not content_id or not live_entry:
            return False
        cached_date = record.get('image_date')
        live_date = live_entry.get('image_date')
        if cached_date is not None and live_date is not None:
            return cached_date == live_date
        matched = await self.pil.matches_file(
            content_id,
            os.path.join(self.media_root, path),
        )
        if matched and live_date is not None:
            record['image_date'] = live_date
        return matched is True

    def _publish_slideshow_available(self, override_collections=None, wait_for_publish=True):
        """Scan selected collections on disk and publish full image list to MQTT.
        Pass override_collections to preview a candidate set without committing it.
        """
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            # Build a set of path_rel values currently uploaded to the TV
            uploaded_paths = {v.get('path_rel', k) for k, v in self.uploaded_files.items()}
            effective_collections = override_collections if override_collections is not None else (self.selected_collections or [])
            selected_set = set(effective_collections)

            images = []
            for collection in list(effective_collections):
                coll_path = os.path.join(self.media_root, collection)
                if not os.path.isdir(coll_path):
                    continue
                try:
                    raw_files = os.listdir(coll_path)
                    # Cache the raw listing so _preview_random_selection can reuse it
                    # without a second cold scan on first shuffle click.
                    if override_collections is None:
                        if not hasattr(self, '_collection_file_cache'):
                            self._collection_file_cache = {}
                        self._collection_file_cache[coll_path] = raw_files
                    image_files = [
                        f for f in raw_files
                        if f.lower().endswith(('.jpg', '.jpeg', '.png'))
                    ]
                    files = self.museum_labels.preferred_filenames(
                        coll_path,
                        image_files,
                    )
                    if collection == BING_COLLECTION_ID:
                        files = self.bing_daily.preview_filenames(files)
                    else:
                        files = sorted(files)
                except Exception:
                    continue
                for fname in files:
                    path_rel = f"{collection}/{fname}"
                    full_path = os.path.join(coll_path, fname)
                    try:
                        modified = os.stat(full_path).st_mtime_ns
                    except OSError:
                        modified = 0
                    csv_rec = self.museum_labels.metadata_for_path(
                        path_rel,
                        getattr(self, '_csv_by_path', {}),
                        self._csv_by_file,
                    )
                    artist = (csv_rec.get('artist_name') or '').strip()
                    if not artist:
                        artist = getattr(self, '_dir_to_artist', {}).get(collection, '').strip()
                    images.append({
                        'path': path_rel,
                        'folder': collection,
                        'file': fname,
                        'title': (csv_rec.get('title') or csv_rec.get('artwork_title') or '').strip(),
                        'artist': artist,
                        'year': (csv_rec.get('year') or '').strip(),
                        'uploaded': path_rel in uploaded_paths,
                        'modified': modified,
                    })

            # Cap to 5000 images to keep the MQTT payload manageable while covering
            # large libraries (at ~200 bytes/entry, 5000 ≈ 1MB — fine for any modern broker).
            # Always include every uploaded file so the uploaded-overlap count stays correct.
            if len(images) > 5000:
                uploaded_imgs = [img for img in images if img['uploaded']]
                other_imgs = [img for img in images if not img['uploaded']]
                images = uploaded_imgs + other_imgs[:max(0, 5000 - len(uploaded_imgs))]

            payload = json.dumps({
                'images': images,
                'collections': list(effective_collections),
            }, separators=(',', ':'))
            try:
                if wait_for_publish:
                    self._publish_and_wait(self.mqtt_slideshow_available_topic, payload, qos=1, retain=True)
                else:
                    self._mqtt.publish(self.mqtt_slideshow_available_topic, payload, qos=1, retain=True)
            except Exception:
                self._mqtt.publish(self.mqtt_slideshow_available_topic, payload, qos=0, retain=True)
            avail_paths = {img['path'] for img in images}
            uploaded_overlap = uploaded_paths & avail_paths
            self.log.debug('Published slideshow available: %d images across %d collections (uploaded overlap: %d/%d)',
                          len(images), len(selected_set), len(uploaded_overlap), len(uploaded_paths))
        except Exception as e:
            self.log.warning('MQTT slideshow available publish failed: %s', e)

    async def _apply_slideshow_override(
        self,
        paths,
        req_id=None,
        new_collections=None,
        max_uploads=None,
        force_reupload=False,
        ack_cmd='slideshow/override/set',
    ):
        """Apply a specific image selection to the TV.
        Normal applies retain matching cached uploads and change only missing,
        modified, re-matted, or deselected uploader-managed images. Forced
        applies delete all TV user uploads and upload every path.
        When max_uploads is provided, persists the new limit to overrides.env.
        """
        def ack(status, msg):
            self._publish_ack(ack_cmd, status, msg, req_id)

        async with self._tv_state_lock:
            if self._refresh_in_progress or self._collections_sync_running:
                ack('error', 'Another upload or refresh is already running')
                return
            self._refresh_in_progress = True
        work_started = True
        try:
            missing_local = [
                path for path in paths
                if not os.path.isfile(os.path.join(self.media_root, path))
            ]
            if missing_local:
                raise RuntimeError(
                    f'{len(missing_local)} selected image(s) no longer exist locally'
                )

            if new_collections is not None:
                # Commit the collection selection without triggering a separate full reseed.
                # Also sync self.folder so apply_selection() sees no pending change.
                self.selected_collections = self.bing_daily.normalize_collections(
                    new_collections
                )
                self._pending_selection_change = False
                desired = self.get_selected_folder()
                if os.path.isdir(desired):
                    self.folder = desired
                self._publish_selected_collections_state()
                self._cache_selected_collections()
                self.log.info('Collections committed atomically with override: %s', new_collections)

            # Optionally update max_uploads (user manually exceeded the limit)
            if (
                max_uploads is not None
                and isinstance(max_uploads, int)
                and max_uploads > 0
                and max_uploads != self.max_uploads
            ):
                self.max_uploads = max_uploads
                os.environ['SAMSUNG_TV_ART_MAX_UPLOADS'] = str(max_uploads)
                self._write_overrides({'SAMSUNG_TV_ART_MAX_UPLOADS': str(max_uploads)})
                self.log.info('max_uploads updated to %d', max_uploads)

            # Persist intent before contacting the TV. If the TV is off, this pending
            # override is retried automatically when Art Mode becomes available.
            self.slideshow_override = list(paths)
            self.slideshow_override_pending = True
            self.slideshow_override_force_reupload = force_reupload
            self.shown_content_ids = set()
            self._last_slideshow_paths = set(paths)
            self._save_slideshow_override()
            self._publish_slideshow_state()

            if self.tv is None or not await self.safe_in_artmode(allow_during_refresh=True):
                self.log.info(
                    'Saved slideshow override with %d image(s); TV upload is pending',
                    len(paths),
                )
                ack('ok', f'Saved {len(paths)} image(s); upload will start when the TV enters Art Mode')
                return

            live_entries = await self.get_tv_content_entries('MY-C0002')
            if live_entries is None:
                raise RuntimeError('Unable to verify existing TV uploads')
            live_by_id = {
                entry.get('content_id'): entry
                for entry in live_entries
                if isinstance(entry, dict) and entry.get('content_id')
            }
            selection_applied = False

            if force_reupload:
                self._publish_slideshow_state()
                ack('progress', 'Preparing TV for full re-upload...')
                old_content_ids = set(live_by_id)
                previous_content_by_path = {
                    record.get('path_rel', key): record.get('content_id')
                    for key, record in self.uploaded_files.items()
                }
                ack('progress', f'Uploading {len(paths)} replacement image(s)...')
                uploaded_count = await self.upload_files(paths)
                if uploaded_count != len(paths):
                    current_content_by_path = {
                        record.get('path_rel', key): record.get('content_id')
                        for key, record in self.uploaded_files.items()
                    }
                    replaced_old_ids = {
                        previous_content_by_path.get(path)
                        for path in paths
                        if (
                            current_content_by_path.get(path)
                            and current_content_by_path.get(path)
                            != previous_content_by_path.get(path)
                        )
                    }
                    self._queue_pending_delete_ids(replaced_old_ids)
                    raise RuntimeError(
                        f'Only {uploaded_count} of {len(paths)} replacement image(s) uploaded'
                    )
                new_content_ids = {
                    record.get('content_id')
                    for key, record in self.uploaded_files.items()
                    if (record.get('path_rel') or key) in set(paths)
                    and record.get('content_id') not in old_content_ids
                }
                if len(new_content_ids) != len(paths):
                    raise RuntimeError('Unable to verify all replacement uploads')
                for key, record in list(self.uploaded_files.items()):
                    if record.get('content_id') in old_content_ids:
                        self.uploaded_files.pop(key, None)
                self.write_program_data()
                self._queue_pending_delete_ids(old_content_ids)
                await self._select_replacement(new_content_ids)
                selection_applied = True
                delete_ids = old_content_ids - new_content_ids
                if delete_ids:
                    ack('progress', f'Removing {len(delete_ids)} replaced image(s)...')
                await self._drain_pending_delete_ids()
                self.slideshow_override_force_reupload = False
                self._save_slideshow_override()
                to_upload = []
            else:
                desired_paths = set(paths)
                cached_by_path = {
                    record.get('path_rel', key): (key, record)
                    for key, record in self.uploaded_files.items()
                }
                to_upload = self._slideshow_paths_requiring_upload(paths)
                to_upload_set = set(to_upload)
                delete_records = []
                stale_records = {}
                cache_metadata_changed = False

                for path, (key, record) in cached_by_path.items():
                    content_id = record.get('content_id')
                    if not await self._cached_upload_matches_tv(
                        path,
                        record,
                        live_by_id,
                    ):
                        stale_records[key] = record
                        if path in desired_paths and path not in to_upload_set:
                            to_upload.append(path)
                            to_upload_set.add(path)
                    elif path not in desired_paths or path in to_upload_set:
                        delete_records.append((key, path, content_id))
                    elif not record.get('sha256'):
                        signature = self._get_file_signature(
                            os.path.join(self.media_root, path)
                        )
                        if signature:
                            record.update(signature)
                            try:
                                record['modified'] = self.get_last_updated(
                                    os.path.basename(path),
                                    os.path.join(self.media_root, path),
                                )
                            except OSError:
                                pass
                            cache_metadata_changed = True

                if delete_records or to_upload:
                    self._publish_slideshow_state()

                delete_ids = {content_id for _, _, content_id in delete_records}
                if to_upload:
                    ack('progress', f'Uploading {len(to_upload)} missing or changed image(s)...')
                    await self.upload_files(to_upload)
                    await asyncio.sleep(2)
                    remaining = self._slideshow_paths_requiring_upload(paths)
                    if remaining:
                        remaining_paths = set(remaining)
                        replaced_old_ids = {
                            content_id
                            for _, path, content_id in delete_records
                            if path not in desired_paths or path not in remaining_paths
                        }
                        self._queue_pending_delete_ids(replaced_old_ids)
                        raise RuntimeError(
                            f'{len(remaining)} replacement image(s) failed to upload'
                        )
                    to_upload = []

                if delete_records:
                    ack('progress', f'Removing {len(delete_records)} changed or unselected image(s)...')
                    self._queue_pending_delete_ids(delete_ids)
                    for key, path, content_id in delete_records:
                        current_record = self.uploaded_files.get(key)
                        if (
                            current_record
                            and current_record.get('content_id') == content_id
                        ):
                            self.uploaded_files.pop(key, None)
                        self.log.info('Removing replaced TV upload: %s (%s)', path, content_id)
                    for key, stale_record in stale_records.items():
                        current_record = self.uploaded_files.get(key)
                        if (
                            current_record is stale_record
                            or (
                                current_record
                                and current_record.get('content_id')
                                == stale_record.get('content_id')
                            )
                        ):
                            self.uploaded_files.pop(key, None)
                    self.write_program_data()
                    desired_content_ids = {
                        record.get('content_id')
                        for key, record in self.uploaded_files.items()
                        if (record.get('path_rel') or key) in desired_paths
                        and record.get('content_id') not in delete_ids
                    }
                    await self._select_replacement(desired_content_ids)
                    selection_applied = True
                    await self._drain_pending_delete_ids()
                elif stale_records or cache_metadata_changed:
                    for key, stale_record in stale_records.items():
                        current_record = self.uploaded_files.get(key)
                        if (
                            current_record is stale_record
                            or (
                                current_record
                                and current_record.get('content_id')
                                == stale_record.get('content_id')
                            )
                        ):
                            self.uploaded_files.pop(key, None)
                    self.write_program_data()

            if to_upload:
                ack('progress', f'Uploading {len(to_upload)} missing or changed image(s)...')
                await self.upload_files(to_upload)
                await asyncio.sleep(2)

            remaining = self._slideshow_paths_requiring_upload(paths)
            if remaining:
                should_retry = not await self.safe_in_artmode(allow_during_refresh=True)
                self.slideshow_override_pending = should_retry
                if not should_retry and not force_reupload:
                    self.slideshow_override_force_reupload = False
                self._save_slideshow_override()
                self._publish_slideshow_state()
                if self.get_content_ids():
                    await self.change_art()
                    self.start = time.time()
                    self.write_program_data()
                ack(
                    'error',
                    (
                        f'Upload paused — {len(remaining)} image(s) remain and will resume in Art Mode'
                        if should_retry
                        else f'Upload incomplete — {len(remaining)} image(s) failed; use Apply to retry'
                    ),
                )
                return

            self.slideshow_override_pending = False
            self.slideshow_override_force_reupload = False
            self._save_slideshow_override()
            self._publish_slideshow_state()
            if not selection_applied:
                await self.change_art()
            await self._drain_pending_delete_ids()
            if work_started:
                self.start = time.time()
                self.write_program_data()
            ack('ok', f'Applied {len(paths)} image(s)')
        except Exception as e:
            self.log.warning('Error applying selection: %s', e)
            should_retry = self.tv is not None and not await self.safe_in_artmode(allow_during_refresh=True)
            self.slideshow_override_pending = should_retry
            if not should_retry and not force_reupload:
                self.slideshow_override_force_reupload = False
            self._save_slideshow_override()
            ack('error', str(e))
        finally:
            if work_started:
                self._refresh_in_progress = False
                self._publish_slideshow_state()
                self._publish_slideshow_available()
                try:
                    await self._publish_current_artwork_state(force=True)
                except Exception:
                    pass

    async def _upload_single_image(
        self,
        path,
        req_id=None,
        ack_cmd='slideshow/upload/add',
    ):
        """Upload one image and display it, changing nothing else.

        Deliberately leaves slideshow_override, max_uploads, the rotation timer
        and the daily-mode schedule untouched, and never deletes anything that
        is already on the TV. Because the path is not added to the override,
        get_content_ids() keeps it out of the rotation pool, and a later Apply
        or daily sync reclaims it.
        """
        def ack(status, msg):
            self._publish_ack(ack_cmd, status, msg, req_id)

        if not os.path.isfile(os.path.join(self.media_root, path)):
            ack('error', 'Image no longer exists locally')
            return

        async with self._tv_state_lock:
            if self._refresh_in_progress or self._collections_sync_running:
                ack('error', 'Another upload or refresh is already running')
                return
            self._refresh_in_progress = True
        try:
            if self.tv is None or not await self.safe_in_artmode(
                allow_during_refresh=True
            ):
                # Nothing is persisted for this action, so there is no pending
                # intent to resume; the user retries once the TV is available.
                ack('error', 'TV is not in Art Mode; nothing was uploaded')
                return

            live_entries = await self.get_tv_content_entries('MY-C0002')
            if live_entries is None:
                raise RuntimeError('Unable to verify existing TV uploads')
            live_by_id = {
                entry.get('content_id'): entry
                for entry in live_entries
                if isinstance(entry, dict) and entry.get('content_id')
            }

            content_id = await self._reusable_upload_content_id(path, live_by_id)
            if content_id:
                self.log.info(
                    'Upload skipped: %s is already on the TV as %s',
                    path,
                    content_id,
                )
            else:
                ack('progress', 'Uploading image...')
                await self.upload_files([path])
                content_id = self._cached_content_id(path)
                if not content_id:
                    raise RuntimeError('Upload did not complete')
                self.write_program_data()

            if content_id != self.current_content_id:
                await self.tv.select_image(content_id)
                self.current_content_id = content_id
                self.shown_content_ids.add(content_id)
                await self.update_ha_selected_artwork(content_id)
            ack('ok', 'Image uploaded and displayed')
        except Exception as e:
            self.log.warning('Error uploading single image %s: %s', path, e)
            ack('error', str(e))
        finally:
            self._refresh_in_progress = False
            self._publish_slideshow_state()
            self._publish_slideshow_available()
            try:
                await self._publish_current_artwork_state(force=True)
            except Exception:
                pass

    def _cached_content_id(self, path):
        for key, record in self.uploaded_files.items():
            if record.get('path_rel', key) == path:
                return record.get('content_id')
        return None

    async def _reusable_upload_content_id(self, path, live_by_id):
        """Return the content_id to reuse, or None when a fresh upload is due.

        Reuses only when the cache is current for this path, the desired matte
        still matches, and the TV genuinely still holds that content_id.
        """
        if self._slideshow_paths_requiring_upload([path]):
            return None
        for key, record in self.uploaded_files.items():
            if record.get('path_rel', key) != path:
                continue
            if not await self._cached_upload_matches_tv(path, record, live_by_id):
                return None
            return record.get('content_id')
        return None

    def _write_overrides(self, updates: dict) -> bool:
        """Write overrides to /data/overrides.env, merging with existing content."""
        try:
            path = '/data/overrides.env'
            current = {}
            if os.path.isfile(path):
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if not line or line.startswith('#') or '=' not in line:
                                continue
                            k, v = line.split('=', 1)
                            current[k.strip()] = v.strip()
                except Exception:
                    current = {}
            allowed = {
                'SAMSUNG_TV_ART_MAX_UPLOADS',
                'SAMSUNG_TV_ART_UPDATE_MINUTES',
                'SAMSUNG_TV_ART_TV_IP',
                'SAMSUNG_TV_ART_SEQUENTIAL',
                'SAMSUNG_TV_ART_MQTT_HOST',
                'SAMSUNG_TV_ART_MQTT_WS_HOST',
                'SAMSUNG_TV_ART_MQTT_PORT',
                'SAMSUNG_TV_ART_MQTT_WS_PORT',
                'SAMSUNG_TV_ART_MQTT_USERNAME',
                'SAMSUNG_TV_ART_MQTT_PASSWORD',
            }
            for k, v in updates.items():
                if k in allowed:
                    current[k] = str(v)
            os.makedirs('/data', exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                for k in sorted(current.keys()):
                    f.write(f"{k}={current[k]}\n")
            return True
        except Exception as e:
            self.log.warning('Failed to write overrides: %s', e)
            return False

    def _publish_collections_state(self):
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            opts = []
            has_label_col = self._csv_headers and (
                'artist_name' in self._csv_headers or 'collection_name' in self._csv_headers
            )
            if self.collections_from_csv and has_label_col and self._csv_headers and 'artwork_dir' in self._csv_headers:
                try:
                    # Build collection label options; prefer collection_name over artist_name when present
                    pairs = set()
                    csv_dirs = set()
                    for row in self._csv_by_file.values():
                        an = (row.get('artist_name') or '').strip()
                        cn = (row.get('collection_name') or '').strip()
                        dn = (row.get('artwork_dir') or '').strip()
                        label = cn if cn else an
                        if label and dn:
                            if not os.path.isdir(os.path.join(self.media_root, dn)):
                                self.log.debug(
                                    'Collections label "%s": artwork_dir "%s" not found under media_root; '
                                    'including in options anyway (dir may not be synced yet)', label, dn
                                )
                            pairs.add(label.replace('_', ' '))
                            csv_dirs.add(dn)
                    # Always merge in any on-disk folders not covered by the CSV so
                    # collections without a CSV entry still appear in the dropdown.
                    # Use only the basename (leaf) as the display label so subdir collections
                    # like "Artists/Kelly_Burns" show as "Kelly Burns", not the full path.
                    for d in self._scan_collections():
                        if d not in csv_dirs:
                            pairs.add(os.path.basename(d).replace('_', ' '))
                    opts = sorted(pairs)
                    if not opts:
                        # Fallback to folders if CSV produced nothing usable
                        self.log.info(
                            'No usable labels found in CSV (rows=%d, headers=%s); falling back to folder scan',
                            len(self._csv_by_file), self._csv_headers
                        )
                        opts = self._scan_collections()
                    else:
                        self.log.info('Publishing %d collections from CSV+scan', len(opts))
                        self.log.debug('Publishing collections from CSV+scan: %s', opts)
                except Exception as e:
                    self.log.warning('Failed to derive collection label options from CSV; falling back to folders: %s', e)
                    opts = self._scan_collections()
            else:
                if self.collections_from_csv and self._csv_headers:
                    self.log.info(
                        'collections_from_csv enabled but CSV has no artist_name or collection_name column '
                        '(headers: %s); falling back to folder scan', self._csv_headers
                    )
                opts = self._scan_collections()
            opts = self.bing_daily.order_collection_options(opts)
            # State: human-friendly count
            try:
                self._publish_and_wait(self.mqtt_collections_state_topic, str(len(opts)), qos=1, retain=True)
            except Exception:
                self._mqtt.publish(self.mqtt_collections_state_topic, str(len(opts)), qos=0, retain=True)
            # Attrs: provide options list
            attrs = {"options": opts}
            try:
                self._publish_and_wait(self.mqtt_collections_attr_topic, json.dumps(attrs, separators=(",", ":")), qos=1, retain=True)
            except Exception:
                self._mqtt.publish(self.mqtt_collections_attr_topic, json.dumps(attrs, separators=(",", ":")), qos=0, retain=True)
        except Exception as e:
            self.log.warning('MQTT collections state publish failed: %s', e)

    def _maybe_reload_csv_and_publish_collections(self):
        try:
            # Throttle checks
            if self.csv_check_interval <= 0:
                return
            now = time.time()
            if now - self._csv_last_check < self.csv_check_interval:
                return
            self._csv_last_check = now
            if not self.csv_path or not os.path.isfile(self.csv_path):
                return
            current_mtime = None
            try:
                current_mtime = os.path.getmtime(self.csv_path)
            except Exception:
                return
            if self._csv_mtime is None or current_mtime != self._csv_mtime:
                self.log.info('Detected CSV change; reloading metadata and refreshing collections')
                self._load_csv_metadata()
                self._publish_collections_state()
        except Exception as e:
            self.log.debug('CSV reload check skipped due to error: %s', e)

    def _publish_selected_collections_state(self, wait_for_publish=True):
        # Mirror selected collections back to the shared state topic
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            # Publish labels (artist_name) when possible so UI shows friendly names
            labels = []
            try:
                rev = getattr(self, '_dir_to_artist', {})
                for d in self.selected_collections:
                    label = self.bing_daily.display_collection(rev.get(d, d))
                    labels.append(label.replace('_', ' '))
            except Exception:
                labels = [str(x).replace('_', ' ') for x in self.selected_collections]
            # 1) Keep shared selection topic as CSV for Web UI / command flow compatibility
            value = ", ".join(labels)
            try:
                if wait_for_publish:
                    self._publish_and_wait(self.selection_mqtt_topic, value, qos=1, retain=True)
                else:
                    self._mqtt.publish(self.selection_mqtt_topic, value, qos=1, retain=True)
            except Exception:
                self._mqtt.publish(self.selection_mqtt_topic, value, qos=1, retain=True)
            # 2) Publish HA-safe selected collections sensor state/attributes
            if len(labels) == 1:
                state_value = labels[0]
            elif len(labels) == 0:
                state_value = "none"
            else:
                state_value = f"{len(labels)} selected"
            self._mqtt.publish(self.mqtt_selected_collections_state_topic, state_value, qos=1, retain=True)
            attrs = {
                "selected_collections": list(self.selected_collections),
                "selected_labels": labels,
                "selected_csv": value,
            }
            self._mqtt.publish(self.mqtt_selected_collections_attr_topic, json.dumps(attrs, separators=(",", ":")), qos=1, retain=True)
        except Exception as e:
            self.log.warning('Failed to publish selected collections state: %s', e)

    def _publish_ack(
        self,
        cmd,
        status='ok',
        message='',
        req_id=None,
        extra=None,
        qos=0,
    ):
        if not self.mqtt_enabled or not self._mqtt:
            return
        try:
            ack = {"cmd": cmd, "status": status}
            if message:
                ack["message"] = message
            if req_id is not None:
                ack["req_id"] = req_id
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in ack:
                        ack[k] = v
            ack["selected_collections"] = self.selected_collections
            self._mqtt.publish(
                f"{self.mqtt_ack_prefix}/{cmd}",
                json.dumps(ack, separators=(",", ":")),
                qos=qos,
                retain=False,
            )
        except Exception:
            pass

    def _publish_and_wait(self, topic: str, payload: str, qos: int = 1, retain: bool = False) -> bool:
        """Publish with QoS and wait for completion to avoid silent drops."""
        try:
            if not self._mqtt:
                self.log.warning('MQTT publish skipped (client not initialised): %s', topic)
                return False
            info = self._mqtt.publish(topic, payload, qos=qos, retain=retain)
            try:
                # Wait briefly for publish; don't block forever
                info.wait_for_publish(timeout=5.0)
            except Exception:
                pass
            rc = getattr(info, 'rc', 0)
            mid = getattr(info, 'mid', None)
            if rc != 0:
                self.log.warning('MQTT publish failed rc=%s topic=%s', rc, topic)
                return False
            self.log.debug('MQTT publish ok mid=%s topic=%s retain=%s qos=%s', mid, topic, retain, qos)
            return True
        except Exception as e:
            self.log.warning('MQTT publish exception for %s: %s', topic, e)
            return False

    def _handle_mqtt_command(self, subtopic, payload_raw):
        # subtopic examples: 'collections/set', 'collections/add', 'collections/remove', 'collections/clear', 'collections/refresh', 'artwork/set'
        cmd = subtopic.strip()
        req_id = None
        try:
            payload = payload_raw.strip() if isinstance(payload_raw, str) else str(payload_raw)
            data = None
            if payload and payload.startswith('{') and payload.endswith('}'):
                try:
                    data = json.loads(payload)
                except Exception:
                    data = None
            if data and isinstance(data, dict):
                req_id = data.get('req_id')
        except Exception:
            payload = ''
            data = None

        try:
            if cmd == 'collections/set':
                cols = []
                if data and 'collections' in data and isinstance(data['collections'], list):
                    cols = [str(c).strip() for c in data['collections'] if str(c).strip()]
                elif payload:
                    cols = [c.strip() for c in payload.split(',') if c.strip()]
                ui_only = bool(data and data.get('ui_only'))
                if ui_only:
                    self._ignore_retained_selection_until_reconnect = True

                    def apply_ui_collection_filter():
                        started = time.monotonic()
                        try:
                            mapped = []
                            for collection in cols:
                                resolved = self._map_to_artwork_dir(collection)
                                if resolved and resolved not in mapped:
                                    mapped.append(resolved)
                            self.selected_collections = self.bing_daily.normalize_collections(mapped)
                            self._pending_selection_change = False
                            desired = self.get_selected_folder()
                            if os.path.isdir(desired):
                                self.folder = desired
                            self._publish_ack(
                                'collections/set',
                                'ok',
                                'Collection filter updated; TV unchanged',
                                req_id,
                            )
                            self._publish_selected_collections_state(wait_for_publish=False)
                            self._cache_selected_collections()
                            self._publish_slideshow_available(wait_for_publish=False)
                        except Exception as e:
                            self.log.exception(
                                'Collection filter worker failed: req_id=%s elapsed=%.3fs',
                                req_id,
                                time.monotonic() - started,
                            )
                            self._publish_ack('collections/set', 'error', str(e), req_id)

                    worker = threading.Thread(
                        target=apply_ui_collection_filter,
                        name='collection-filter-worker',
                        daemon=True,
                    )
                    worker.start()
                    return

                # Map incoming values to artwork_dir folder names when possible
                mapped = []
                for c in cols:
                    mc = self._map_to_artwork_dir(c)
                    if mc and mc not in mapped:
                        mapped.append(mc)
                self.selected_collections = self.bing_daily.normalize_collections(mapped)
                self._pending_selection_change = True
                # Automatic callers retain the existing reseed behavior.
                self._publish_selected_collections_state(wait_for_publish=False)
                self._cache_selected_collections()
                self._publish_ack('collections/set', 'ok', 'Collections set', req_id)
                return
            if cmd == 'collections/add':
                col = None
                if data and 'collection' in data:
                    col = str(data['collection']).strip()
                elif payload:
                    col = payload.strip()
                if col:
                    mc = self._map_to_artwork_dir(col) or col
                    self.selected_collections = self.bing_daily.add_collection(
                        self.selected_collections,
                        mc,
                    )
                    self._pending_selection_change = True
                    self._publish_selected_collections_state()
                    self._cache_selected_collections()
                    self._publish_ack('collections/add', 'ok', f'Added {mc}', req_id)
                else:
                    self._publish_ack('collections/add', 'error', 'No collection provided', req_id)
                return
            if cmd == 'collections/remove':
                col = None
                if data and 'collection' in data:
                    col = str(data['collection']).strip()
                elif payload:
                    col = payload.strip()
                if col:
                    mc = self._map_to_artwork_dir(col) or col
                    self.selected_collections = [c for c in self.selected_collections if c != mc]
                    self._pending_selection_change = True
                    self._publish_selected_collections_state()
                    self._cache_selected_collections()
                    self._publish_ack('collections/remove', 'ok', f'Removed {mc}', req_id)
                else:
                    self._publish_ack('collections/remove', 'error', 'No collection provided', req_id)
                return
            if cmd == 'collections/clear':
                self.selected_collections = []
                self._pending_selection_change = True
                self._publish_selected_collections_state()
                self._cache_selected_collections()
                self._publish_ack('collections/clear', 'ok', 'Cleared collections', req_id)
                return
            if cmd == 'collections/refresh':
                # Reshuffle uploads for current selection without changing selections
                fut = self._schedule_command_coro(self._do_collections_refresh(req_id=req_id), 'collections/refresh')
                if not fut:
                    self._publish_ack('collections/refresh', 'error', 'Failed to queue refresh task', req_id)
                    return
                # Also refresh discovery/state for UI consistency
                self._publish_collections_discovery()
                self._publish_collections_state()
                self._publish_ack('collections/refresh', 'queued', 'Collections refresh queued', req_id)
                return
            if cmd == 'settings/refresh':
                self._publish_settings_discovery()
                self._publish_settings_state()
                self._publish_ack('settings/refresh', 'ok', 'Settings refreshed', req_id)
                return
            if cmd == 'settings/sync_collections':
                fut = self._schedule_command_coro(self._do_sync_collections(req_id=req_id), 'settings/sync_collections')
                if not fut:
                    self._publish_ack('collections/refresh', 'error', 'Failed to queue update & refresh', req_id)
                    return
                self._publish_ack('collections/refresh', 'queued', 'Update & refresh queued', req_id)
                return
            if cmd == 'settings/pull_collections':
                fut = self._schedule_command_coro(self._do_pull_collections(req_id=req_id), 'settings/pull_collections')
                if not fut:
                    self._publish_ack('settings/pull_collections', 'error', 'Failed to queue collection pull', req_id)
                    return
                self._publish_ack('settings/pull_collections', 'queued', 'Fetching list...', req_id)
                return
            if cmd == 'artwork/set':
                # Accept either { "path": "Collection/file.jpg" } or a plain string payload
                path = None
                if data and 'path' in data:
                    path = str(data['path']).strip()
                elif payload:
                    path = payload.strip()
                if not path:
                    self._publish_ack('artwork/set', 'error', 'No path provided', req_id)
                    return
                # Normalize to relative path from media_root
                rel_path = path
                if os.path.isabs(path):
                    try:
                        rel_path = os.path.relpath(path, self.media_root)
                    except Exception:
                        pass
                full_path = os.path.join(self.media_root, rel_path)
                if not os.path.isfile(full_path):
                    self._publish_ack('artwork/set', 'error', f'File not found: {rel_path}', req_id)
                    return
                # Upload (if needed) and select immediately
                awaitable = self.upload_files([rel_path])
                # Ensure the coroutine is executed in loop-safe way
                fut = self._schedule_command_coro(self._post_upload_select(awaitable, rel_path, req_id), 'artwork/set')
                if not fut:
                    self._publish_ack('artwork/set', 'error', 'Failed to queue artwork upload/select', req_id)
                return
            if cmd == 'settings/set':
                if not isinstance(data, dict):
                    self._publish_ack('settings/set', 'error', 'Invalid JSON', None)
                    return
                updates = {}
                apply_runtime = {}
                try:
                    if 'SAMSUNG_TV_ART_MAX_UPLOADS' in data:
                        updates['SAMSUNG_TV_ART_MAX_UPLOADS'] = str(int(data['SAMSUNG_TV_ART_MAX_UPLOADS']))
                    if 'SAMSUNG_TV_ART_UPDATE_MINUTES' in data:
                        minutes = int(float(data['SAMSUNG_TV_ART_UPDATE_MINUTES']))
                        updates['SAMSUNG_TV_ART_UPDATE_MINUTES'] = str(minutes)
                        apply_runtime['UPDATE_SECONDS'] = max(0, minutes * 60)
                    if 'SAMSUNG_TV_ART_TV_IP' in data:
                        updates['SAMSUNG_TV_ART_TV_IP'] = str(data['SAMSUNG_TV_ART_TV_IP']).strip()
                    if 'SAMSUNG_TV_ART_MQTT_HOST' in data:
                        updates['SAMSUNG_TV_ART_MQTT_HOST'] = str(data['SAMSUNG_TV_ART_MQTT_HOST']).strip()
                    if 'SAMSUNG_TV_ART_MQTT_WS_HOST' in data:
                        updates['SAMSUNG_TV_ART_MQTT_WS_HOST'] = str(data['SAMSUNG_TV_ART_MQTT_WS_HOST']).strip()
                    if 'SAMSUNG_TV_ART_MQTT_PORT' in data:
                        updates['SAMSUNG_TV_ART_MQTT_PORT'] = str(int(data['SAMSUNG_TV_ART_MQTT_PORT']))
                    if 'SAMSUNG_TV_ART_MQTT_WS_PORT' in data:
                        updates['SAMSUNG_TV_ART_MQTT_WS_PORT'] = str(int(data['SAMSUNG_TV_ART_MQTT_WS_PORT']))
                    if 'SAMSUNG_TV_ART_MQTT_USERNAME' in data:
                        updates['SAMSUNG_TV_ART_MQTT_USERNAME'] = str(data['SAMSUNG_TV_ART_MQTT_USERNAME']).strip()
                    if 'SAMSUNG_TV_ART_MQTT_PASSWORD' in data:
                        updates['SAMSUNG_TV_ART_MQTT_PASSWORD'] = str(data['SAMSUNG_TV_ART_MQTT_PASSWORD'])
                except Exception:
                    self._publish_ack('settings/set', 'error', 'Validation failed', None)
                    return
                if not updates:
                    self._publish_ack('settings/set', 'error', 'No updates', None)
                    return
                if self._write_overrides(updates):
                    # Update process env for immediate reflect in settings state
                    try:
                        for k, v in updates.items():
                            os.environ[k] = v
                    except Exception:
                        pass
                    # Apply runtime-safe changes without restart
                    try:
                        if 'UPDATE_SECONDS' in apply_runtime:
                            self.update_time = int(apply_runtime['UPDATE_SECONDS'])
                            # Reset slideshow timer so new interval takes effect cleanly
                            self.start = time.time()
                        if 'SAMSUNG_TV_ART_MAX_UPLOADS' in updates:
                            self.max_uploads = int(updates['SAMSUNG_TV_ART_MAX_UPLOADS'])
                            self._publish_slideshow_state()
                    except Exception:
                        pass
                    self._publish_settings_state()
                    # Indicate if restart is recommended (e.g., TV IP or MQTT change)
                    msg = 'Settings updated'
                    try:
                        if 'SAMSUNG_TV_ART_TV_IP' in updates and updates['SAMSUNG_TV_ART_TV_IP'] and updates['SAMSUNG_TV_ART_TV_IP'] != str(self.ip):
                            msg += ' (TV IP change will apply after restart)'
                            self.ip = updates['SAMSUNG_TV_ART_TV_IP']
                        if any(k in updates for k in ('SAMSUNG_TV_ART_MQTT_HOST', 'SAMSUNG_TV_ART_MQTT_PORT', 'SAMSUNG_TV_ART_MQTT_USERNAME', 'SAMSUNG_TV_ART_MQTT_PASSWORD')):
                            msg += ' (MQTT changes will apply after restart)'
                    except Exception:
                        pass
                    self._publish_ack('settings/set', 'ok', msg, None)
                else:
                    self._publish_ack('settings/set', 'error', 'Failed to write overrides', None)
                return
            if cmd == 'settings/restart':
                # Exit to allow container restart policy to restart us
                self._publish_ack('settings/restart', 'ok', 'Restarting', None)
                try:
                    os._exit(0)
                except Exception:
                    pass
                return
            if cmd == 'slideshow/settings/set':
                if not isinstance(data, dict):
                    self._publish_ack('slideshow/settings/set', 'error', 'Invalid JSON', req_id)
                    return
                updates = {}
                try:
                    if 'sequential' in data:
                        self.sequential = bool(data['sequential'])
                        updates['SAMSUNG_TV_ART_SEQUENTIAL'] = '1' if self.sequential else '0'
                    if 'update_minutes' in data:
                        minutes = max(0, int(float(data['update_minutes'])))
                        self.update_time = minutes * 60
                        self.start = time.time()  # Reset slideshow timer
                        updates['SAMSUNG_TV_ART_UPDATE_MINUTES'] = str(minutes)
                except Exception:
                    self._publish_ack('slideshow/settings/set', 'error', 'Validation failed', req_id)
                    return
                self._write_overrides(updates)
                self._publish_slideshow_state()
                self._publish_settings_state()
                self._publish_ack('slideshow/settings/set', 'ok', 'Slideshow settings updated', req_id)
                return
            if cmd == 'slideshow/museum_label/generate':
                collections = []
                if data and isinstance(data.get('collections'), list):
                    raw_collections = [
                        str(collection).strip()
                        for collection in data['collections']
                        if str(collection).strip()
                    ]
                    collections = self.bing_daily.normalize_collections([
                        self._map_to_artwork_dir(collection) or collection
                        for collection in raw_collections
                    ])
                if not collections:
                    collections = list(self.selected_collections)
                paths = self.museum_labels.paths_for_collections(collections)
                if not collections or not paths:
                    self._publish_ack(
                        cmd,
                        'error',
                        'The selected collections contain no images',
                        req_id,
                        qos=1,
                    )
                    return

                async def generate_museum_labels():
                    try:
                        self._publish_ack(
                            cmd,
                            'progress',
                            (
                                f'Generating Museum Label images for '
                                f'{len(paths)} file(s)...'
                            ),
                            req_id,
                            qos=1,
                        )
                        def publish_progress(completed, total):
                            self._publish_ack(
                                cmd,
                                'progress',
                                (
                                    f'Generating Museum Label images '
                                    f'({completed}/{total})...'
                                ),
                                req_id,
                                qos=1,
                            )

                        result = await asyncio.to_thread(
                            self.museum_labels.regenerate_paths,
                            paths,
                            getattr(self, '_csv_by_path', {}),
                            self._csv_by_file,
                            publish_progress,
                        )
                        bing_generated = [
                            generated for generated in result['generated']
                            if generated['path'].startswith(
                                f'{BING_COLLECTION_ID}/'
                            )
                        ]
                        self.bing_daily.record_regenerated_derivatives(
                            bing_generated
                        )
                        if hasattr(self, '_collection_file_cache'):
                            self._collection_file_cache.clear()
                        self._publish_slideshow_available()
                        generated_paths = [
                            item['path'] for item in result['generated']
                        ]
                        if result['errors']:
                            self._publish_ack(
                                cmd,
                                'error',
                                (
                                    f"Generated {len(generated_paths)} label(s); "
                                    f"{len(result['errors'])} failed"
                                ),
                                req_id,
                                extra={
                                    'paths': generated_paths,
                                    'errors': result['errors'],
                                },
                                qos=1,
                            )
                        elif not generated_paths:
                            self._publish_ack(
                                cmd,
                                'error',
                                'No selected images belong to downloaded collections',
                                req_id,
                                qos=1,
                            )
                        else:
                            message = (
                                f'Generated {len(generated_paths)} '
                                'Museum Label image(s); TV was not changed'
                            )
                            self._publish_ack(
                                cmd,
                                'ok',
                                message,
                                req_id,
                                extra={
                                    'paths': generated_paths,
                                    'generated': result['generated'],
                                },
                                qos=1,
                            )
                    except Exception as exc:
                        self.log.warning(
                            'Museum Label generation failed: %s',
                            exc,
                        )
                        self._publish_ack(
                            cmd,
                            'error',
                            str(exc),
                            req_id,
                            qos=1,
                        )

                if not self._schedule_command_coro(
                    generate_museum_labels(),
                    cmd,
                ):
                    self._publish_ack(
                        cmd,
                        'error',
                        'Failed to queue Museum Label generation',
                        req_id,
                    )
                return
            if cmd == 'slideshow/upload/add':
                path = str((data or {}).get('path', '')).strip()
                if not path:
                    self._publish_ack(cmd, 'error', 'No path provided', req_id)
                    return
                fut = self._schedule_command_coro(
                    self._upload_single_image(path, req_id=req_id, ack_cmd=cmd),
                    'slideshow/upload/add',
                )
                if not fut:
                    self._publish_ack(
                        cmd,
                        'error',
                        'Failed to queue the upload',
                        req_id,
                    )
                return
            if cmd in ('slideshow/override/set', 'slideshow/override/reupload'):
                paths = []
                if data and 'paths' in data and isinstance(data['paths'], list):
                    paths = [str(p).strip() for p in data['paths'] if str(p).strip()]
                if not paths:
                    self._publish_ack(cmd, 'error', 'No paths provided', req_id)
                    return
                # Optional 'collections' field: commit collection changes atomically with override.
                new_cols = None
                if data and 'collections' in data and isinstance(data['collections'], list):
                    raw_cols = [str(c).strip() for c in data['collections'] if str(c).strip()]
                    new_cols = [
                        self._map_to_artwork_dir(c) or c
                        for c in raw_cols
                        if (self._map_to_artwork_dir(c) or c)
                    ]
                    new_cols = self.bing_daily.normalize_collections(new_cols)
                # Optional 'max_uploads' field: user manually exceeded the limit.
                new_max = None
                if data and 'max_uploads' in data:
                    try:
                        new_max = int(data['max_uploads'])
                    except (ValueError, TypeError):
                        new_max = None
                daily_collections = (
                    new_cols
                    if new_cols is not None
                    else self.selected_collections
                )
                if self.bing_daily.is_daily_collection_selection(daily_collections):
                    fut = self._schedule_command_coro(
                        self.bing_daily.apply_selection(
                            req_id=req_id,
                            ack_cmd=cmd,
                            force_reupload=cmd == 'slideshow/override/reupload',
                        ),
                        'bing/daily/apply',
                    )
                    if not fut:
                        self._publish_ack(
                            cmd,
                            'error',
                            'Failed to queue Bing Daily Wallpaper update',
                            req_id,
                        )
                    return
                force_reupload = cmd == 'slideshow/override/reupload'
                fut = self._schedule_command_coro(
                    self._apply_slideshow_override(
                        paths,
                        req_id,
                        new_collections=new_cols,
                        max_uploads=new_max,
                        force_reupload=force_reupload,
                        ack_cmd=cmd,
                    ),
                    cmd,
                )
                if not fut:
                    self._publish_ack(cmd, 'error', 'Failed to queue slideshow update', req_id)
                return
            if cmd == 'slideshow/presets/set':
                # Clients publish their full presets array here; backend persists it
                # to disk and re-publishes as a retained message so all other clients
                # receive it on subscribe and so it survives container recreation.
                try:
                    presets = json.loads(payload_raw) if payload_raw else []
                    if not isinstance(presets, list):
                        presets = []
                    self._save_slideshow_presets(presets)
                    self._mqtt.publish(
                        self.mqtt_slideshow_presets_topic,
                        json.dumps(presets),
                        qos=1,
                        retain=True,
                    )
                    self._publish_ack('slideshow/presets/set', 'ok', 'Presets saved', req_id)
                except Exception as e:
                    self._publish_ack('slideshow/presets/set', 'error', str(e), req_id)
                return
            if cmd == 'slideshow/presets/generate':
                # Offload to the event loop (and a thread executor for the CPU/IO
                # work) so we never block paho's network loop thread — a long
                # callback here can starve the MQTT keepalive and trigger a
                # broker disconnect ([Errno 32] Broken pipe).
                async def _do_generate_presets():
                    try:
                        loop = asyncio.get_running_loop()
                        generated = await loop.run_in_executor(None, self._generate_default_presets)
                        if not generated:
                            self._publish_ack('slideshow/presets/generate', 'error', 'No images found', req_id)
                            return
                        self._save_slideshow_presets(generated)
                        self._mqtt.publish(
                            self.mqtt_slideshow_presets_topic,
                            json.dumps(generated),
                            qos=1,
                            retain=True,
                        )
                        self._publish_ack('slideshow/presets/generate', 'ok',
                                          f'{len(generated)} preset(s) generated', req_id)
                    except Exception as e:
                        self._publish_ack('slideshow/presets/generate', 'error', str(e), req_id)
                fut = self._schedule_command_coro(_do_generate_presets(), 'slideshow/presets/generate')
                if not fut:
                    self._publish_ack('slideshow/presets/generate', 'error', 'Failed to queue preset generation', req_id)
                return
            if cmd == 'slideshow/override/clear':
                self.slideshow_override = None
                self.slideshow_override_pending = False
                self.slideshow_override_force_reupload = False
                self._save_slideshow_override()
                self.shown_content_ids = set()
                self._publish_slideshow_state()
                self._publish_ack('slideshow/override/clear', 'ok', 'Slideshow override cleared; auto mode restored', req_id)
                return
            if cmd == 'slideshow/matte/set':
                # Save the per-image matte override silently. We do NOT call
                # change_matte here — on this firmware that's a no-op for the
                # rendered image (the matte is baked at upload time). The main
                # slideshow Apply workflow later compares this desired value
                # with the upload cache and reseeds when they differ.
                try:
                    if not isinstance(data, dict):
                        self._publish_ack('slideshow/matte/set', 'error', 'Invalid JSON', req_id)
                        return
                    path = str(data.get('path', '')).strip()
                    matte = str(data.get('matte', '')).strip() or 'none'
                    if not path:
                        self._publish_ack('slideshow/matte/set', 'error', 'Missing path', req_id)
                        return
                    if matte in ('', 'default', '__default__'):
                        self._matte_overrides.pop(path, None)
                        effective = self._resolve_matte_for(path, os.path.basename(path))
                    else:
                        self._matte_overrides[path] = matte
                        effective = matte
                    self._save_matte_overrides()
                    self._publish_matte_overrides()
                    self._publish_slideshow_state()
                    self._publish_ack('slideshow/matte/set', 'ok',
                                      f'Matte saved ({effective}). Use slideshow Apply to update the TV.',
                                      req_id, extra={'path': path, 'matte': effective})
                except Exception as e:
                    self._publish_ack('slideshow/matte/set', 'error', str(e), req_id)
                return
            if cmd == 'slideshow/matte/apply':
                # Make a previously-saved matte override visible on the TV by
                # deleting the image and re-uploading it with the new matte
                # baked into the send_image request. This is the only path
                # that produces a visible matte change on this firmware.
                try:
                    if not isinstance(data, dict):
                        self._publish_ack('slideshow/matte/apply', 'error', 'Invalid JSON', req_id)
                        return
                    path = str(data.get('path', '')).strip()
                    if not path:
                        self._publish_ack('slideshow/matte/apply', 'error', 'Missing path', req_id)
                        return
                    async def _apply_reupload():
                        async with self._tv_state_lock:
                            if self._refresh_in_progress or self._collections_sync_running:
                                self._publish_ack(
                                    'slideshow/matte/apply',
                                    'error',
                                    'Another upload or refresh is already running',
                                    req_id,
                                    extra={'path': path},
                                )
                                return
                            self._refresh_in_progress = True
                        self._publish_slideshow_state()
                        try:
                            new_id, effective = await self._apply_matte_via_reupload(path)
                            self._publish_ack(
                                'slideshow/matte/apply', 'ok',
                                f'Matte {effective} applied',
                                req_id, extra={'path': path, 'matte': effective, 'content_id': new_id},
                            )
                        except _MatteRejectedError as ex:
                            self._publish_ack(
                                'slideshow/matte/apply', 'error',
                                f'TV rejected matte "{ex.matte}" for this image; pinned to none.',
                                req_id, extra={'path': path, 'matte': 'none'},
                            )
                        except Exception as ex:
                            self.log.warning('matte/apply failed for %s: %s', path, ex)
                            self._publish_ack('slideshow/matte/apply', 'error', str(ex), req_id, extra={'path': path})
                        finally:
                            self._refresh_in_progress = False
                            self._publish_slideshow_state()
                    self._schedule_command_coro(_apply_reupload(), 'slideshow/matte/apply')
                except Exception as e:
                    self._publish_ack('slideshow/matte/apply', 'error', str(e), req_id)
                return
            if cmd == 'slideshow/matte_options/request':
                fut = self._schedule_command_coro(self._publish_matte_options(), 'slideshow/matte_options/request')
                if fut:
                    self._publish_ack('slideshow/matte_options/request', 'ok', 'Matte options publishing', req_id)
                else:
                    self._publish_ack('slideshow/matte_options/request', 'error', 'Failed to schedule', req_id)
                return
            if cmd == 'slideshow/available/request':
                # Optional 'collections' field allows the UI to preview a staged
                # collection set in the grid without committing selected_collections.
                preview_cols = None
                if data and 'collections' in data and isinstance(data['collections'], list):
                    raw_cols = [str(c).strip() for c in data['collections'] if str(c).strip()]
                    preview_cols = self.bing_daily.normalize_collections(
                        [self._map_to_artwork_dir(c) or c for c in raw_cols]
                    )
                def publish_available():
                    self._publish_slideshow_available(
                        override_collections=preview_cols,
                        wait_for_publish=False,
                    )
                    self._publish_ack(
                        'slideshow/available/request',
                        'ok',
                        'Available list published',
                        req_id,
                    )

                worker = threading.Thread(
                    target=publish_available,
                    name='slideshow-available-publisher',
                    daemon=True,
                )
                worker.start()
                return
            if cmd == 'slideshow/preview/request':
                # Compute a random selection dry-run without uploading anything.
                # Optional 'collections' field overrides selected_collections for the preview.
                preview_cols = list(self.selected_collections)
                if data and 'collections' in data and isinstance(data['collections'], list):
                    raw_cols = [str(c).strip() for c in data['collections'] if str(c).strip()]
                    preview_cols = self.bing_daily.normalize_collections(
                        [self._map_to_artwork_dir(c) or c for c in raw_cols]
                    )
                max_n = self.max_uploads
                paths = self._preview_random_selection(preview_cols, max_n)
                try:
                    ack = {
                        'cmd': cmd,
                        'status': 'ok',
                        'message': f'Preview generated: {len(paths)} image(s)',
                        'paths': paths,
                    }
                    if req_id is not None:
                        ack['req_id'] = req_id
                    ack['selected_collections'] = self.selected_collections
                    self._mqtt.publish(
                        f'{self.mqtt_ack_prefix}/{cmd}',
                        json.dumps(ack, separators=(',', ':')),
                        qos=0,
                        retain=False,
                    )
                except Exception as e:
                    self._publish_ack(cmd, 'error', str(e), req_id)
                return
            # Unknown command
            self._publish_ack(cmd, 'error', 'Unknown command', req_id)
        except Exception as e:
            self._publish_ack(cmd, 'error', f'Exception: {e}', req_id)

    async def _update_local_collections(self, ack_cmd, req_id):
        """Fetch configured repositories, rebuild metadata, and publish collection state."""
        try:
            proc = await asyncio.create_subprocess_exec(
                '/bin/sh', '-c', 'chmod +x /app/scripts/fetch_collections.sh 2>/dev/null || true; /app/scripts/fetch_collections.sh',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            output_tail = []
            fetch_errors = []
            async for raw_line in proc.stdout:
                line = raw_line.decode('utf-8', errors='replace').strip()
                if line:
                    output_tail.append(line)
                    output_tail = output_tail[-10:]
                    if line.startswith(('Warning:', 'Clone failed')):
                        fetch_errors.append(line)
                match = re.match(r'^(?:Updating|Cloning) collection (.+?) from ', line)
                if match:
                    name = match.group(1).replace('_', ' ')
                    self._publish_ack(ack_cmd, 'progress', f'Fetching list {name}...', req_id)
            await proc.wait()
            if proc.returncode != 0 or fetch_errors:
                self.log.warning(
                    'On-demand collections fetch failed rc=%s output=%s',
                    proc.returncode,
                    '\n'.join(output_tail)[:1000],
                )
                self._publish_ack(ack_cmd, 'error', 'Git fetch failed — check container logs', req_id)
                return False
        except Exception as e:
            self.log.warning('On-demand collections fetch exception: %s', e)
            self._publish_ack(ack_cmd, 'error', 'Git fetch failed — check container logs', req_id)
            return False

        if self.museum_labels.enabled:
            try:
                self._publish_ack(
                    ack_cmd,
                    'progress',
                    'Generating Museum Label images...',
                    req_id,
                )
                await asyncio.to_thread(
                    self.museum_labels.process_git_collections
                )
                if hasattr(self, '_collection_file_cache'):
                    self._collection_file_cache.clear()
            except Exception as e:
                self.log.warning('Museum Label processing failed: %s', e)
                self._publish_ack(
                    ack_cmd,
                    'error',
                    'Museum Label generation failed — check container logs',
                    req_id,
                )
                return False

        try:
            self._publish_ack(ack_cmd, 'progress', 'Rebuilding artwork database...', req_id)
            proc = await asyncio.create_subprocess_exec(
                'python', '-m', 'loop.aggregate_csv', '/app/frame_tv_art_collections', '/app/artwork_data.csv',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _out, err = await proc.communicate()
            if proc.returncode != 0:
                self.log.warning(
                    'On-demand CSV aggregate failed rc=%s err=%s',
                    proc.returncode,
                    (err or b'').decode('utf-8', errors='ignore')[:400],
                )
                self._publish_ack(ack_cmd, 'error', 'CSV rebuild failed — check container logs', req_id)
                return False
        except Exception as e:
            self.log.warning('On-demand CSV aggregate exception: %s', e)
            self._publish_ack(ack_cmd, 'error', 'CSV rebuild failed — check container logs', req_id)
            return False

        self._publish_ack(ack_cmd, 'progress', 'Reloading collection metadata...', req_id)
        self._load_csv_metadata()
        self._publish_collections_state()
        self._publish_selected_collections_state()
        self._publish_settings_state()
        return True

    async def _do_pull_collections(self, req_id=None):
        """Update local collection repositories and metadata without touching the TV."""
        ack_cmd = 'settings/pull_collections'
        if self._collections_sync_running or self._refresh_in_progress:
            self.log.info('%s ignored: collection update already running', ack_cmd)
            self._publish_ack(ack_cmd, 'error', 'Collection update already running', req_id)
            return
        self._collections_sync_running = True
        try:
            self.log.info('%s started (req_id=%s)', ack_cmd, req_id)
            self._publish_ack(ack_cmd, 'started', 'Fetching list...', req_id)
            if await self._update_local_collections(ack_cmd, req_id):
                self._publish_ack(ack_cmd, 'ok', 'Collections updated', req_id)
        except Exception as e:
            self.log.warning('%s exception: %s', ack_cmd, e)
            self._publish_ack(ack_cmd, 'error', f'Exception: {e}', req_id)
        finally:
            self._collections_sync_running = False

    async def _do_sync_collections(self, req_id=None):
        """Fetch git repos → rebuild CSV → reload metadata → reseed TV."""
        ack_cmd = 'collections/refresh'
        if self._collections_sync_running or self._refresh_in_progress:
            self.log.info('settings/sync_collections ignored: already running')
            self._publish_ack(ack_cmd, 'error', 'Update already running', req_id)
            return
        self._collections_sync_running = True
        try:
            self.log.info('settings/sync_collections started (req_id=%s)', req_id)
            self._publish_ack(ack_cmd, 'started', 'Fetching latest collection updates...', req_id)
            if not await self._update_local_collections(ack_cmd, req_id):
                return

            if self.tv is None:
                self.log.info('settings/sync_collections: TV unavailable — skipping reseed, collections updated on disk only')
                self._publish_ack(ack_cmd, 'done', 'Collections updated. TV reseed will happen automatically once the TV is reachable.', req_id)
            else:
                self.log.info('settings/sync_collections proceeding to TV reseed (req_id=%s)', req_id)
                await self._do_full_reseed(req_id=req_id, skip_started_ack=True)
        except Exception as e:
            self.log.warning('settings/sync_collections exception: %s', e)
            self._publish_ack(ack_cmd, 'error', f'Exception: {e}', req_id)
        finally:
            self._collections_sync_running = False

    async def _post_upload_select(self, upload_coro, rel_path, req_id):
        async with self._tv_state_lock:
            if self._refresh_in_progress or self._collections_sync_running:
                upload_coro.close()
                self._publish_ack(
                    'artwork/set',
                    'error',
                    'Another upload or refresh is already running',
                    req_id,
                )
                return
            self._refresh_in_progress = True
        self._publish_slideshow_state()
        try:
            await upload_coro
            # Select the just-uploaded image
            record = self.uploaded_files.get(rel_path)
            content_id = record.get('content_id') if record else None
            if content_id:
                await self.tv.select_image(content_id)
                self.current_content_id = content_id
                await self.update_ha_selected_artwork(content_id)
                self._publish_ack('artwork/set', 'ok', f'Selected {os.path.basename(rel_path)}', req_id)
            else:
                self._publish_ack('artwork/set', 'error', f'Upload failed for {os.path.basename(rel_path)}', req_id)
        except Exception as e:
            self._publish_ack('artwork/set', 'error', f'Exception selecting: {e}', req_id)
        finally:
            self._refresh_in_progress = False
            self._publish_slideshow_state()
