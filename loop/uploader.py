#!/usr/bin/env python3
# fully async example program to monitor a folder and upload/display on Frame TV
# NOTE: install Pillow (pip install Pillow) to automatically syncronize art on TV wth uploaded_files.json.

'''
This program will read the files in a designated folder (with allowed extensions) and upload them to your TV. It keeps track of which files correspond to what
content_id on your TV by saving the data in a file called uploaded_files.json. it also keeps track of when the selected artwork was last changed.

It monitors the folder for changes every check seconds (5 by default), new files are uploaded to the TV, removed files are deleted from the TV, and if a file
is changed, the old content is removed from the TV and the new content uploaded to the TV. Content is only changed if the TV is in art mode.

if check is set to 0 seconds, the program will run once and exit. You can then run it periodically (say with a cron job).

if there is more than one file in the folder, the current artword displayed is changed every update minutes (0) by default (which means do not select any artwork),
otherwise the single file in the folder is selected to be displayed. this also only happens when the TV is in art mode.

If you have PIL installed, the initial syncronization is automatic, the first time the program is run.

If the on (-O) option is selected, the program wil exit if the TV is not on (TV or art mode).
If the sequential (-S) option is selected, then the slideshow is sequential, not random (random is the default)
The default checking period is 60 seconds or the update period whichever is less.

Example:
    1) Your TV is used to display one image, that changes every day, you have a program that grabs the image and puts it in a folder. The image always has the same name.
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path> -c 0
       to update the image on the Tv after the script that grabs the file runs
       If you are unsure if the TV will be on when you run the program
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path> -c 0 -O
       or
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path> -c 60
       and leave it running
       
    2) You use your TV to display your own artwork, you want a slideshow that displays a random artwork every minute, but want to add/remove art from a network share
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path_to_share> -u 1
       and leave it running. Add/remove art from the network share folder to include it/remove it from the slideshow.
       If you want an update every 15 seconds
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path_to_share> -u 0.25
       
    3) you have artwork on the TV marked as "favourites", but want to inclue your own artwork from a folder in a random slideshow that updates once a day
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path> -c 3600 -u 1440 -F
       and leave it running. Add/remove art from the folder to include it/remove it from the slideshow.
       
    4) You have some standard art uploaded to your TV, that you slideshow from the TV, but want to add seasonal artworks to the slideshow that you change from time to time.
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path> -c 3600
       and leave it running. Add/remove art from the folder to include it/remove it from the slideshow.
       or
       run ./async_art_update_from_directory.py <tv_ip> -f <folder_path> -c 0 -O
       after updating the files in the folder
'''

import logging
import os
import socket
import uuid
import io
import random
import json
import asyncio
import time
import datetime
import argparse
import csv
import hashlib
import unicodedata
from signal import SIGTERM, SIGINT
from .BingDailyWallpaperManager import (
    BING_COLLECTION_ID,
    BingDailyWallpaperManager,
)
from .MuseumLabelManager import MuseumLabelManager
from .mqtt_integration import MQTTIntegrationMixin, _MatteRejectedError
from .pil_methods import PIL_methods
from .tv_connection import (
    FrameTVConnection,
    TEST_MODE_HANG_ON_READY,
    TEST_MODE_NORMAL,
    describe_test_mode,
    read_test_mode,
)
HAVE_PIL = False
try:
    # Import Pillow submodules defensively. In some runtime environments a
    # partially-initialised PIL package may exist and lack attributes like
    # ImageFilter; catch broad exceptions to avoid aborting module import.
    from PIL import Image
    # ImageFilter and ImageChops are optional helpers; import if available
    try:
        from PIL import ImageFilter, ImageChops  # type: ignore
    except Exception:
        ImageFilter = None
        ImageChops = None
    HAVE_PIL = True
except Exception:
    HAVE_PIL = False

logging.basicConfig(level=logging.INFO)





def parseargs():
    # Add command line argument parsing
    parser = argparse.ArgumentParser(description='Async Upload images to Samsung TV')
    parser.add_argument('ip', action="store", type=str, default=None, help='ip address of TV (default: %(default)s))')
    parser.add_argument('-f','--folder', action="store", type=str, default="./images", help='folder to load images from (default: %(default)s))')
    parser.add_argument('-m','--matte', action="store", type=str, default="none", help='default matte to use (default: %(default)s))')
    parser.add_argument('-t','--token_file', action="store", type=str, default="token_file.txt", help='default token file to use (default: %(default)s))')
    parser.add_argument('-u','--update', action="store", type=float, default=0, help='slideshow update period (mins) 0=off (default: %(default)s))')
    parser.add_argument('-c','--check', action="store", type=int, default=60, help='how often to check for new art 0=run once (default: %(default)s))')
    parser.add_argument('-s','--sync', action='store_false', default=True, help='automatically syncronize (needs Pil library) (default: %(default)s))')
    parser.add_argument('-S','--sequential', action='store_true', default=False, help='sequential slide show (default: %(default)s))')
    parser.add_argument('-O','--on', action='store_true', default=False, help='exit if TV is off (default: %(default)s))')
    parser.add_argument('-F','--favourite', action='store_true', default=False, help='include favourites in rotation (default: %(default)s))')
    parser.add_argument('-D','--debug', action='store_true', default=False, help='Debug mode (default: %(default)s))')
    parser.add_argument('-e','--exclude', action="store", type=str, nargs='*', default=[], help='filenames to exclude from slideshow (default: %(default)s))')
    parser.add_argument('-E','--exclude-content-ids', action="store", type=str, nargs='*', default=[], help='content_ids to exclude from slideshow (default: %(default)s))')
    # MQTT discovery is now the default integration path; no HA REST args
    return parser.parse_args()
    




class monitor_and_display(MQTTIntegrationMixin):
    
    allowed_ext = ['jpg', 'jpeg', 'png', 'bmp', 'tif']
    
    def __init__(self, ip, folder, period=5, update_time=1440, include_fav=False, sync=True, matte='none', sequential=False, on=False, token_file=None, exclude=[], exclude_content_ids=[]):
        self.log = logging.getLogger('Main.'+__class__.__name__)
        self.debug = self.log.getEffectiveLevel() <= logging.DEBUG
        self.ip = ip
        self.folder = folder
        self.media_root = os.environ.get('SAMSUNG_TV_ART_MEDIA_ROOT', folder)
        self.cache_path = os.environ.get('SAMSUNG_TV_ART_CACHE_FILE', '/data/uploaded_files_cache.json')
        self.pending_delete_path = '/data/pending_tv_delete_ids.json'
        self.current_key = None
        self.cache = {}
        self.selection_mtime = None
        self.selected_collections = []  # List of selected collection folders for multi-select mode
        # MQTT-driven selection (optional)
        # Always drive selections from retained MQTT; do not allow disabling via env
        self.selection_from_mqtt = True
        self.selection_mqtt_topic = os.environ.get('SAMSUNG_TV_ART_SELECTION_MQTT_TOPIC', 'frame_tv/selected_collections/state')
        self._pending_selection_change = False
        self._ignore_retained_selection_until_reconnect = False
        self.selection_only = os.environ.get('SAMSUNG_TV_ART_SELECTION_ONLY', '').lower() in ['1', 'true', 'yes']
        self.consecutive_failures = 0
        self.reconnect_delay = 5
        # Art WebSocket handshake failures get their own escalating cooldown so a
        # wedged art-app channel is not hammered every few seconds. Rapid retries
        # leave half-open sessions on the TV and can keep the channel wedged.
        self._connect_failures = 0
        self._next_connect_attempt = 0.0
        self._first_connect_failure_time = None
        self.connect_retry_max_seconds = max(
            5,
            int(os.environ.get('SAMSUNG_TV_ART_CONNECT_RETRY_MAX_SECONDS', '60')),
        )
        # Last-resort self-heal: if the TV answers REST but no Art handshake has
        # succeeded for this long, exit so the container restart policy supplies
        # a clean process. Set to 0 to disable.
        self.connect_watchdog_seconds = max(
            0,
            int(os.environ.get('SAMSUNG_TV_ART_CONNECT_WATCHDOG_SECONDS', '1800')),
        )
        self.art_status_probe_seconds = max(
            5,
            int(os.environ.get('SAMSUNG_TV_ART_STATUS_PROBE_SECONDS', '15')),
        )
        # Diagnostic only: controls how a missing ms.channel.ready is handled.
        self.test_mode = read_test_mode(self.log)
        self._last_art_status_probe = 0
        self.update_time = int(max(0, update_time*60))   #convert minutes to seconds
        self.period = min(max(5, period), self.update_time) if self.update_time > 0 else period
        self.include_fav = include_fav
        self.sync = sync
        self.matte = matte
        self.sequential = sequential
        # Allow SAMSUNG_TV_ART_SEQUENTIAL env var to override CLI arg so it persists across restarts
        _env_seq = os.environ.get('SAMSUNG_TV_ART_SEQUENTIAL', '').lower()
        if _env_seq in ('1', 'true', 'yes'):
            self.sequential = True
        elif _env_seq in ('0', 'false', 'no'):
            self.sequential = False
        # Slideshow override (persisted to /data/slideshow_override.json)
        self.slideshow_override = None  # None = auto; list[str] of path_rel = manual override
        self.slideshow_override_pending = False
        self.slideshow_override_path = '/data/slideshow_override.json'
        self.on = on
        self.exclude = exclude
        self.exclude_content_ids = exclude_content_ids
        # Autosave token to file
        self.token_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), token_file) if token_file else token_file
        self.program_data_path = './uploaded_files.json'
        self.uploaded_files = {}
        self.fav = set()
        self.api_version = 0
        self.api_version_str = None        # Raw string from get_api_version(), e.g. '0.97' or '4.3.4.0'
        # Set when the TV-dependent half of initialize() could not run because the
        # Art channel was unavailable, so the main loop can retry it later.
        self._tv_init_pending = False
        self.api_version_failed = False    # True when api_version request itself errors (e.g. error -9)
        self._ws_binary_latched = False    # True once a WS-binary upload has succeeded via -1 fallback
        self._upload_compat_warned = False  # Emit old-API diagnostic at most once
        self.start = time.time()
        self.current_content_id = None
        self.shown_content_ids = set()  # Track shown images for shuffle-without-repeat
        self.pil = PIL_methods(self)
        self.tv = None  # Defer TV connection until start_monitoring
        # Rate limits (configurable)
        self.upload_delay_seconds = int(os.environ.get('SAMSUNG_TV_ART_UPLOAD_DELAY_SECONDS', '1'))
        self.delete_delay_seconds = int(os.environ.get('SAMSUNG_TV_ART_DELETE_DELAY_SECONDS', '1'))
        self.post_delete_recovery_seconds = int(os.environ.get('SAMSUNG_TV_ART_POST_DELETE_RECOVERY_SECONDS', '5'))
        self.max_uploads = int(os.environ.get('SAMSUNG_TV_ART_MAX_UPLOADS', '10'))
        # MQTT configuration (optional)
        # mqtt_enabled: True when MQTT_HOST is explicitly configured, OR MQTT_DISCOVERY is set.
        # Previously gated only on MQTT_DISCOVERY, which silently disabled all MQTT for users
        # who configured MQTT_HOST but didn't need HA discovery.
        _mqtt_host_set = bool(os.environ.get('SAMSUNG_TV_ART_MQTT_HOST'))
        _mqtt_discovery_val = os.environ.get('SAMSUNG_TV_ART_MQTT_DISCOVERY', '').lower()
        self.mqtt_discovery = _mqtt_discovery_val in ['1', 'true', 'yes']
        self.mqtt_enabled = _mqtt_host_set or self.mqtt_discovery
        self.mqtt_host = os.environ.get('SAMSUNG_TV_ART_MQTT_HOST', 'mosquitto')
        self.mqtt_port = int(os.environ.get('SAMSUNG_TV_ART_MQTT_PORT', '1883'))
        self.mqtt_username = os.environ.get('SAMSUNG_TV_ART_MQTT_USERNAME')
        self.mqtt_password = os.environ.get('SAMSUNG_TV_ART_MQTT_PASSWORD')
        self.mqtt_discovery_prefix = os.environ.get('SAMSUNG_TV_ART_MQTT_DISCOVERY_PREFIX', 'homeassistant')
        self.mqtt_state_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_STATE_TOPIC', 'frame_tv/selected_artwork/state')
        self.mqtt_attr_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_ATTR_TOPIC', 'frame_tv/selected_artwork/attributes')
        self.mqtt_unique_id = os.environ.get('SAMSUNG_TV_ART_MQTT_UNIQUE_ID', 'frame_tv_art_selected_artwork')
        # MQTT command/ack topics
        self.mqtt_cmd_prefix = os.environ.get('SAMSUNG_TV_ART_MQTT_CMD_PREFIX', 'frame_tv/cmd')
        self.mqtt_ack_prefix = os.environ.get('SAMSUNG_TV_ART_MQTT_ACK_PREFIX', 'frame_tv/ack')
        # Collections sensor topics
        self.mqtt_collections_state_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_COLLECTIONS_STATE', 'frame_tv/collections/state')
        self.mqtt_collections_attr_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_COLLECTIONS_ATTR', 'frame_tv/collections/attributes')
        self.mqtt_collections_unique_id = os.environ.get('SAMSUNG_TV_ART_MQTT_COLLECTIONS_UNIQUE_ID', 'frame_tv_art_collections')
        self.mqtt_selected_collections_state_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SELECTED_COLLECTIONS_STATE', 'frame_tv/selected_collections/summary')
        self.mqtt_selected_collections_attr_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SELECTED_COLLECTIONS_ATTR', 'frame_tv/selected_collections/attributes')
        # Settings topics (MQTT-only settings management)
        self.mqtt_settings_state_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SETTINGS_STATE', 'frame_tv/settings/state')
        self.mqtt_settings_attr_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SETTINGS_ATTR', 'frame_tv/settings/attributes')
        # Slideshow state/available topics
        self.mqtt_slideshow_state_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SLIDESHOW_STATE', 'frame_tv/slideshow/state')
        self.mqtt_slideshow_attr_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SLIDESHOW_ATTR', 'frame_tv/slideshow/attributes')
        self.mqtt_slideshow_available_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SLIDESHOW_AVAILABLE', 'frame_tv/slideshow/available')
        self.mqtt_slideshow_presets_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SLIDESHOW_PRESETS', 'frame_tv/slideshow/presets')
        self.slideshow_presets_path = '/data/slideshow_presets.json'
        self._slideshow_presets = []  # in-memory cache, source of truth for republish
        # Per-image matte overrides — stored {path_rel: matte_id}
        self.mqtt_slideshow_mattes_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SLIDESHOW_MATTES', 'frame_tv/slideshow/mattes')
        self.mqtt_slideshow_matte_options_topic = os.environ.get('SAMSUNG_TV_ART_MQTT_SLIDESHOW_MATTE_OPTIONS', 'frame_tv/slideshow/matte_options')
        self.matte_overrides_path = '/data/matte_overrides.json'
        self._matte_overrides = {}
        self._matte_options_cache = None  # {'matte_types':[...], 'matte_colors':[...]}
        # Matte support is reported by the TV via get_matte_list() (published
        # to MQTT on demand). The TV lists EVERY layout it supports, not just
        # the ones that will succeed for a given image — Samsung's own app
        # decides per-image at render time, which is too expensive for us to
        # replicate. Instead we surface every option and react to the TV's
        # error -7 at apply time (revert + warn).
        self.ha_rest_enabled = False  # REST disabled in MQTT-only build
        self._mqtt = None
        self._presets_from_broker = False   # True once retained presets msg received
        self._presets_bootstrap_timer = None
        self._mqtt_config_published = False
        # CSV metadata (optional)
        self.csv_path = os.environ.get('SAMSUNG_TV_ART_CSV_PATH', '/app/artwork_data.csv')
        self._csv_headers = []
        self._csv_by_file = {}
        self._csv_by_path = {}  # keyed by artwork_dir/artwork_file — avoids filename collisions across collections
        # Collections source (folders by default; optional unique artists from CSV)
        self.collections_from_csv = os.environ.get('SAMSUNG_TV_ART_COLLECTIONS_FROM_CSV', 'true').lower() in ['1','true','yes']
        # CSV change detection (polling)
        self.csv_check_interval = int(os.environ.get('SAMSUNG_TV_ART_CSV_CHECK_SECONDS', '60'))
        self._csv_last_check = 0
        self._csv_mtime = None
        try:
            self.wait_for_csv_seconds = int(os.environ.get('SAMSUNG_TV_ART_WAIT_FOR_CSV_SECONDS', '120'))
        except Exception:
            self.wait_for_csv_seconds = 120
        self.require_csv_on_start = os.environ.get('SAMSUNG_TV_ART_REQUIRE_CSV_ON_START', 'true').lower() in ['1', 'true', 'yes']
        # Startup selections are driven by retained MQTT only; no default env selection
        # Memory logging interval in seconds (0 disables)
        try:
            self.memlog_seconds = int(os.environ.get('SAMSUNG_TV_ART_MEMLOG_SECONDS', '0'))
        except Exception:
            self.memlog_seconds = 0
        # Periodic MQTT state refresh (seconds). Publishes current TV artwork even if unchanged
        try:
            self.state_refresh_seconds = int(os.environ.get('SAMSUNG_TV_ART_STATE_REFRESH_SECONDS', '300'))
        except Exception:
            self.state_refresh_seconds = 300
        self._last_state_publish = 0
        self._refresh_in_progress = False
        self._startup_in_progress = False  # True between MQTT init and end of initialize()
        self._in_art_mode = None           # None = unknown, True/False = last known state
        self._tv_powered_on = None         # None = unknown; updated only from REST or power events
        self._last_slideshow_paths = set() # path_rel values from the previous seed, used to avoid re-picking the same images
        self._loop = None
        self._collections_sync_running = False
        self._artmode_event = asyncio.Event()  # Set when TV signals an Art Mode change
        self._status_check_needed = True
        self._not_in_artmode_logged = False    # suppress repeated 'not in art mode' messages
        self._tv_shutdown_signaled = False
        self._tv_off_confirmed = False
        self._tv_state_lock = asyncio.Lock()
        self.museum_labels = MuseumLabelManager(self)
        self.bing_daily = BingDailyWallpaperManager(self)
        try:
            #doesn't work in Windows
            asyncio.get_running_loop().add_signal_handler(SIGINT, self.close)
            asyncio.get_running_loop().add_signal_handler(SIGTERM, self.close)
        except Exception:
            pass
    
    def _map_to_artwork_dir(self, name: str):
        """Translate a provided collection name (possibly artist_name with spaces)
        to the on-disk folder (artwork_dir). Returns a valid folder name or None."""
        try:
            if not name:
                return None
            if self.bing_daily.is_bing_collection(name):
                return BING_COLLECTION_ID
            # If already a valid folder, keep as-is
            path = os.path.join(self.media_root, name)
            if os.path.isdir(path):
                return name
            # Try direct directory normalization (works even before CSV metadata is loaded)
            resolved = self._resolve_dir_from_name(name)
            if resolved:
                return resolved
            # Try CSV artist_name -> artwork_dir mapping (populated by _load_csv_metadata)
            amap = getattr(self, '_artist_to_dir', {})
            if amap:
                dn = amap.get(name) or amap.get(self._normalize_collection_key(name))
                if dn and os.path.isdir(os.path.join(self.media_root, dn)):
                    return dn
        except Exception:
            pass
        return None

    def _normalize_collection_key(self, value: str) -> str:
        try:
            s = str(value or '').strip().lower()
            s = unicodedata.normalize('NFKD', s)
            s = ''.join(ch for ch in s if not unicodedata.combining(ch))
            s = s.replace('_', ' ')
            s = ' '.join(s.split())
            return s
        except Exception:
            return str(value or '').strip().lower()

    def _resolve_dir_from_name(self, name: str):
        try:
            target = self._normalize_collection_key(name)
            if not target:
                return None
            dirs = self._scan_collections()
            for d in dirs:
                if self._normalize_collection_key(d) == target:
                    return d
            # For subdirectory collections (e.g. "Artists/Kelly_Burns"), also try matching
            # against just the leaf name so a bare label like "Kelly Burns" resolves correctly.
            for d in dirs:
                if self._normalize_collection_key(os.path.basename(d)) == target:
                    return d
            # common fallback: spaces in labels vs underscores on disk
            underscored = target.replace(' ', '_')
            for d in dirs:
                if d.lower() == underscored.lower():
                    return d
                if os.path.basename(d).lower() == underscored.lower():
                    return d
        except Exception:
            pass
        return None
    
    def _create_tv_connection(self):
        """Create TV connection object. May raise if TV is unreachable."""
        self.tv = FrameTVConnection(
            host=self.ip,
            token_file=self.token_file,
            artmode_event=self._artmode_event,
            logger=self.log,
            reconnect_delay=self.reconnect_delay,
            power_state_callback=self._handle_tv_power_signal,
        )

    def _handle_tv_power_signal(self, state):
        self._status_check_needed = True
        if state == 'standby':
            self._tv_shutdown_signaled = True
            self._tv_off_confirmed = False
            self._tv_powered_on = False
            self._in_art_mode = False
            self._publish_mqtt_state('TV Power Off', 'power_off', None)
        elif state == 'wakeup':
            # A wake event proves the shutdown transition completed. The old
            # connection remains retired and will be replaced after REST is on.
            self._tv_off_confirmed = True
        
    async def start_monitoring(self):
        '''
        program entry point
        '''
        try:
            self._loop = asyncio.get_running_loop()
        except Exception:
            self._loop = None
        if self.test_mode != TEST_MODE_NORMAL:
            self.log.warning(
                'SAMSUNG_TV_ART_TEST_MODE=%d active: %s. This is a diagnostic '
                'setting and must not be left enabled in normal operation.',
                self.test_mode,
                describe_test_mode(self.test_mode),
            )
        # Create TV connection (may raise if TV offline)
        try:
            self._create_tv_connection()
        except Exception as e:
            self.log.warning('TV unavailable at startup — MQTT/web UI will run without TV (retry on reconnect): %s', e)
        # Ensure CSV metadata is loaded before MQTT and selection logic.
        if self.require_csv_on_start:
            ready = await self._wait_for_csv_metadata()
            if not ready:
                raise RuntimeError(f'CSV metadata unavailable at startup: {self.csv_path}')
        else:
            self._load_csv_metadata()
        self.bing_daily.register_cached_metadata()
        # Load persisted slideshow override before MQTT so state is ready when topics publish
        self._load_slideshow_override()
        self._load_slideshow_presets()
        self._load_matte_overrides()
        self._restore_cached_selection()
        # Init MQTT if enabled
        if self.mqtt_enabled:
            self._init_mqtt()
            # Lock the UI as early as possible. TV connect and the optional matte
            # probe can take many seconds; without an early uploading=true
            # signal, the web UI shows slideshow controls as available during
            # this window (driven by the previous session's retained state).
            # We publish discovery + slideshow_state now so the grid locks
            # immediately on (re)connect.  These are re-published below once the
            # full discovery suite runs; the duplicate is harmless.
            try:
                self._startup_in_progress = True
                self._publish_mqtt_discovery()
                self._publish_slideshow_state()
            except Exception:
                pass
        # Start periodic memory logging if enabled
        try:
            if getattr(self, 'memlog_seconds', 0) > 0:
                asyncio.create_task(self._memlogger())
        except Exception:
            pass

        if self.on and self.tv is not None and not await self.tv.on():
            self.log.info('TV is off, exiting')
        else:
            self.log.info('Start Monitoring')
            if self.tv is not None:
                try:
                    # Use a longer timeout when no token exists (first-time pairing) so the
                    # user has time to see and accept the pairing prompt on the TV.
                    # An empty token file counts as "no token" — the TV will prompt for
                    # approval and the handshake needs the full window, otherwise it
                    # times out (ms.channel.timeOut) and re-prompts on every reconnect.
                    needs_pairing = self.tv.requires_pairing
                    connect_timeout = 120 if needs_pairing else 15
                    if needs_pairing:
                        self.log.info('No saved token — connecting (up to %ds); the TV may show a one-time pairing prompt', connect_timeout)
                    if await self.tv.is_powered_on():
                        await asyncio.wait_for(self.tv.start_listening(), timeout=connect_timeout)
                        self.log.info('Started')
                    else:
                        self.log.info('TV is powered off; waiting via REST before opening Art WebSocket')
                        self.tv.retire()
                except asyncio.TimeoutError:
                    self.log.warning('TV connection timed out at startup — will retry in main loop')
                except Exception as e:
                    self.log.error('failed to connect with TV: {}'.format(e))
                if self.tv.is_alive():
                    try:
                        # Determine the Art API version before the first upload so legacy
                        # Frame TVs use the correct WS-binary transport.
                        await self.get_api_version()
                        await self.check_matte()
                    except Exception as e:
                        self.log.warning('Startup TV setup error (non-fatal): %s', e)
            # Always run select_artwork — even if TV is not reachable right now,
            # the main loop will wait for art mode in the meantime.
            try:
                if self.mqtt_enabled:
                    self._publish_mqtt_discovery()
                    self._publish_collections_discovery()
                    self._publish_settings_discovery()
                    await asyncio.sleep(0)
                    self._publish_collections_state()
                    self._publish_settings_state()
                    self._startup_in_progress = True
                    self._publish_slideshow_state()
                    self._publish_slideshow_available()
                    # If TV is offline, clear any stale retained artwork state so the
                    # web UI doesn't show "in art mode" from a previous session.
                    if self.tv is None:
                        self._in_art_mode = False
                        self._publish_mqtt_state('Unavailable', 'unavailable', None)
                await self.select_artwork()
            finally:
                if self.tv is not None:
                    await self.tv.close()

    async def _wait_for_csv_metadata(self):
        """Wait for CSV file and metadata headers to be available before startup continues."""
        timeout = max(0, int(getattr(self, 'wait_for_csv_seconds', 0)))
        deadline = time.time() + timeout if timeout > 0 else None
        announced_wait = False
        while True:
            try:
                if self.csv_path and os.path.isfile(self.csv_path):
                    self._load_csv_metadata()
                    if self._csv_headers:
                        return True
                if deadline is not None and time.time() >= deadline:
                    self.log.error('Timed out waiting for CSV metadata at %s after %ss', self.csv_path, timeout)
                    return False
                if not announced_wait:
                    if timeout > 0:
                        self.log.info('Waiting for CSV metadata at %s (timeout %ss) before continuing startup', self.csv_path, timeout)
                    else:
                        self.log.info('Waiting for CSV metadata at %s before continuing startup', self.csv_path)
                    announced_wait = True
                await asyncio.sleep(1)
            except Exception as e:
                self.log.warning('Error while waiting for CSV metadata: %s', e)
                if deadline is not None and time.time() >= deadline:
                    return False
                await asyncio.sleep(1)

    def _note_connect_success(self):
        """Clear handshake backoff after a working Art WebSocket is established."""
        if self._connect_failures:
            self.log.info(
                'Art WebSocket recovered after %d failed attempt(s)',
                self._connect_failures,
            )
        self._connect_failures = 0
        self._next_connect_attempt = 0.0
        self._first_connect_failure_time = None

    def _reset_connect_backoff(self):
        """Allow an immediate retry, e.g. once the TV power-cycles."""
        self._connect_failures = 0
        self._next_connect_attempt = 0.0
        self._first_connect_failure_time = None

    def _note_connect_failure(self, reason):
        """Record a handshake failure, log the real cause, and back off."""
        now = time.time()
        self._connect_failures += 1
        # The com.samsung.art-app channel only completes its handshake while the
        # TV is in Art Mode. On HDMI or live TV the socket opens but never emits
        # ms.channel.ready, which is expected Samsung behaviour rather than a
        # fault, so it must not be escalated or counted toward the watchdog.
        expected = self._in_art_mode is not True
        if expected:
            self._first_connect_failure_time = None
        elif self._first_connect_failure_time is None:
            self._first_connect_failure_time = now
        delay = min(
            self.reconnect_delay * (2 ** (self._connect_failures - 1)),
            self.connect_retry_max_seconds,
        )
        self._next_connect_attempt = now + delay
        if expected:
            log_fn = (
                self.log.info
                if self._connect_failures == 1
                else self.log.debug
            )
            log_fn(
                'Art channel unavailable while TV is not in Art Mode '
                '(attempt %d, next retry in %ss): %s',
                self._connect_failures,
                int(delay),
                reason,
            )
            return
        # The first few failures and every tenth afterwards are visible, so the
        # underlying cause is never silently swallowed the way it used to be.
        log_fn = (
            self.log.warning
            if self._connect_failures <= 3 or self._connect_failures % 10 == 0
            else self.log.debug
        )
        log_fn(
            'Fresh TV connection failed (attempt %d, next retry in %ss): %s',
            self._connect_failures,
            int(delay),
            reason,
        )
        self._check_connect_watchdog(now)

    def _check_connect_watchdog(self, now):
        """Exit when the Art channel stays unusable while the TV answers REST."""
        if self.connect_watchdog_seconds <= 0:
            return
        if self._first_connect_failure_time is None:
            return
        stuck_seconds = now - self._first_connect_failure_time
        if stuck_seconds < self.connect_watchdog_seconds:
            return
        self.log.error(
            'No Art WebSocket handshake succeeded for %d minute(s) across %d '
            'attempts while the TV answered REST; exiting so the container '
            'restarts with a clean process',
            int(stuck_seconds // 60),
            self._connect_failures,
        )
        try:
            logging.shutdown()
        finally:
            os._exit(1)

    async def _art_liveness_loop(self):
        """Prove the Art channel still answers, independent of the main loop.

        This runs regardless of the cached art mode: a TV sitting on HDMI is
        exactly when a silently dead socket would otherwise go unnoticed,
        because no art_mode_changed event can arrive to wake anything up.
        """
        while True:
            await asyncio.sleep(self.art_status_probe_seconds)
            try:
                if self._refresh_in_progress or self.tv is None:
                    continue
                if self._tv_shutdown_signaled:
                    continue
                if self._tv_powered_on is False:
                    # The main loop already polls REST on its own cadence while
                    # the TV is off, and it clears this flag as soon as power
                    # returns. Probing here would only duplicate that work.
                    continue
                self._log_art_liveness_probe()
                previous = self._in_art_mode
                started = time.monotonic()
                in_artmode = await self.safe_in_artmode()
                self._last_art_status_probe = time.time()
                # _in_art_mode is None when the Art channel could not be reached
                # at all, which is different from the TV genuinely reporting that
                # Art Mode is off. Reporting both as False was misleading.
                if self._in_art_mode is None:
                    outcome = 'unknown (art channel unreachable)'
                else:
                    outcome = str(bool(in_artmode))
                self.log.info(
                    'Art liveness probe result: art_mode=%s (socket=%s, '
                    'total=%.2fs incl. REST probe)',
                    outcome,
                    self._describe_socket_state(),
                    time.monotonic() - started,
                )
                if self._in_art_mode is not None and bool(in_artmode) != bool(previous):
                    # Wake the main loop so a transition is acted on promptly.
                    self._status_check_needed = True
                    self._artmode_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.debug('Art liveness probe failed: %s', exc)

    def _describe_socket_state(self):
        """Report the Art WebSocket transport state for diagnostic logging."""
        tv = self.tv
        if tv is None:
            return 'no-connection'
        try:
            if tv.retired:
                return '{} (retired)'.format(tv.socket_state)
            return tv.socket_state
        except Exception:
            return 'unavailable'

    def _log_art_liveness_probe(self):
        """Announce the periodic real Art request used to prove liveness."""
        tv = self.tv
        try:
            channel_ready = bool(tv.channel_ready) if tv is not None else False
        except Exception:
            channel_ready = False
        self.log.info(
            'Art liveness probe starting: REST power probe + '
            'get_artmode_status (socket=%s, channel_ready=%s)',
            self._describe_socket_state(),
            channel_ready,
        )

    async def reconnect_tv(self, power_verified=False):
        """Replace a dropped Art client; never reopen the old WebSocket object."""
        self._status_check_needed = True
        if time.time() < self._next_connect_attempt:
            return False
        old_tv = self.tv
        if old_tv is not None:
            old_tv.retire()
            try:
                await old_tv.close()
            except Exception:
                pass
        self.tv = None
        try:
            self._create_tv_connection()
            if not power_verified and not await self.tv.is_powered_on():
                # A powered-off TV is not a handshake failure; keep recovery
                # instant for when it wakes back up.
                self._reset_connect_backoff()
                self.tv.retire()
                return False
            if self.test_mode == TEST_MODE_HANG_ON_READY:
                # No outer timeout, so the stall is observable exactly as it is in
                # homebridge-samsung-tizen rather than being cut short at 15s.
                await self.tv.start_listening()
            else:
                await asyncio.wait_for(self.tv.start_listening(), timeout=15)
            if self.tv.is_alive():
                self._note_connect_success()
                self.log.info('Connected to TV with a fresh WebSocket client')
                return True
            self._note_connect_failure('WebSocket closed immediately after start')
        except Exception as e:
            self._note_connect_failure('{}: {}'.format(type(e).__name__, e))
        if self.tv is not None:
            self.tv.retire()
        return False

    async def safe_in_artmode(self, allow_during_refresh=False):
        async with self._tv_state_lock:
            return await self._safe_in_artmode_unlocked(allow_during_refresh)

    async def _safe_in_artmode_unlocked(self, allow_during_refresh=False):
        """Return True if TV reports art mode; False on any error. Uses exponential backoff."""
        try:
            if self.tv is None:
                self._create_tv_connection()
            try:
                powered_on = await self.tv.is_powered_on()
            except Exception as e:
                self.log.debug('TV REST power probe failed: %s', e)
                # A failed REST probe does not prove the Art socket is dead, and
                # discarding a live socket here forced a replacement that the TV
                # has to allocate memory for. Leave the connection alone; the
                # listener and the Art request threshold retire it if it is
                # genuinely gone.
                self._tv_powered_on = None
                self._in_art_mode = None
                self.consecutive_failures += 1
                self._status_check_needed = True
                self._publish_mqtt_state('Unknown status', 'unknown', None)
                return False

            if not powered_on:
                self._tv_off_confirmed = True
                self.tv.retire()
                # The TV being off is not an Art handshake failure. Clearing the
                # backoff keeps wake-up recovery immediate.
                self._reset_connect_backoff()
                prev = self._in_art_mode
                power_was_on = self._tv_powered_on is not False
                self._tv_powered_on = False
                self._in_art_mode = False
                self._status_check_needed = False
                self.consecutive_failures = 0
                if prev is True or power_was_on:
                    self._publish_mqtt_state('TV Power Off', 'power_off', None)
                return False

            if self._tv_shutdown_signaled and not self._tv_off_confirmed:
                # The TV may report REST power=on briefly while shutting down.
                # Wait until off is observed (or a wakeup event arrives) before
                # creating any replacement Art WebSocket.
                self._tv_powered_on = False
                self._in_art_mode = False
                return False

            if self._refresh_in_progress and not allow_during_refresh:
                return False

            if self._tv_shutdown_signaled:
                self._tv_shutdown_signaled = False
            self._tv_off_confirmed = False
            self._tv_powered_on = True

            if self.tv.retired or not self.tv.is_alive():
                if not await self.reconnect_tv(power_verified=True):
                    self.consecutive_failures += 1
                    self._in_art_mode = None
                    self._publish_mqtt_state('Unknown status', 'unknown', None)
                    return False

            in_artmode = await self.tv.query_artmode(power_verified=True)
            # Success - reset failure counter
            self.consecutive_failures = 0
            prev = self._in_art_mode
            self._in_art_mode = bool(in_artmode)
            self._status_check_needed = False
            if prev is not False and not in_artmode:
                # TV just left art mode — publish sentinel with in_art_mode: False so UIs disable
                self._publish_mqtt_state('Unavailable', 'unavailable', None)
            elif prev is not True and in_artmode:
                # TV just (re-)entered art mode, or this is the first confirmed
                # True after startup (prev=None).  Immediately republish so UIs
                # re-enable and any stale retained in_art_mode:false from a
                # previous session is cleared.
                # Set _last_state_publish=0 AND do an immediate forced publish so the
                # in_art_mode: True state is pushed to MQTT now, even if a reseed starts
                # right after (which would set _refresh_in_progress=True and block the
                # next periodic publish for the entire reseed duration).
                self._last_state_publish = 0
                if not self._refresh_in_progress:
                    try:
                        await self._publish_current_artwork_state(
                            force=True,
                            state_locked=True,
                        )
                    except Exception:
                        pass
            return in_artmode
        except AssertionError:
            self.consecutive_failures += 1
            log_fn = self.log.warning if self.consecutive_failures == 1 else self.log.debug
            log_fn('TV artmode check failed (empty response, failure %d); status unknown', self.consecutive_failures)
            self._in_art_mode = None
            self._status_check_needed = True
            self._publish_mqtt_state('Unknown status', 'unknown', None)
            return False
        except Exception as e:
            self.consecutive_failures += 1
            log_fn = self.log.warning if self.consecutive_failures == 1 else self.log.debug
            log_fn('TV artmode check failed (failure %d): %s', self.consecutive_failures, e)
            self._in_art_mode = None
            self._status_check_needed = True
            self._publish_mqtt_state('Unknown status', 'unknown', None)
            return False

    def get_backoff_delay(self):
        """Calculate exponential backoff delay based on consecutive failures."""
        # Failure-only retry cadence: 1, 2, 4, then every 8 seconds.
        return min(2 ** max(0, self.consecutive_failures - 1), 8)

    async def _delete_tv_upload_ids(self, content_ids):
        """Delete known old uploads after replacement artwork is active."""
        content_ids = list(dict.fromkeys(cid for cid in content_ids if cid))
        if not content_ids:
            return
        self.log.info('Cleaning up %d replaced upload(s) from TV...', len(content_ids))
        failed = []
        referenced_ids = {
            record.get('content_id')
            for record in self.uploaded_files.values()
            if record.get('content_id')
        }
        for index, content_id in enumerate(content_ids):
            if (
                content_id == self.current_content_id
                or content_id in referenced_ids
            ):
                failed.append(content_id)
                self.log.warning(
                    'Deferring deletion of active or referenced artwork: %s',
                    content_id,
                )
                continue
            try:
                await self.tv.delete_list([content_id])
                self.log.debug('Deleted %d/%d', index + 1, len(content_ids))
            except Exception as exc:
                failed.append(content_id)
                self.log.warning(
                    'Failed to delete replaced upload %s: %s',
                    content_id,
                    exc,
                )
            if index < len(content_ids) - 1:
                await asyncio.sleep(self.delete_delay_seconds)
        self._save_pending_delete_ids(failed)
        if len(failed) != len(content_ids):
            self.log.info('Waiting for TV to recover after deletions...')
            await asyncio.sleep(self.post_delete_recovery_seconds)

    def _load_pending_delete_ids(self):
        try:
            with open(self.pending_delete_path, 'r', encoding='utf-8') as source:
                value = json.load(source)
            return {
                content_id
                for content_id in value
                if isinstance(content_id, str) and content_id
            }
        except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError):
            return set()

    def _save_pending_delete_ids(self, content_ids):
        content_ids = sorted(set(content_ids))
        try:
            if not content_ids:
                if os.path.isfile(self.pending_delete_path):
                    os.remove(self.pending_delete_path)
                return
            temp_path = self.pending_delete_path + '.tmp'
            with open(temp_path, 'w', encoding='utf-8') as output:
                json.dump(content_ids, output)
            os.replace(temp_path, self.pending_delete_path)
        except OSError as exc:
            self.log.warning('Failed to persist pending TV deletions: %s', exc)

    def _queue_pending_delete_ids(self, content_ids):
        pending = self._load_pending_delete_ids()
        pending.update(content_id for content_id in content_ids if content_id)
        self._save_pending_delete_ids(pending)

    async def _select_replacement(self, content_ids):
        content_ids = set(content_ids)
        target = next(
            (
                content_id
                for content_id in self.get_content_ids()
                if content_id in content_ids
            ),
            None,
        )
        if not target:
            raise RuntimeError('No uploaded replacement artwork is available')
        if target != self.current_content_id:
            await self.tv.select_image(target)
            self.current_content_id = target
            self.shown_content_ids.add(target)
            await self.update_ha_selected_artwork(target)
        return target

    async def _drain_pending_delete_ids(self):
        pending = self._load_pending_delete_ids()
        if pending:
            await self._delete_tv_upload_ids(pending)

    def get_selected_folder(self):
        """Return selected folder based on MQTT-driven selection.

        Uses the first selected collection (if any); otherwise keeps current folder.
        """
        if self.selected_collections:
            return os.path.join(self.media_root, self.selected_collections[0])
        return self.folder

    def apply_selection(self):
        """Update folder if the selection changed (via MQTT)."""
        previous_collections = self.selected_collections.copy()
        desired = self.get_selected_folder()
        collections_changed = (self.selected_collections != previous_collections) or self._pending_selection_change
        if desired != self.folder or collections_changed:
            if not os.path.isdir(desired):
                self.log.warning('Selected folder does not exist: %s', desired)
                return False
            self.log.info('Selection changed, switching folder to %s', desired)
            if collections_changed:
                self.log.info('Collections changed: %s', self.selected_collections)
            self.folder = desired
            self.fav = set()
            self.shown_content_ids = set()  # Reset shuffle tracking on collection change
            self.set_current_cache()
            self._pending_selection_change = False
            return True
        return False

    def get_cache_key(self, folder_path):
        try:
            return os.path.relpath(folder_path, self.media_root)
        except Exception:
            return folder_path

    def load_cache(self):
        if self.cache and self.current_key is not None:
            return
        if os.path.isfile(self.cache_path):
            try:
                with open(self.cache_path, 'r') as f:
                    self.cache = json.load(f)
            except Exception:
                self.cache = {}
        else:
            self.cache = {}

    def _read_cached_selected_collections(self):
        try:
            self.load_cache()
            return self.cache.get('_selected_collections', [])
        except Exception:
            return []

    def _restore_cached_selection(self):
        """Restore the prior selection before retained MQTT state is delivered."""
        cached = self._read_cached_selected_collections()
        if not cached:
            return
        try:
            available = set(self._scan_collections())
            mapped = []
            for collection in cached:
                resolved = self._map_to_artwork_dir(collection) or collection
                if resolved in available and resolved not in mapped:
                    mapped.append(resolved)
            mapped = self.bing_daily.normalize_collections(mapped)
            if not mapped:
                return
            self.selected_collections = mapped
            desired = self.get_selected_folder()
            if os.path.isdir(desired):
                self.folder = desired
            self.set_current_cache()
            self.log.info('Restored cached collection selection: %s', mapped)
        except Exception as e:
            self.log.warning('Failed to restore cached collection selection: %s', e)

    def save_cache(self):
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f)
        except Exception as e:
            self.log.warning('Failed to save cache: %s', e)

    def set_current_cache(self):
        self.load_cache()
        self.current_key = self.get_cache_key(self.folder)
        data = self.cache.get(self.current_key, {})
        loaded_files = data.get('uploaded_files', {})
        self.uploaded_files = {
            record.get('path_rel') or key: record
            for key, record in loaded_files.items()
        }
        self.start = data.get('last_update', time.time())
        # Restore last slideshow paths so the next seed avoids repeating the same images.
        # Fall back to deriving them from uploaded_files (backward compat with old cache).
        persisted_paths = self.cache.get('_last_slideshow_paths')
        if persisted_paths is not None:
            self._last_slideshow_paths = set(persisted_paths)
        elif self.uploaded_files:
            self._last_slideshow_paths = {
                v.get('path_rel') for v in self.uploaded_files.values() if v.get('path_rel')
            }

    def _cache_selected_collections(self):
        try:
            self.load_cache()
            self.cache['_selected_collections'] = list(self.selected_collections)
            self.save_cache()
        except Exception as e:
            self.log.warning('Failed to cache selected_collections: %s', e)

    def close(self):
        '''
        exit on signal
        '''
        self.log.info('SIGINT/SIGTERM received, exiting')
        os._exit(1)
        
    # Art API versions known to require the WS-binary upload transport instead of the
    # D2D socket path (2018/2019 Frame TVs and legacy Art API builds such as 1.07 on
    # the UE55LS003 / 17_KANTM_UHD).
    _WS_BINARY_API_VERSIONS = ('0.97', '1.07')

    async def get_api_version(self):
        '''
        checks api version to see if it's old (<2021) or new type
        sets api_version to 0 for old, and 1 for new

        Idempotent: this may be queried before initialization; once the version is
        known we skip the redundant TV request.
        A previous failure leaves api_version_str unset so a later call can retry.
        '''
        if self.api_version_str is not None:
            return
        try:
            api_version = await self.tv.get_api_version()
            self.log.info('API version: {}'.format(api_version))
            self.api_version_str = api_version
            self.api_version = 0 if int(api_version.replace('.','')) < 4000 else 1
        except Exception as e:
            self.log.warning('Failed to get API version: %s', e)
            self.api_version = 0
            self.api_version_failed = True
        
    async def _upload_ws_binary(self, file_data, file_type, matte):
        '''Upload image via WebSocket binary frame — required for Art API 0.97 (2018/2019 Frame TVs).

        SmartThings protocol confirmed by Wireshark (see https://github.com/xchwarze/samsung-tv-ws-api/issues/130):
          payload = uint16_be(len(header_json)) + header_json_bytes + image_bytes
        where header_json wraps the send_image request in a ms.channel.emit binary frame.
        The TV responds with an image_added d2d_service_message containing content_id.
        '''
        upload_id = str(uuid.uuid4())
        ft = file_type.lower()
        ft_hdr = 'JPEG' if ft in ('jpg', 'jpeg') else ft.upper()

        inner = json.dumps({
            'request': 'send_image',
            'file_type': ft_hdr,
            'matte_id': matte or 'none',
            'id': upload_id,
        }, separators=(',', ':'))
        outer = json.dumps({
            'method': 'ms.channel.emit',
            'params': {
                'data': inner,
                'to': 'host',
                'event': 'art_app_request',
            },
        }, separators=(',', ':')).encode('utf-8')
        payload = len(outer).to_bytes(2, 'big') + outer + file_data

        # Register response future BEFORE sending to avoid a race with fast TVs
        self.tv.pending_requests[upload_id] = asyncio.Future()
        await self.tv.start_listening()
        assert self.tv.connection, 'TV WebSocket connection not available'
        await self.tv.connection.send(payload)   # bytes → WebSocket binary frame
        data = await self.tv.wait_for_response(upload_id, timeout=30)
        return data.get('content_id') if data else None

    async def _upload_to_tv(self, file_data, file_type, matte):
        '''Route upload to the correct method based on Art API version.

        Known-legacy Art API versions (_WS_BINARY_API_VERSIONS, e.g. 0.97/1.07) use the
        WS-binary frame upload directly. Every other case tries the D2D socket upload
        first; if the TV rejects it with error -1 — the symptom of a model that silently
        requires WS-binary — we fall back to the binary path and latch it for the rest of
        the session so subsequent uploads skip the failing D2D attempt. This covers both
        TVs whose version could not be determined and TVs that report a version but still
        need WS-binary (e.g. Art API 1.07 on UE55LS003 / 17_KANTM_UHD).
        '''
        if self._ws_binary_latched or self.api_version_str in self._WS_BINARY_API_VERSIONS:
            return await self._upload_ws_binary(file_data, file_type, matte)
        try:
            return await self.tv.upload(file_data, file_type=file_type, matte=matte, portrait_matte=matte)
        except Exception as e:
            if 'error number -1' not in str(e):
                raise
            self.log.warning(
                'D2D upload failed with error -1 — retrying with WS-binary transport '
                '(legacy Frame TV fallback). api_version=%s',
                self.api_version_str or 'unknown',
            )
            result = await self._upload_ws_binary(file_data, file_type, matte)
            if result:
                # Latch so all subsequent uploads use the binary path directly.
                self._ws_binary_latched = True
                self.log.info('WS-binary upload succeeded — latching binary transport for this session')
            return result

    def _warn_upload_compat(self, error):
        '''Emit a one-time diagnostic when uploads fail with error -1.
        _upload_to_tv already retries such failures over WS-binary, so this is a
        best-effort hint for the case where the binary fallback also failed.'''
        if self._upload_compat_warned:
            return
        if 'error number -1' not in str(error):
            return
        self._upload_compat_warned = True
        self.log.warning(
            'Upload failed with error -1 and the WS-binary fallback did not recover. '
            'If this is a legacy Frame TV (2018/2019 or Art API 0.97/1.07), check the '
            'api_version reported in the logs. '
            'See https://github.com/xchwarze/samsung-tv-ws-api/issues/130 for background.'
        )

    async def check_matte(self):
        '''
        checks if the matte passed for uploads to use is valid type and color
        '''
        if self.matte != 'none':
            matte = self.matte.split('_')
            try:
                mattes = await self.tv.get_matte_list(True)
                matte_types, matte_colors = ([m['matte_type'] for m in mattes[0]], [m['color'] for m in mattes[1]])
                if matte[0] in matte_types and matte[1] in matte_colors:
                    self.log.info('using matte: {}'.format(self.matte))
                    return
                else:
                    self.log.info('Valid mattes types: {} and colors: {}'.format(matte_types, matte_colors))
                self.log.warning('Invalid matte selected: {}. A valid matte would be shadowbox_polar for eample, using none'.format(self.matte))
            except AssertionError:
                self.log.warning('Error getting mattes list, setting to none')
            self.matte = 'none'
            
    def _art_channel_is_live(self):
        """True when the Art channel finished its handshake and is still usable."""
        tv = self.tv
        if tv is None or tv.retired:
            return False
        try:
            return bool(tv.is_alive() and tv.channel_ready)
        except Exception:
            return False

    async def _initialize_tv_state(self):
        """Run the TV-dependent half of initialization, deferring if needed.

        Startup frequently happens while the TV is on HDMI, where the art-app
        channel never completes its handshake. This used to run once against a
        dead channel, report the TV as empty, and never retry for the life of
        the process, leaving uploaded_files unreconciled all session.
        """
        if not self._art_channel_is_live():
            if not self._tv_init_pending:
                self.log.info(
                    'Art channel is not available yet; deferring TV-dependent '
                    'initialization until the handshake succeeds'
                )
            self._tv_init_pending = True
            return
        await self.get_api_version()
        if self._in_art_mode is True:
            await self._drain_pending_delete_ids()
        if not self.sync:
            self.log.warning('syncing disabled, not updating uploaded files list')
            self._tv_init_pending = False
            return
        if self.api_version_str in self._WS_BINARY_API_VERSIONS:
            # Legacy Frame TVs (Art API 0.97/1.07) return thumbnails as a single
            # unframed binary WebSocket packet on the main connection. The library's
            # listen loop tries to JSON-decode it, throws, and leaves the async
            # request/response dispatcher desynchronized — after which every request
            # (get_artmode, etc.) times out and rotation stalls. Skip thumbnail sync
            # entirely on these models; content IDs are tracked via the persistent
            # cache, so the PIL reconciliation isn't needed.
            self.log.info(
                'Skipping PIL thumbnail sync on legacy Art API %s — this firmware returns '
                'thumbnails as an unframed binary WS packet that desyncs the async dispatcher; '
                'content IDs are tracked via the persistent cache instead.',
                self.api_version_str,
            )
            self._tv_init_pending = False
            return
        completed = await self.pil.initialize()
        self._tv_init_pending = not completed
        if not completed:
            self.log.info(
                'TV-dependent initialization is incomplete; it will retry once '
                'the Art channel recovers'
            )

    async def initialize(self):
        '''
        initializes program
        gets API version, and current displayed art content_id
        uses PIL if available to try to match files in folder with content_id on tv.
        this matching is not really needed if uploaded_files (loaded from file) is accurate,
        and can be skipped by setting sync (-s) to False
        '''
        await self.get_api_version()
        self.current_content_id = await self.get_current_artwork()
        self.log.info('Current artwork is: {}'.format(self.current_content_id))
        # If art mode hasn't been confirmed True yet, do one more check before
        # publishing to avoid a transient false state immediately after connection.
        if self._in_art_mode is not True and self.tv is not None:
            try:
                await asyncio.sleep(1)
                await self.safe_in_artmode()
            except Exception:
                pass
        try:
            await self._publish_current_artwork_state(force=True)
        except Exception:
            pass
        # Fallback selection: if nothing selected via MQTT, restore cached selection
        # or auto-select all available collections.
        try:
            if not self.selected_collections:
                cached = self._read_cached_selected_collections()
                mapped = []
                try:
                    have = set(self._scan_collections())
                except Exception:
                    have = set()
                if cached:
                    for c in cached:
                        mc = self._map_to_artwork_dir(c) or c
                        if mc in have and mc not in mapped:
                            mapped.append(mc)
                # If no cached (or none valid), default to all available collections
                if not mapped:
                    mapped = self.bing_daily.normalize_collections(
                        sorted(collection for collection in have if collection != BING_COLLECTION_ID)
                    )
                if mapped:
                    self.selected_collections = mapped
                    desired = self.get_selected_folder()
                    if os.path.isdir(desired):
                        self.folder = desired
                    self.set_current_cache()
                    self._pending_selection_change = False
                    # Reflect fallback selection on the shared state so UIs stay consistent
                    try:
                        self._publish_selected_collections_state()
                    except Exception:
                        pass
                    self.log.info('No MQTT selection found; using fallback collections: %s', self.selected_collections)
        except Exception:
            # Non-fatal; continue with no selection
            pass
        self.load_program_data()
        self.log.info('files in directory: {}: {}'.format(self.folder, self.get_folder_files()))
        await self._initialize_tv_state()
        
        # Display art immediately after init only if TV is already in art mode.
        # If not, the main loop will pick it up when art mode is detected naturally.
        if len(self.get_content_ids()) > 0:
            if await self.safe_in_artmode():
                self.log.info('Content available after init and TV is in art mode, displaying first artwork')
                await self.change_art()
                self.start = time.time()
                self.write_program_data()
            else:
                self.log.info('Content available after init but TV is not in art mode; waiting for art mode')
        # Initialization complete — clear startup lock and let UI know it's safe
        self._startup_in_progress = False
        self._publish_slideshow_state()
        # Force-publish current artwork state so UIs clear any stale in_art_mode=false
        # retained from a previous session.
        try:
            await self._publish_current_artwork_state(force=True)
        except Exception:
            pass
        
    async def get_tv_content(self, category='MY-C0002'):
        '''
        gets content_id list of category - either My Photos (MY-C0002) or Favourites (MY-C0004) from tv
        '''
        try:
            result = [v['content_id'] for v in await self.tv.available(category, timeout=10)]
        except AssertionError:
            self.log.warning('failed to get contents from TV')
            result = None
        except Exception as e:
            self.log.warning('failed to get contents from TV: %s', e)
            result = None
        return result

    async def get_tv_content_entries(self, category='MY-C0002'):
        '''
        Full content entries (dicts including content_id and image_date) for a category,
        or None on failure.
        '''
        try:
            return list(await self.tv.available(category, timeout=10))
        except AssertionError:
            self.log.warning('failed to get contents from TV')
            return None
        except Exception as e:
            self.log.warning('failed to get contents from TV: %s', e)
            return None

    def get_folder_files(self):
        '''
        returns list of files in folder is extension matches allowed image types
        '''
        files = [
            f for f in os.listdir(self.folder)
            if os.path.isfile(os.path.join(self.folder, f))
            and self.get_file_type(os.path.join(self.folder, f)) in self.allowed_ext
        ]
        return self.museum_labels.preferred_filenames(self.folder, files)
        
    async def get_current_artwork(self):
        '''
        reads currently displayed art content_id from tv
        '''
        try:
            content_id = (await self.tv.get_current()).get('content_id')
        except Exception:
            content_id = None
        return content_id
        
            
    async def sync_file_list(self):
        '''
        if art has been deleted on tv, resyncronises uploaded_files with tv
        '''
        my_photos = await self.get_tv_content('MY-C0002')
        if my_photos is not None:
            self.uploaded_files = {k:v for k,v in self.uploaded_files.items() if v['content_id'] in my_photos}
            self.write_program_data()
        
    def get_time(self, sec):
        '''
        returns seconds as timedelta for display as h:m:s
        '''
        return datetime.timedelta(seconds = sec)
   
    def load_program_data(self):
        '''
        load previous settings on program start update
        '''
        self.set_current_cache()
        
    def write_program_data(self):
        '''
        save current settings, including file list with content_id on tv and last updated time
        also save the last time that art was updated, for timing slideshows
        '''
        program_data = {'last_update': self.start, 'uploaded_files': self.uploaded_files}
        try:
            with open(self.program_data_path, 'w') as f:
                json.dump(program_data, f)
        except Exception as e:
            self.log.warning('Failed to save program data: %s', e)

        self.load_cache()
        key = self.get_cache_key(self.folder)
        self.cache[key] = program_data
        # Persist current selected_collections for restart restore
        self.cache['_selected_collections'] = list(self.selected_collections)
        # Persist last slideshow paths so the next seed avoids repeating the same images
        self.cache['_last_slideshow_paths'] = list(self._last_slideshow_paths)
        self.save_cache()
            
    def read_file(self, filename):
        '''
        read image file, return file binary data and file type
        Resizes images larger than 4K to 4K to ensure compatibility with Samsung Frame TV.
        Also compresses images that exceed the Samsung TV art upload limit (~2 MB).
        '''
        # Art API 0.97 (2018/2019 Frame TVs) use a WS-binary upload path handled by _upload_ws_binary().
        # For all other TVs the standard D2D socket upload from the samsungtvws library is used.
        # File-size-based recompression via SAMSUNG_TV_ART_MAX_FILE_BYTES is unrelated to the
        # protocol version and can be used independently on any TV model.
        # Unset by default — no size-based recompression on modern TVs.
        _max_bytes_env = os.environ.get('SAMSUNG_TV_ART_MAX_FILE_BYTES', '').strip()
        MAX_UPLOAD_BYTES = int(_max_bytes_env) if _max_bytes_env else None
        # Optionally cap image dimensions, e.g. SAMSUNG_TV_ART_MAX_DIMENSION=1920x1080.
        # Useful for 2019 1080p Frame TVs where large images are rejected even under the byte limit.
        # Accepts "WxH" (e.g. "1920x1080") or a single number for square cap (e.g. "1920").
        _max_dim_env = os.environ.get('SAMSUNG_TV_ART_MAX_DIMENSION', '').strip()
        if _max_dim_env:
            _parts = _max_dim_env.lower().replace('x', ' ').split()
            MAX_DIM_W = int(_parts[0]) if _parts else None
            MAX_DIM_H = int(_parts[1]) if len(_parts) > 1 else MAX_DIM_W
        else:
            MAX_DIM_W, MAX_DIM_H = None, None
        try:
            with open(filename, 'rb') as f:
                file_data = f.read()
            file_type = self.get_file_type(filename)
            
            if HAVE_PIL and file_data:
                try:
                    img = Image.open(io.BytesIO(file_data))
                    # Ensure we have a mutable copy for any reprocessing below
                    img_fmt = img.format or 'JPEG'
                    needs_save = False

                    # Step 1a: Resize to fit 4K if dimensions exceed it
                    if img.width > 3840 or img.height > 2160:
                        self.log.info('Resizing image {} from {}x{} to fit 4K'.format(filename, img.width, img.height))
                        img.thumbnail((3840, 2160), Image.Resampling.LANCZOS)
                        needs_save = True

                    # Step 1b: Optionally cap to a user-specified max dimension
                    if MAX_DIM_W and (img.width > MAX_DIM_W or img.height > MAX_DIM_H):
                        self.log.info('Resizing image {} from {}x{} to fit {}x{}'.format(
                            filename, img.width, img.height, MAX_DIM_W, MAX_DIM_H))
                        img.thumbnail((MAX_DIM_W, MAX_DIM_H), Image.Resampling.LANCZOS)
                        needs_save = True

                    # Step 2: Re-encode to get accurate byte count after any resize,
                    # then progressively reduce JPEG quality until under the size limit.
                    if needs_save or (MAX_UPLOAD_BYTES and len(file_data) > MAX_UPLOAD_BYTES):
                        # Convert palette/RGBA to RGB for JPEG compatibility
                        if img.mode not in ('RGB', 'L'):
                            img = img.convert('RGB')
                        output = io.BytesIO()
                        quality = 92
                        img.save(output, format='JPEG', quality=quality)
                        file_data = output.getvalue()
                        file_type = 'jpg'

                        # Reduce quality in steps until the file fits
                        while MAX_UPLOAD_BYTES and len(file_data) > MAX_UPLOAD_BYTES and quality > 50:
                            quality -= 8
                            output = io.BytesIO()
                            img.save(output, format='JPEG', quality=max(quality, 50))
                            file_data = output.getvalue()

                        # If quality reduction alone isn't enough, also halve the resolution
                        # and retry. This handles 4K images on TVs with low byte limits.
                        if MAX_UPLOAD_BYTES and len(file_data) > MAX_UPLOAD_BYTES:
                            new_w, new_h = max(1, img.width // 2), max(1, img.height // 2)
                            self.log.info(
                                'Quality reduction insufficient for %s (%d bytes); '
                                'scaling to %dx%d and retrying',
                                filename, len(file_data), new_w, new_h
                            )
                            img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
                            quality = 92
                            output = io.BytesIO()
                            img.save(output, format='JPEG', quality=quality)
                            file_data = output.getvalue()
                            while MAX_UPLOAD_BYTES and len(file_data) > MAX_UPLOAD_BYTES and quality > 50:
                                quality -= 8
                                output = io.BytesIO()
                                img.save(output, format='JPEG', quality=max(quality, 50))
                                file_data = output.getvalue()

                        actual_quality = max(quality, 50)
                        if MAX_UPLOAD_BYTES and len(file_data) > MAX_UPLOAD_BYTES:
                            self.log.warning(
                                'Image %s is still %d bytes (quality=%d, %dx%d) after compression; '
                                'TV may reject it (limit ~%d bytes)',
                                filename, len(file_data), actual_quality,
                                img.width, img.height, MAX_UPLOAD_BYTES
                            )
                        elif needs_save or actual_quality < 92:
                            self.log.info(
                                'Image %s reprocessed to %d bytes (quality=%d, %dx%d)',
                                filename, len(file_data), actual_quality,
                                img.width, img.height
                            )
                except Exception as e:
                    self.log.warning('Failed to process image {}: {}'.format(filename, e))
            
            return file_data, file_type
        except Exception as e:
            self.log.error('Error reading file: {}, {}'.format(filename, e))
        return None, None
        
    def get_file_type(self, filename, image_data=None):
        '''
        try to figure out what kind of image file is, starting with the extension
        use PIL if available to check
        fix the file type if it's wrong
        '''
        try:
            file_type = os.path.splitext(filename)[1][1:].lower()
            file_type = file_type.lower() if file_type else None
            # Fast-path for clearly non-image extensions to avoid noisy PIL errors
            if image_data is None and file_type and file_type not in self.allowed_ext:
                return file_type
            file_type = self.pil.fix_file_type(filename, file_type, image_data)
            return file_type
        except Exception as e:
            self.log.error('Error reading file: {}, {}'.format(filename, e))
        return None

    def _get_rss_kb(self):
        try:
            with open('/proc/self/status', 'r') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            return int(parts[1])  # kB
        except Exception:
            pass
        return None

    async def _memlogger(self):
        while True:
            try:
                rss_kb = self._get_rss_kb()
                fd_count = 0
                try:
                    fd_count = len(os.listdir('/proc/self/fd'))
                except Exception:
                    fd_count = -1
                if rss_kb is not None:
                    self.log.info('Memory usage: RSS=%.2f MB, FDs=%s', rss_kb / 1024.0, fd_count)
                else:
                    self.log.info('Memory usage: RSS=unknown, FDs=%s', fd_count)
            except Exception:
                pass
            await asyncio.sleep(max(5, int(getattr(self, 'memlog_seconds', 60))))
            
    def _get_file_signature(self, path):
        try:
            digest = hashlib.sha256()
            with open(path, 'rb') as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    digest.update(chunk)
            return {
                'size': os.path.getsize(path),
                'sha256': digest.hexdigest(),
            }
        except OSError:
            return {}

    def update_uploaded_files(self, filename, content_id, full_path=None, matte_id=None):
        '''
        if file is uploaded, update the dictionary entry
        if content_id is None, file failed to upload, so remove it from the dict
        full_path is used for multi-collection mode where filename is just basename
        '''
        rel_path = None
        try:
            if full_path and self.media_root and os.path.commonpath([self.media_root, full_path]) == self.media_root:
                rel_path = os.path.relpath(full_path, self.media_root)
        except Exception:
            rel_path = None
        cache_key = rel_path or filename
        if not content_id:
            return
        self.uploaded_files.pop(filename, None)
        self.uploaded_files.pop(cache_key, None)
        record = {
            'content_id': content_id,
            'modified': self.get_last_updated(filename, full_path),
            'path_rel': rel_path or filename,
        }
        if full_path:
            record.update(self._get_file_signature(full_path))
        if matte_id is not None:
            record['matte'] = matte_id or 'none'
        self.uploaded_files[cache_key] = record

    async def _refresh_uploaded_image_dates(self, content_ids):
        content_ids = set(content_ids)
        if not content_ids:
            return
        entries = await self.get_tv_content_entries('MY-C0002')
        if entries is None:
            return
        dates_by_id = {
            entry.get('content_id'): entry.get('image_date')
            for entry in entries
            if isinstance(entry, dict)
            and entry.get('content_id') in content_ids
            and entry.get('image_date') is not None
        }
        changed = False
        for record in self.uploaded_files.values():
            image_date = dates_by_id.get(record.get('content_id'))
            if image_date is not None and record.get('image_date') != image_date:
                record['image_date'] = image_date
                changed = True
        if changed:
            self.write_program_data()
        
    async def upload_files(self, filenames, progress_cb=None):
        '''
        upload files in list to tv with rate limiting to avoid overwhelming TV
        Supports both simple filenames (from current folder) and relative paths (from multi-collection mode).
        progress_cb(idx, total, display_name) is called before each upload if provided.
        '''
        upload_delay = self.upload_delay_seconds  # seconds between uploads
        self._last_upload_attempt_count = len(filenames)
        consecutive_failures = 0
        max_consecutive_failures = 3
        uploaded_count = 0
        uploaded_content_ids = []
        
        for idx, filename in enumerate(filenames):
            # Handle both simple filenames and relative paths from multi-collection mode
            if os.path.dirname(filename):
                # Multi-collection mode: filename includes collection subfolder
                path = os.path.join(self.media_root, filename)
                display_name = filename  # Show full relative path in logs
                path_rel_for_matte = filename
            else:
                # Single folder mode: simple filename
                path = os.path.join(self.folder, filename)
                display_name = filename
                # Build a path_rel under media_root if possible, for matte lookup
                try:
                    if self.media_root and os.path.commonpath([self.media_root, path]) == self.media_root:
                        path_rel_for_matte = os.path.relpath(path, self.media_root)
                    else:
                        path_rel_for_matte = filename
                except Exception:
                    path_rel_for_matte = filename
            
            # Verify file exists before attempting upload
            if not os.path.isfile(path):
                self.log.error('File not found: %s', path)
                continue
                
            file_data, file_type = self.read_file(path)
            if file_data:
                self.log.info('uploading : {} to tv ({}/{})'.format(display_name, idx + 1, len(filenames)))
                if progress_cb:
                    try:
                        progress_cb(idx + 1, len(filenames), display_name)
                    except Exception:
                        pass
                content_id = None
                matte_for_upload = self._resolve_matte_for(path_rel_for_matte, os.path.basename(filename))
                # NOTE: On this firmware, the matte is baked into the rendered
                # composite at upload time via the send_image matte_id field.
                # Subsequent change_matte calls update stored metadata but do
                # NOT trigger a re-render, so the only way to change an
                # image's displayed matte is to delete + re-upload it. That's
                # why we pass matte_for_upload here (not 'none').
                try:
                    content_id = await self._upload_to_tv(file_data, file_type, matte_for_upload)
                    consecutive_failures = 0  # Reset on success
                except AssertionError:
                    self.log.warning('file: %s failed to upload (empty response)', display_name)
                    consecutive_failures += 1
                except Exception as e:
                    # If the failure was the TV rejecting an invalid matte
                    # for this image (error -7), retry once with matte=none
                    # and pin the per-image override to 'none' so we don't
                    # repeat the same bad combo on future uploads.
                    if (matte_for_upload and matte_for_upload != 'none'
                            and self._is_matte_minus_7(e)):
                        self.log.warning(
                            'file: %s upload rejected by TV (-7) with matte %r; '
                            'retrying with matte=none and pinning override',
                            display_name, matte_for_upload,
                        )
                        try:
                            content_id = await self._upload_to_tv(file_data, file_type, 'none')
                        except Exception as e2:
                            self.log.warning('file: %s retry without matte also failed: %s', display_name, e2)
                        if content_id:
                            self._matte_overrides[path_rel_for_matte] = 'none'
                            matte_for_upload = 'none'
                            self._save_matte_overrides()
                            self._publish_matte_overrides()
                            consecutive_failures = 0
                        else:
                            consecutive_failures += 1
                    else:
                        self._warn_upload_compat(e)
                        self.log.warning('file: %s failed to upload: %s', display_name, e)
                        consecutive_failures += 1
                # Some TV firmwares don't raise on a bad matte — they accept
                # the upload call but return no content_id. Detect that here
                # and apply the same retry-without-matte + pin-to-none path.
                if (not content_id and matte_for_upload
                        and matte_for_upload != 'none'):
                    self.log.warning(
                        'file: %s upload returned no content_id with matte %r; '
                        'retrying with matte=none and pinning override',
                        display_name, matte_for_upload,
                    )
                    try:
                        content_id = await self._upload_to_tv(file_data, file_type, 'none')
                    except Exception as e3:
                        self.log.warning('file: %s retry without matte also failed: %s', display_name, e3)
                    if content_id:
                        self._matte_overrides[path_rel_for_matte] = 'none'
                        matte_for_upload = 'none'
                        self._save_matte_overrides()
                        self._publish_matte_overrides()
                        consecutive_failures = 0
                
                # If too many consecutive failures, try reconnecting to TV
                if consecutive_failures >= max_consecutive_failures:
                    self.log.warning('Multiple consecutive upload failures, attempting TV reconnect...')
                    await self.reconnect_tv()
                    await asyncio.sleep(5)
                    consecutive_failures = 0
                    # Skip to next file after reconnect
                    continue
                    
                base_name = os.path.basename(filename)
                self.update_uploaded_files(
                    base_name,
                    content_id,
                    full_path=path,
                    matte_id=matte_for_upload,
                )
                cache_key = path_rel_for_matte or base_name
                if self.uploaded_files.get(cache_key, {}).get('content_id'):
                    self.log.info('uploaded : {} to tv as {}'.format(display_name, self.uploaded_files[cache_key]['content_id']))
                    uploaded_content_ids.append(self.uploaded_files[cache_key]['content_id'])
                    uploaded_count += 1
                else:
                    self.log.warning('file: {} failed to upload'.format(display_name))
                self.write_program_data()
                # Add delay between uploads to let TV process
                if idx < len(filenames) - 1:
                    await asyncio.sleep(upload_delay)
        await self._refresh_uploaded_image_dates(uploaded_content_ids)
        return uploaded_count
            
    async def delete_files_from_tv(self, content_ids):
        '''
        remove files from tv if tv is in art mode
        '''
        if self.tv.art_mode:
            self.log.info('removing files from tv : {}'.format(content_ids))
            await self.tv.delete_list(content_ids)
            await self.sync_file_list()

    def get_last_updated(self, filename, full_path=None):
        '''
        get last updated timestamp for file
        If full_path is provided, use it directly. Otherwise construct from self.folder + filename.
        '''
        if full_path:
            return os.path.getmtime(full_path)
        return os.path.getmtime(os.path.join(self.folder, filename))
        
    async def remove_files(self, files):
        '''
        if files deleted, remove them from tv
        '''
        # Determine which basenames are removed
        removed_basenames = [k for k in list(self.uploaded_files.keys()) if k not in files]
        content_ids_removed = [self.uploaded_files[k]['content_id'] for k in removed_basenames]
        #delete images from tv
        if content_ids_removed:
            await self.delete_files_from_tv(content_ids_removed)
            return True
        return False
            
    async def add_files(self, files):
        '''
        if new files found, upload to tv
        Limits uploads to avoid overwhelming the TV - we only need a few images for rotation
        When one or more collections are selected, uses collection-based randomization.
        '''
        max_uploads = int(os.environ.get('SAMSUNG_TV_ART_MAX_UPLOADS', '10'))

        # Account for images already on the TV (e.g. from failed/partial cleanup).
        # We want the total on-TV count (including what's already there) to stay at
        # or below max_uploads, not just blindly upload max_uploads more on top.
        already_on_tv = len([
            record for key, record in self.uploaded_files.items()
            if key not in self.exclude
        ])
        headroom = max(0, max_uploads - already_on_tv)
        if headroom == 0:
            self.log.info('TV already has %d/%d uploads; skipping add_files', already_on_tv, max_uploads)
            return 0

        # If collections are selected, always source candidates from those collections
        # (works for single and multi-collection modes).
        collections = getattr(self, 'selected_collections', [])

        # When slideshow override is active, only upload the override files (ensure they stay on TV)
        if self.slideshow_override is not None:
            uploaded_paths = {v.get('path_rel') for v in self.uploaded_files.values()}
            missing = [p for p in self.slideshow_override if p not in uploaded_paths]
            if not missing:
                return 0  # All override files already on TV; nothing to upload
            new_files = missing[:headroom]
        elif len(collections) > 0:
            new_files = await self.get_files_from_multiple_collections(collections, headroom)
        else:
            # Fallback: legacy single-folder mode when no collections are selected
            new_files = [f for f in files if f not in self.uploaded_files.keys()]
            if len(new_files) > headroom:
                self.log.info('Limiting upload from %d to %d files to protect TV', len(new_files), headroom)
                # Always pick a random sample for variety when changing collections
                new_files = random.sample(new_files, headroom)
        
        #upload new files
        if new_files:
            # Sort for sequential playback if enabled
            if self.sequential:
                new_files = sorted(new_files)
            self.log.info('adding files to tv : {}'.format(new_files))
            await self.wait_for_files(new_files)
            return await self.upload_files(new_files, progress_cb=getattr(self, '_reseed_progress_cb', None))
        return 0

    async def get_files_from_multiple_collections(self, collections, max_uploads):
        '''
        Get files evenly distributed from multiple collections.
        If max_uploads=8 and collections=2, gets 4 from each.
        Prefers files not seen in the previous slideshow; only supplements with
        previously-shown images when the fresh pool is too small to fill all slots.
        Returns list of paths relative to media_root.
        '''
        if self.bing_daily.is_daily_collection_selection(collections):
            path = self.bing_daily.current_relative_path()
            if (
                max_uploads > 0
                and path
                and os.path.isfile(os.path.join(self.media_root, path))
            ):
                return [path]
            return []

        last_paths = getattr(self, '_last_slideshow_paths', set())
        # Shuffle so remainder-based extra slots are distributed randomly each run,
        # not always biased toward the alphabetically-first collections.
        collections = list(collections)
        random.shuffle(collections)
        num_collections = len(collections)
        per_collection = max_uploads // num_collections
        remainder = max_uploads % num_collections

        self.log.info('Distributing %d uploads across %d collections (%d each, %d extra)',
                      max_uploads, num_collections, per_collection, remainder)

        # Collect fresh (not in last slideshow) and stale (were in last slideshow) separately
        fresh_by_col = []  # list of (take_count, [fresh_rel_paths])
        all_stale = []     # stale rel_paths across all collections — fallback pool

        for idx, collection in enumerate(collections):
            collection_path = os.path.join(self.media_root, collection)
            if not os.path.isdir(collection_path):
                self.log.warning('Collection directory not found: %s', collection_path)
                continue

            try:
                raw_files = [
                    f for f in os.listdir(collection_path)
                    if os.path.isfile(os.path.join(collection_path, f))
                    and self.get_file_type(os.path.join(collection_path, f)) in self.allowed_ext
                ]
                col_rel = [
                    os.path.join(collection, f)
                    for f in self.museum_labels.preferred_filenames(
                        collection_path,
                        raw_files,
                    )
                ]
            except Exception as e:
                self.log.warning('Failed to list collection %s: %s', collection, e)
                continue

            take_count = per_collection + (1 if idx < remainder else 0)

            fresh = [f for f in col_rel if f not in last_paths]
            stale = [f for f in col_rel if f in last_paths]
            random.shuffle(fresh)
            random.shuffle(stale)

            fresh_by_col.append((take_count, fresh))
            all_stale.extend(stale)
            self.log.info('Collection %s: %d fresh, %d stale (of %d total)',
                          collection, len(fresh), len(stale), len(col_rel))

        # Fill slots from fresh files first, respecting per-collection quotas
        selected = []
        for take_count, fresh in fresh_by_col:
            selected.extend(fresh[:take_count])

        # Supplement with stale files if the fresh pool didn't fill all slots
        deficit = max_uploads - len(selected)
        if deficit > 0 and all_stale:
            random.shuffle(all_stale)
            selected.extend(all_stale[:deficit])
            self.log.info('Supplementing with %d previously-shown file(s) (fresh pool was short)',
                          min(deficit, len(all_stale)))

        # Safety filter: never re-upload something already on the TV in this session
        already_uploaded = {
            record.get('path_rel', key)
            for key, record in self.uploaded_files.items()
        }
        selected = [f for f in selected if f not in already_uploaded]

        self.log.info('Total files selected for upload: %d', len(selected))
        return selected

    _PREVIEW_IMAGE_EXT = frozenset(('.jpg', '.jpeg', '.png', '.bmp', '.tif', '.tiff'))

    def _preview_random_selection(self, collections, max_n):
        """Return a randomly-picked list of up to max_n path_rel values from
        the given collections, using the same fresh-first logic as
        get_files_from_multiple_collections but without filtering out files
        that are already on the TV (this is a dry-run preview).

        Uses a fast extension check instead of PIL get_file_type() so that
        this method is safe to call from the synchronous paho MQTT callback
        thread — PIL disk I/O would block paho's network loop and delay the
        ack publish by as long as it takes to read every file header.
        """
        last_paths = getattr(self, '_last_slideshow_paths', set())
        collections = list(collections)
        num_collections = len(collections)
        if num_collections == 0:
            return []

        # Build one candidate per collection (prefer fresh images within each collection),
        # then shuffle the per-collection candidates so any max_n of the num_collections
        # collections can contribute equally regardless of collection size or disk order.
        # Overflow images (beyond the first per collection) go into spill pools used to
        # fill any remaining deficit after the per-collection pass.
        candidates_fresh = []  # best (fresh) candidate from each collection
        candidates_stale = []  # best (stale) candidate from collections with no fresh images
        spill_fresh = []       # additional fresh images beyond the first per collection
        spill_stale = []       # all stale images
        total_found = 0

        for collection in collections:
            collection_path = os.path.join(self.media_root, collection)
            if not os.path.isdir(collection_path):
                continue
            try:
                # Use cached listing from _publish_slideshow_available if available
                # so the first shuffle click doesn't block paho with a cold disk scan.
                raw_files = getattr(self, '_collection_file_cache', {}).get(collection_path)
                if raw_files is None:
                    raw_files = os.listdir(collection_path)
                col_rel = [
                    os.path.join(collection, f)
                    for f in raw_files
                    if os.path.isfile(os.path.join(collection_path, f))
                    and os.path.splitext(f)[1].lower() in self._PREVIEW_IMAGE_EXT
                ]
            except Exception:
                continue

            total_found += len(col_rel)
            fresh = [f for f in col_rel if f not in last_paths]
            stale = [f for f in col_rel if f in last_paths]
            random.shuffle(fresh)
            random.shuffle(stale)

            if fresh:
                candidates_fresh.append(fresh[0])
                spill_fresh.extend(fresh[1:])
                spill_stale.extend(stale)
            elif stale:
                candidates_stale.append(stale[0])
                spill_stale.extend(stale[1:])

        # Shuffle so the cut of max_n collections is uniformly random
        random.shuffle(candidates_fresh)
        random.shuffle(candidates_stale)

        # Fresh-first: prefer fresh candidates, supplement with stale candidates, then spill
        selected = (candidates_fresh + candidates_stale)[:max_n]
        deficit = max_n - len(selected)
        if deficit > 0 and spill_fresh:
            random.shuffle(spill_fresh)
            selected.extend(spill_fresh[:deficit])
            deficit = max_n - len(selected)
        if deficit > 0 and spill_stale:
            random.shuffle(spill_stale)
            selected.extend(spill_stale[:deficit])

        self.log.info('Preview result: %d/%d images (total_available=%d across %d collections)',
                      len(selected), max_n, total_found, num_collections)
        return selected

    async def update_files(self, files):
        '''
        check if files were modified
        if so, delete old content on tv and upload new
        '''
        modified_files = [f for f in files if f in self.uploaded_files.keys() and self.uploaded_files[f].get('modified') != self.get_last_updated(f)]
        #delete old file and upload new:
        if modified_files:
            self.log.info('updating files on tv : {}'.format(modified_files))
            await self.wait_for_files(modified_files)
            files_to_delete = [v['content_id'] for k, v in self.uploaded_files.items() if k in modified_files]
            await self.delete_files_from_tv(files_to_delete)
            await self.upload_files(modified_files)
            return True
        return False

    async def wait_for_files(self, files):
        #wait for files to arrive
        await asyncio.sleep(min(10, 5 * len(files)))
            
    async def update_art_timer(self):
        '''
        changes art on tv as part of slideshow if enabled
        updates favourites list if favourites are included in slideshow
        '''
        if self.update_time > 0 and (len(self.uploaded_files.keys()) > 1 or self.include_fav):
            if time.time() - self.start >= self.update_time:
                self.log.info('doing slideshow update, after {}'.format(self.get_time(self.update_time)))
                self.start = time.time()
                self.write_program_data()
                if self.include_fav:
                    self.log.info('updating favourites')
                    fav = await self.get_tv_content('MY-C0004')
                    self.fav = set(fav) if fav is not None else self.fav
                await self.change_art()
            else:
                self.log.info('next {} update in {}'.format('sequential' if self.sequential else 'random', self.get_time(self.update_time - (time.time() - self.start))))
                
    def get_content_ids(self):
        '''
        return list of all content ids available for selecting to display NOTE sets() are not ordered
        if not including favourites, order list by filename in self.uploaded_files
        When a slideshow override is active, only returns content_ids for the override paths.
        '''
        if self.slideshow_override is not None:
            # Override mode: return content_ids in override order for paths currently on the TV
            result = []
            for path in self.slideshow_override:
                rec = self.uploaded_files.get(path)
                if rec is None:
                    base = os.path.basename(path)
                    rec = next(
                        (
                            candidate
                            for key, candidate in self.uploaded_files.items()
                            if key == base and candidate.get('path_rel', key) == base
                        ),
                        None,
                    )
                cid = rec.get('content_id') if rec else None
                if cid and cid not in result:
                    result.append(cid)
            return result
        if self.fav:
            # Exclude from uploaded files and fav
            uploaded_ids = {v['content_id'] for k, v in self.uploaded_files.items() if k not in self.exclude and v['content_id'] not in self.exclude_content_ids}
            fav_ids = self.fav - set(self.exclude_content_ids)
            return list(uploaded_ids.union(fav_ids))
        return [v['content_id'] for k, v in sorted(self.uploaded_files.items()) if k not in self.exclude and v['content_id'] not in self.exclude_content_ids]
        
    def get_next_art(self):
        '''
        get next content_id from list, using shuffle-without-repeat logic.
        Shows all images once before any repeats (like shuffling a deck of cards).
        '''
        all_content_ids = self.get_content_ids()
        if not all_content_ids:
            return None
        
        # Get unshown images (excluding current)
        unshown = [cid for cid in all_content_ids if cid not in self.shown_content_ids and cid != self.current_content_id]
        
        # If all images have been shown, reset the cycle
        if not unshown:
            self.log.info('All %d images shown, starting new shuffle cycle', len(self.shown_content_ids))
            self.shown_content_ids = set()
            # Exclude only current image for the new cycle
            unshown = [cid for cid in all_content_ids if cid != self.current_content_id]
        
        if unshown:
            if self.sequential:
                # Preserve get_content_ids() order. In override mode this is the
                # exact order selected by the user; in auto mode it remains the
                # existing filename order.
                content_id = unshown[0]
            else:
                # Random: pick randomly from unshown
                content_id = random.choice(unshown)
            return content_id
        
        # Fallback: only one image exists
        return all_content_ids[0] if all_content_ids else None

    def get_filename_for_content_id(self, content_id):
        if not content_id:
            return None
        for filename, data in self.uploaded_files.items():
            if data.get('content_id') == content_id:
                return filename
        return None

    # HA REST methods removed in MQTT-only build

    async def update_ha_selected_artwork(self, content_id):
        filename = self.get_filename_for_content_id(content_id)
        if not filename:
            return
        # Resolve to full path and collection when possible
        base_name = os.path.basename(filename)
        rec = self.uploaded_files.get(filename) or self.uploaded_files.get(base_name, {})
        rel_path = rec.get('path_rel')
        full_path = os.path.join(self.media_root, rel_path) if rel_path else os.path.join(self.folder, base_name)
        collection = None
        try:
            # Prefer collection from rel_path parent folder
            if rel_path:
                parts = os.path.normpath(rel_path).split(os.sep)
                if len(parts) > 1:
                    # Join all parts except the filename to support subdir collections
                    # e.g. "Artists/Kelly_Burns/file.jpg" -> collection = "Artists/Kelly_Burns"
                    collection = os.path.join(*parts[:-1])
                elif parts:
                    collection = parts[0]
            else:
                collection = os.path.basename(os.path.dirname(full_path))
        except Exception:
            collection = None
        display_name = os.path.splitext(base_name)[0]
        # Consolidated payload
        state_obj = {"display": display_name, "file": base_name, "collection": collection}
        state_str = json.dumps(state_obj, separators=(",", ":"))
        try:
            if self.mqtt_enabled:
                self._publish_mqtt_discovery()
                self._publish_mqtt_state(display_name, base_name, collection)
        except Exception as e:
            self.log.warning('Failed to update Home Assistant selected artwork: %s', e)


    def _load_csv_metadata(self):
        """Load artwork CSV into memory for attribute publishing. Optional."""
        try:
            if not self.csv_path or not os.path.isfile(self.csv_path):
                self.log.info('CSV metadata not found at %s; attributes will be minimal', self.csv_path)
                return
            with open(self.csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self._csv_headers = list(reader.fieldnames or [])
                self._csv_by_file = {}
                self._csv_by_path = {}
                # Rebuild artist<->dir maps
                self._artist_to_dir = {}
                self._dir_to_artist = {}
                for row in reader:
                    key = (row.get('artwork_file') or '').strip()
                    if key:
                        self._csv_by_file[key] = row
                        dn = (row.get('artwork_dir') or '').strip()
                        if dn:
                            self._csv_by_path[f"{dn}/{key}"] = row
                    # Build bidirectional mapping when columns exist
                    try:
                        an = (row.get('artist_name') or '').strip()
                        cn = (row.get('collection_name') or '').strip()
                        dn = (row.get('artwork_dir') or '').strip()
                        if an and dn:
                            # Keep first-seen mapping to be stable
                            if an not in self._artist_to_dir:
                                self._artist_to_dir[an] = dn
                                spaced = an.replace('_', ' ')
                                if spaced and spaced not in self._artist_to_dir:
                                    self._artist_to_dir[spaced] = dn
                                n1 = self._normalize_collection_key(an)
                                if n1 and n1 not in self._artist_to_dir:
                                    self._artist_to_dir[n1] = dn
                                n2 = self._normalize_collection_key(spaced)
                                if n2 and n2 not in self._artist_to_dir:
                                    self._artist_to_dir[n2] = dn
                            # Also map collection_name variants -> artwork_dir
                            if cn and cn not in self._artist_to_dir:
                                self._artist_to_dir[cn] = dn
                                cn_spaced = cn.replace('_', ' ')
                                if cn_spaced and cn_spaced not in self._artist_to_dir:
                                    self._artist_to_dir[cn_spaced] = dn
                                cn_n1 = self._normalize_collection_key(cn)
                                if cn_n1 and cn_n1 not in self._artist_to_dir:
                                    self._artist_to_dir[cn_n1] = dn
                            # Prefer collection_name as the display label; fall back to artist_name
                            if dn not in self._dir_to_artist:
                                label = cn if cn else an
                                self._dir_to_artist[dn] = label.replace('_', ' ')
                    except Exception:
                        pass
            try:
                self._csv_mtime = os.path.getmtime(self.csv_path)
            except Exception:
                self._csv_mtime = None
            self.bing_daily.register_cached_metadata()
            self.log.info('Loaded CSV metadata: %d rows, %d headers', len(self._csv_by_file), len(self._csv_headers))
        except Exception as e:
            self.log.warning('Failed to load CSV metadata from %s: %s', self.csv_path, e)

    def next_value(self, value, lst):
        '''
        get next value from list, or return first element
        return None if list is empty
        '''
        return lst[(lst.index(value)+1) % len(lst)] if value in lst else lst[0] if lst else None

    async def change_art(self):
        '''
        update displayed art on tv, it next_art is a different content_id to current
        '''
        content_id = self.get_next_art()
        if content_id and content_id != self.current_content_id:
            self.log.info('selecting tv art: content_id: %s (shown %d/%d)', content_id, len(self.shown_content_ids) + 1, len(self.get_content_ids()))
            # NOTE: We do NOT call change_matte here. On this firmware the
            # matte is baked into the rendered composite at upload time and
            # change_matte is metadata-only — it never triggers a re-render.
            # The displayed matte will be whatever was baked when the image
            # was last uploaded. A changed matte becomes visible when the
            # slideshow Apply workflow detects the mismatch and reseeds.
            await self.tv.select_image(content_id)
            self.shown_content_ids.add(content_id)  # Mark as shown
            self.current_content_id = content_id
            await self.update_ha_selected_artwork(content_id)
        else:
            self.log.info('skipping art update, as new content_id: %s is the same', content_id)

    async def _apply_matte_via_reupload(self, path):
        '''Make a per-image matte override visible by uploading a replacement,
        selecting it when necessary, and then deleting the old content ID. This
        is the only path that actually produces a visible matte change on this
        firmware (change_matte alone is metadata-only).

        Raises:
          - _MatteRejectedError if the TV rejects the matte (-7); the override
            is automatically pinned to 'none' and the image is re-uploaded
            with no matte before the exception is raised.
          - RuntimeError on other failures (not in art mode, file missing,
            image not currently uploaded, empty upload response).
        '''
        if not self.tv.art_mode:
            raise RuntimeError('TV is not in art mode')
        # Locate the uploaded_files entry for this path
        info = None
        base = None
        for k, v in self.uploaded_files.items():
            if v.get('path_rel') == path or k == os.path.basename(path):
                info = v
                base = k
                break
        if not info:
            raise RuntimeError(f'Image is not currently uploaded: {path}')
        old_content_id = info.get('content_id')
        rel_path = info.get('path_rel') or path
        # Resolve the file on disk: try media_root + rel_path first, then
        # self.folder + basename for single-folder mode.
        abs_path = None
        if self.media_root:
            cand = os.path.join(self.media_root, rel_path)
            if os.path.isfile(cand):
                abs_path = cand
        if abs_path is None and self.folder:
            cand = os.path.join(self.folder, base)
            if os.path.isfile(cand):
                abs_path = cand
        if abs_path is None:
            raise RuntimeError(f'File not found on disk for path: {path}')
        matte = self._resolve_matte_for(rel_path, os.path.basename(rel_path)) or 'none'
        file_data, file_type = self.read_file(abs_path)
        if not file_data:
            raise RuntimeError(f'Failed to read file: {abs_path}')
        was_current = bool(old_content_id) and old_content_id == self.current_content_id
        # Upload the replacement before deleting the currently stored version.
        self.log.info('matte apply: re-uploading %s with matte=%s', rel_path, matte)
        rejected = False
        try:
            new_content_id = await self._upload_to_tv(file_data, file_type, matte)
        except Exception as ex:
            if matte != 'none' and self._is_matte_minus_7(ex):
                rejected = True
                new_content_id = None
            else:
                raise
        # Silent-failure: empty content_id with a non-'none' matte means the
        # TV swallowed the request — treat as rejection.
        if new_content_id is None and not rejected and matte != 'none':
            rejected = True
        if rejected:
            self.log.warning('matte apply: TV rejected matte %r for %s; pinning to none and re-uploading',
                             matte, rel_path)
            self._matte_overrides[rel_path] = 'none'
            try:
                self._save_matte_overrides()
                self._publish_matte_overrides()
            except Exception:
                pass
            try:
                new_content_id = await self._upload_to_tv(file_data, file_type, 'none')
            except Exception as ex2:
                raise RuntimeError(f'fallback upload (matte=none) failed: {ex2}') from ex2
            if not new_content_id:
                raise RuntimeError('fallback upload (matte=none) returned no content_id')
            self.update_uploaded_files(
                base,
                new_content_id,
                full_path=abs_path,
                matte_id='none',
            )
            self.write_program_data()
            await self._refresh_uploaded_image_dates([new_content_id])
            if old_content_id:
                self._queue_pending_delete_ids([old_content_id])
            if was_current:
                await self.tv.select_image(new_content_id)
                self.current_content_id = new_content_id
            await self._drain_pending_delete_ids()
            try:
                self._publish_slideshow_state()
            except Exception:
                pass
            raise _MatteRejectedError(matte)
        if not new_content_id:
            raise RuntimeError('upload returned no content_id')
        self.update_uploaded_files(
            base,
            new_content_id,
            full_path=abs_path,
            matte_id=matte,
        )
        self.write_program_data()
        await self._refresh_uploaded_image_dates([new_content_id])
        if old_content_id:
            self._queue_pending_delete_ids([old_content_id])
        if was_current:
            await self.tv.select_image(new_content_id)
            self.current_content_id = new_content_id
        await self._drain_pending_delete_ids()
        try:
            self._publish_slideshow_state()
        except Exception:
            pass
        return new_content_id, matte

    async def check_dir(self):
        '''
        scan folder for new, deleted or updated files, but only when tv is in art mode
        '''
        deferred_action = None
        try:
            async with self._tv_state_lock:
                if self._refresh_in_progress:
                    return
                # Refresh CSV-driven collections periodically without needing a restart
                self._maybe_reload_csv_and_publish_collections()
                selection_changed = self.apply_selection()
                update_due = (
                    not self.bing_daily.is_daily_mode()
                    and self.update_time > 0
                    and time.time() - self.start >= self.update_time
                )
                override_pending = bool(
                    self.slideshow_override_pending and self.slideshow_override
                )
                if self.selection_only and not selection_changed and not update_due and not override_pending:
                    return
                if not selection_changed and not update_due and not override_pending:
                    self.log.debug('No selection change or update due; skipping TV poll')
                    return
                if not await self._safe_in_artmode_unlocked():
                    self.log.info('artmode or tv is off')
                    return
                if override_pending:
                    deferred_action = 'override'
                elif selection_changed:
                    deferred_action = 'reseed'
                elif update_due:
                    await self.update_art_timer()
                elif len(self.get_content_ids()) == 1:
                    await self.change_art()

            if deferred_action == 'override':
                self.log.info('TV entered Art Mode; applying pending slideshow override')
                await self._apply_slideshow_override(
                    list(self.slideshow_override),
                    req_id=f'deferred_{int(time.time() * 1000)}',
                    force_reupload=self.slideshow_override_force_reupload,
                    ack_cmd=(
                        'slideshow/override/reupload'
                        if self.slideshow_override_force_reupload
                        else 'slideshow/override/set'
                    ),
                )
            elif deferred_action == 'reseed':
                self.log.info('selection changed, syncing directory: {}'.format(self.folder))
                await self._do_full_reseed()
        except Exception as e:
            self.log.warning('error in check_dir, attempting reconnect: %s', e)
            self._status_check_needed = True
            await self.reconnect_tv()

    async def select_artwork(self):
        '''
        main loop
        initialize, check directory for changed files and update
        '''
        await self.initialize()
        self._status_check_needed = True
        probe_task = None
        if self.art_status_probe_seconds > 0:
            probe_task = asyncio.create_task(self._art_liveness_loop())
        try:
            await self._select_artwork_loop()
        finally:
            if probe_task is not None:
                probe_task.cancel()
                try:
                    await probe_task
                except asyncio.CancelledError:
                    pass

    async def _select_artwork_loop(self):
        while True:
            if self._refresh_in_progress:
                await asyncio.sleep(0.25)
                continue

            await self.bing_daily.tick()

            # Startup may have happened while the Art channel was unreachable, so
            # finish the TV-dependent initialization as soon as one is live.
            if self._tv_init_pending and self._art_channel_is_live():
                await self._initialize_tv_state()

            # The liveness probe now runs on its own task so its interval is
            # independent of this loop's period and of the current art mode.
            if (
                self._status_check_needed
                or self._tv_powered_on is False
            ):
                self._artmode_event.clear()
                in_artmode = await self.safe_in_artmode()
            else:
                in_artmode = self._in_art_mode is True

            if not in_artmode:
                if self._refresh_in_progress:
                    self._status_check_needed = True
                    await asyncio.sleep(0.25)
                    continue
                status_unknown = (
                    self._tv_powered_on is None or self._in_art_mode is None
                )
                powered_off = self._tv_powered_on is False
                backoff_delay = (
                    max(5, int(os.environ.get('SAMSUNG_TV_ART_POWER_PROBE_SECONDS', '10')))
                    if powered_off
                    else self.get_backoff_delay()
                )
                if not self._not_in_artmode_logged:
                    self.log.info(
                        'TV Art Mode status is %s',
                        'unknown' if status_unknown else 'off',
                    )
                    self._not_in_artmode_logged = True
                else:
                    self.log.debug(
                        'TV Art Mode status is %s',
                        'unknown' if status_unknown else 'off',
                    )
                if self._artmode_event.is_set():
                    event_received = True
                    self.log.debug('Art mode event received — rechecking immediately')
                else:
                    event_received = False
                    try:
                        if status_unknown or powered_off:
                            await asyncio.wait_for(
                                self._artmode_event.wait(),
                                timeout=backoff_delay,
                            )
                        else:
                            await asyncio.wait_for(
                                self._artmode_event.wait(),
                                timeout=max(
                                    5,
                                    int(os.environ.get(
                                        'SAMSUNG_TV_ART_POWER_PROBE_SECONDS',
                                        '10',
                                    )),
                                ),
                            )
                        event_received = True
                        self.log.debug('Art mode event received — rechecking immediately')
                    except asyncio.TimeoutError:
                        if (
                            not status_unknown
                            and not powered_off
                            and (
                                self.tv is None
                                or self.tv.retired
                                or not self.tv.is_alive()
                            )
                        ):
                            self._status_check_needed = True
                if status_unknown or powered_off or event_received:
                    self._status_check_needed = True
                continue
            self._not_in_artmode_logged = False
            await self.check_dir()
            # Periodically republish current artwork state to keep MQTT fresh
            try:
                if self.state_refresh_seconds > 0:
                    now = time.time()
                    if now - self._last_state_publish >= self.state_refresh_seconds:
                        await self._publish_current_artwork_state(force=False)
                        self._last_state_publish = now
            except Exception:
                pass
            if self.period == 0:
                break
            if self._artmode_event.is_set():
                self._status_check_needed = True
                continue
            else:
                try:
                    await asyncio.wait_for(self._artmode_event.wait(), timeout=self.period)
                    self.log.debug('Art mode event received during idle — rechecking')
                    self._status_check_needed = True
                except asyncio.TimeoutError:
                    pass

    async def _publish_current_artwork_state(self, force=False, state_locked=False):
        """Poll current TV artwork and publish MQTT state/attributes.
        Uses uploaded_files mapping to derive filename when possible.
        """
        if not state_locked:
            await self._tv_state_lock.acquire()
        try:
            if self._refresh_in_progress:
                return
            if not self.mqtt_enabled or not self._mqtt:
                return
            try:
                cid = await self.get_current_artwork()
            except Exception:
                self._status_check_needed = True
                return
        finally:
            if not state_locked:
                self._tv_state_lock.release()
        # If nothing has changed and not forced, skip
        if cid == self.current_content_id and not force:
            # Still ensure attributes are up to date periodically
            pass
        else:
            self.current_content_id = cid
        # Derive filename and collection if known
        filename = self.get_filename_for_content_id(self.current_content_id) if self.current_content_id else None
        display = None
        collection = None
        if filename:
            display = os.path.splitext(filename)[0]
            # Try to infer collection from cached metadata
            try:
                rec = self.uploaded_files.get(filename, {})
                rel_path = rec.get('path_rel')
                if rel_path:
                    parts = os.path.normpath(rel_path).split(os.sep)
                    if len(parts) > 1:
                        collection = os.path.join(*parts[:-1])
                    elif parts:
                        collection = parts[0]
            except Exception:
                collection = None
        else:
            # Unknown content (e.g., selected outside uploader); publish sentinel values
            display = 'Unknown'
        try:
            self._publish_mqtt_discovery()
            self._publish_mqtt_state(display, filename or '', collection)
        except Exception:
            pass

    async def _do_full_reseed(self, req_id=None, skip_started_ack=False):
        """Delete TV uploads, upload a fresh randomized set, then display the first.
        Shared by the Refresh button, collection selection changes, and Update & Refresh.
        Always publishes MQTT ack progress messages so both UIs show progress for any
        trigger (button press, collection selection change, startup seeding, etc.).
        When skip_started_ack is True, skips the 'started' ack (caller already sent one).
        """
        # Always generate a req_id so acks are published regardless of how we were called.
        # Auto-triggered reseeds (selection change, startup) get a synthetic id.
        if req_id is None:
            req_id = f'auto_{int(time.time() * 1000)}'

        def ack(status, msg):
            self._publish_ack('collections/refresh', status, msg, req_id)

        async with self._tv_state_lock:
            if self._refresh_in_progress:
                ack('error', 'Another upload or refresh is already running')
                return
            self._refresh_in_progress = True
        self._publish_slideshow_state()
        previous_uploads = dict(self.uploaded_files)
        replacement_selected = False
        try:
            def _on_upload_progress(idx, total, name):
                ack('progress', f'Uploading {idx}/{total}: {os.path.basename(name)}')
            self._reseed_progress_cb = _on_upload_progress

            if skip_started_ack:
                ack('progress', 'Preparing TV for update...')
            else:
                ack('started', 'Preparing refresh...')

            # Snapshot the current selection so the next pick avoids repeating the same images
            self._last_slideshow_paths = {
                v.get('path_rel') for v in self.uploaded_files.values() if v.get('path_rel')
            }

            ack('progress', 'Uploading new artwork to TV...')
            old_content_ids = await self.get_tv_content('MY-C0002')
            if old_content_ids is None:
                raise RuntimeError('Unable to list existing TV uploads')
            self.uploaded_files = {}
            self._last_upload_attempt_count = 0
            files_added = await self.add_files([])
            attempted_uploads = getattr(self, '_last_upload_attempt_count', 0)

            if (
                attempted_uploads > 0
                and files_added == attempted_uploads
                and len(self.get_content_ids()) > 0
            ):
                self.log.info('Uploads complete, displaying first artwork')
                new_content_ids = set(self.get_content_ids())
                self._queue_pending_delete_ids(old_content_ids)
                await self._select_replacement(new_content_ids)
                replacement_selected = True
                delete_ids = [
                    content_id
                    for content_id in old_content_ids
                    if content_id not in new_content_ids
                ]
                if delete_ids:
                    ack('progress', f'Removing {len(delete_ids)} replaced upload(s) from TV...')
                await self._drain_pending_delete_ids()
                self.start = time.time()
                self.write_program_data()
                ack('ok', f'Refresh complete — {files_added} photos loaded')
            else:
                replacement_uploads = dict(self.uploaded_files)
                previous_by_path = {
                    record.get('path_rel', key): record.get('content_id')
                    for key, record in previous_uploads.items()
                }
                replaced_old_ids = {
                    previous_by_path.get(record.get('path_rel', key))
                    for key, record in replacement_uploads.items()
                    if record.get('content_id')
                }
                self._queue_pending_delete_ids(replaced_old_ids)
                self.uploaded_files = previous_uploads
                self.uploaded_files.update(replacement_uploads)
                self.write_program_data()
                ack(
                    'error',
                    f'Only {files_added} of {attempted_uploads} replacement image(s) uploaded',
                )
                return
        except Exception as e:
            if not replacement_selected:
                replacement_uploads = dict(self.uploaded_files)
                self.uploaded_files = previous_uploads
                self.uploaded_files.update(replacement_uploads)
                self.write_program_data()
            self.log.warning('Error in full reseed: %s', e)
            ack('error', f'Exception: {e}')
            raise
        finally:
            self._reseed_progress_cb = None
            self._refresh_in_progress = False
            self._publish_slideshow_state()
            self._publish_slideshow_available()

    async def _do_collections_refresh(self, req_id=None):
        """MQTT-triggered refresh: clears slideshow override then reseeds."""
        if self._refresh_in_progress or self._collections_sync_running:
            self._publish_ack(
                'collections/refresh',
                'error',
                'Another upload or refresh is already running',
                req_id,
            )
            return
        if self.bing_daily.is_daily_collection_selection(
            self.selected_collections
        ):
            await self.bing_daily.apply_selection(
                req_id=req_id,
                ack_cmd='collections/refresh',
                force_reupload=True,
            )
            return
        if self.slideshow_override is not None:
            self.slideshow_override = None
            self.slideshow_override_pending = False
            self.slideshow_override_force_reupload = False
            self._save_slideshow_override()
            self._publish_slideshow_state()
        await self._do_full_reseed(req_id=req_id)
            
async def main():
    global log
    log = logging.getLogger('Main')
    args = parseargs()
    log.info('Program Started')
    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
    log.debug('Debug mode')
    
    args.folder = os.path.normpath(args.folder)
    
    if not os.path.exists(args.folder):
        log.warning('folder {} does not exist, exiting'.format(args.folder))
        os._exit(1)
    
    # Retry initialization with backoff to avoid hammering TV on startup
    max_retries = 10
    retry_delay = 30  # Start with 30 seconds
    
    for attempt in range(max_retries):
        try:
            mon = monitor_and_display(  args.ip,
                                        args.folder,
                                        period          = args.check,
                                        update_time     = args.update,
                                        include_fav     = args.favourite,
                                        sync            = args.sync,
                                        matte           = args.matte,
                                        sequential      = args.sequential,
                                        on              = args.on,
                                        token_file      = args.token_file,
                                        exclude         = args.exclude,
                                        exclude_content_ids = args.exclude_content_ids)
            await mon.start_monitoring()
            break  # Success, exit retry loop
        except Exception as e:
            log.warning('Failed to connect to TV (attempt %d/%d): %s', attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** min(attempt, 4))  # Cap at 16x
                log.info('Waiting %d seconds before retry...', wait_time)
                await asyncio.sleep(wait_time)
            else:
                log.error('Max retries reached, giving up')
                os._exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        os._exit(1)
