# syntax=docker/dockerfile:1

FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/kohlerryan/samsung-tv-art-uploader

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git avahi-daemon avahi-utils dbus \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=secret,id=pip_index_url,required=false \
    if [ -s /run/secrets/pip_index_url ]; then \
        PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)" \
        pip install --no-cache-dir git+https://github.com/NickWaterton/samsung-tv-ws-api.git pillow paho-mqtt; \
    else \
        pip install --no-cache-dir git+https://github.com/NickWaterton/samsung-tv-ws-api.git pillow paho-mqtt; \
    fi

WORKDIR /app
COPY start.sh /app/start.sh
COPY serve.py /app/serve.py
COPY standy_util.py /app/standy_util.py
COPY mqtt_integration.py /app/mqtt_integration.py
COPY pil_methods.py /app/pil_methods.py
COPY uploader.py /app/uploader.py
COPY scripts/ /app/scripts/
RUN chmod +x /app/scripts/*.sh || true

# Web UI and standby artwork baked into the image
COPY www/ /app/www/
COPY assets/standby.png /app/frame_tv_art_collections/standby.png
COPY assets/standby.png /app/standby.default.png

RUN chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]
