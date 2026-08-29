# syntax=docker/dockerfile:1

FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/kohlerryan/samsung-tv-art-uploader

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates git avahi-daemon avahi-utils dbus tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=secret,id=pip_index_url,required=false \
    if [ -s /run/secrets/pip_index_url ]; then \
        PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)" \
        pip install --no-cache-dir git+https://github.com/NickWaterton/samsung-tv-ws-api.git@fe95ef1d784cd32f49bf9a07ec479576574eea07 pillow paho-mqtt; \
    else \
        pip install --no-cache-dir git+https://github.com/NickWaterton/samsung-tv-ws-api.git@fe95ef1d784cd32f49bf9a07ec479576574eea07 pillow paho-mqtt; \
    fi

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-qrcode \
    && cp -a /usr/lib/python3/dist-packages/qrcode /usr/local/lib/python3.11/site-packages/ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY start.sh /app/start.sh
COPY loop/ /app/loop/
COPY scripts/ /app/scripts/
COPY assets/fonts/ /app/assets/fonts/
RUN chmod +x /app/scripts/*.sh || true

# Web UI and standby artwork baked into the image
COPY www/ /app/www/
COPY assets/standby.png /app/frame_tv_art_collections/standby.png
COPY assets/standby.png /app/standby.default.png

RUN chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]
