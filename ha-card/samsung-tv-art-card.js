/**
 * Frame TV Art Card v0.4.2
 *
 * Viewer-only card. Displays the currently selected artwork with metadata
 * (title / artist / year / medium / description) and a "TV not in art mode"
 * compact state.
 *
 * Configuration (refresh / sync / collection selection / slideshow controls)
 * is handled exclusively by the standalone Samsung TV Art Uploader web UI.
 * The cog icon opens that web UI in a new tab — point `web_ui_url` at it.
 */

class FrameTVArtCard extends HTMLElement {
  constructor() {
    super();
    this._config = {};
    this._hass = null;
    this._lastStateHash = '';
  }

  setConfig(config) {
    this._config = {
      title: config.title || 'Frame TV Art',
      icon: config.icon || 'mdi:palette',
      // Entity exposing current artwork (file + metadata + in_art_mode attribute)
      selected_artwork_file_entity:
        config.selected_artwork_file_entity || 'sensor.frame_tv_art_selected_artwork',
      // Base path for thumbnail images (mirrors of the on-disk collections)
      image_path: config.image_path || '/local/images/frame_tv_art_collections',
      // URL of the standalone Samsung TV Art Uploader web UI. The cog opens this.
      web_ui_url: config.web_ui_url || 'http://samsung-tv-art.local:8080',
      // 'fixed' = 16:9 compressed; 'dynamic' = grows with content
      layout_mode: config.layout_mode || 'fixed',
      ...config,
    };
    this._lastStateHash = '';
  }

  _getBaseImagePath() {
    return this._config.image_path || '';
  }

  set hass(hass) {
    this._hass = hass;
    const newHash = this._getStateHash();
    if (newHash === this._lastStateHash) return;
    this._lastStateHash = newHash;
    this._render();
  }

  _getStateHash() {
    const file = this._getSelectedData().file || '';
    const artAttrs = this._getAttrs(this._config.selected_artwork_file_entity);
    const inArtMode = artAttrs ? String(artAttrs.in_art_mode) : 'true';
    return `${file}|${inArtMode}`;
  }

  _getState(entityId) {
    if (!this._hass || !this._hass.states[entityId]) return '';
    return this._hass.states[entityId].state;
  }

  _getAttrs(entityId) {
    if (!this._hass || !this._hass.states[entityId]) return {};
    return this._hass.states[entityId].attributes || {};
  }

  _getSelectedData() {
    const entityId = this._config.selected_artwork_file_entity;
    const raw = this._getState(entityId);
    const attrs = this._getAttrs(entityId);
    if (attrs && (attrs.file || attrs.display || attrs.collection)) {
      return {
        file: attrs.file || '',
        display: attrs.display || null,
        collection: attrs.collection || null,
      };
    }
    if (!raw || raw === 'unknown' || raw === 'unavailable') {
      return { file: '', display: null, collection: null };
    }
    const trimmed = (raw || '').trim();
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
      try {
        const obj = JSON.parse(trimmed);
        return {
          file: obj.file || '',
          display: obj.display || null,
          collection: obj.collection || null,
        };
      } catch (_) {}
    }
    return { file: trimmed, display: null, collection: null };
  }

  _parseArtworkInfo(file) {
    if (!file || file === 'unknown' || file === 'unavailable' || file === 'None' || file === '') {
      return null;
    }
    const raw = file.replace(/\.(jpg|jpeg|png|gif|bmp|webp)$/i, '');
    let artist = null;
    let title = raw;
    let year = null;
    const underscoreParts = raw.split('_');
    if (underscoreParts.length >= 3) {
      const possibleYear = underscoreParts[1];
      if (/^\d{4}$/.test(possibleYear) && parseInt(possibleYear) >= 1000 && parseInt(possibleYear) <= 2100) {
        artist = underscoreParts[0].replace(/_/g, ' ');
        year = possibleYear;
        title = underscoreParts.slice(2).join(' ').replace(/_/g, ' ');
      } else {
        title = raw.replace(/_/g, ' ');
      }
    } else {
      title = raw;
      const yearMatch = title.match(/\((\d{4})\)/);
      if (yearMatch) {
        year = yearMatch[1];
        title = title.replace(/\s*\(\d{4}\)/, '');
      }
      const artistMatch = title.match(/^(.+?)\s*[-–—]\s*(.+)$/);
      if (artistMatch) {
        artist = artistMatch[1].trim();
        title = artistMatch[2].trim();
      }
    }
    return { artist, title, year };
  }

  _escapeHtml(text) {
    if (text == null) return '';
    return String(text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  _formatInline(text) {
    let s = this._escapeHtml(text);
    s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    s = s.replace(/\*(.+?)\*/g, '<strong>$1</strong>');
    s = s.replace(/_(.+?)_/g, '<em>$1</em>');
    s = s.replace(/`([^`]+)`/g, '<code>$1</code>');
    return s;
  }

  _formatMultiline(text) {
    return this._formatInline(text).replace(/\r?\n/g, '<br>');
  }

  _getBackgroundUrl() {
    const entityId = this._config.selected_artwork_file_entity;
    const { file } = this._getSelectedData();
    const attrs = this._getAttrs(entityId);
    if (!file || file === 'unknown' || file === 'unavailable' || file === 'None' || file === '') return null;
    if (attrs && attrs.artwork_dir) {
      return `${this._getBaseImagePath()}/${encodeURIComponent(attrs.artwork_dir)}/${encodeURIComponent(file)}`;
    }
    if (attrs && attrs.collection) {
      return `${this._getBaseImagePath()}/${encodeURIComponent(attrs.collection)}/${encodeURIComponent(file)}`;
    }
    if (attrs && attrs.artist_name) {
      return `${this._getBaseImagePath()}/${encodeURIComponent(attrs.artist_name)}/${encodeURIComponent(file)}`;
    }
    const match = file.match(/^(.+?)_[^_]+_/);
    if (match) {
      return `${this._getBaseImagePath()}/${encodeURIComponent(match[1])}/${encodeURIComponent(file)}`;
    }
    return null;
  }

  _buildArtworkText(file, isUnavailable) {
    const attrs = this._getAttrs(this._config.selected_artwork_file_entity);
    const normalizedFile = String(file || '').trim().toLowerCase();
    const unavailable =
      isUnavailable || !normalizedFile || normalizedFile === 'unknown' ||
      normalizedFile === 'unavailable' || normalizedFile === 'none';

    if (isUnavailable && attrs && attrs.in_art_mode === false) {
      return 'TV is not in art mode';
    }
    if (unavailable) {
      return 'Artwork is unavailable';
    }

    const title = attrs.artwork_title || null;
    const year = attrs.artwork_year || null;
    const artist = attrs.artist_name || null;
    const lifespan = attrs.artist_lifespan || null;
    const medium = attrs.artwork_medium || attrs.artist_medium || null;
    const descriptionRaw = attrs.artwork_description;
    const description = (typeof descriptionRaw === 'string') ? descriptionRaw.trim() : '';
    const hasDescription = !!(description && !/^\s*(null|none|n\/a)\s*$/i.test(description) && description !== (file || ''));

    if (title || artist || year || lifespan || medium || hasDescription) {
      const titleText = title || file || 'Selected Artwork';
      const topLine = (artist || lifespan) ? `
        <div style="line-height:1.3; margin-top: 2px;">
          ${artist ? `<span style="font-size:1.1em; font-weight:bold; color: white;">${this._formatInline(artist)}</span>` : ''}
          ${lifespan ? `<span style="font-size:0.9em; color: rgba(255,255,255,0.7);"> (${lifespan})</span>` : ''}
        </div>
      ` : '';
      const middleLine = `
        <div style="line-height:1.3; margin-top: 8px;"><em style="font-size:1.1em; color: white;">${this._formatInline(titleText)}</em>${year ? `<span style="font-size:0.9em; color: rgba(255,255,255,0.7);">, ${year}</span>` : ''}</div>
      `;
      const bottomLine = medium ? `
        <div style="font-size:0.9em; color: rgba(255,255,255,0.7); line-height:1.4; margin-top: 4px;">${this._formatInline(medium)}</div>
      ` : '';
      let out = `${topLine}${middleLine}${bottomLine}`;
      if (hasDescription) {
        out += `<hr style="border: none; border-top: 1px solid rgba(255,255,255,0.3); margin: 8px 0; width: calc(100% + 20px); margin-left: -10px;"><div style="text-align: justify; color: rgba(255,255,255,0.7); font-size: 0.9em; line-height: 1.4;">${this._formatMultiline(description)}</div>`;
      }
      return out;
    }

    // Fallback: parse filename
    const info = this._parseArtworkInfo(file);
    const fTitle = (info && info.title) || file || 'Selected Artwork';
    const fYear = (info && info.year) || null;
    const fArtist = (info && info.artist) || null;
    const topLine = fArtist ? `
      <div style="line-height:1.3; margin-top: 2px;">
        <span style="font-size:1.1em; font-weight:bold; color: white;">${this._formatInline(fArtist)}</span>
      </div>
    ` : '';
    const middleLine = `
      <div style="line-height:1.3; margin-top: 8px;">
        <em style="font-size:1.1em; color: white;">${this._formatInline(fTitle)}</em>
        ${fYear ? `<span style="font-size:0.9em; color: rgba(255,255,255,0.7);">, ${fYear}</span>` : ''}
      </div>
    `;
    return `${topLine}${middleLine}`;
  }

  _syncInfoFade() {
    const wrapEl = this.querySelector('.ftv-progress-wrap');
    const fadeEl = this.querySelector('.ftv-info-fade');
    if (!wrapEl) return;
    requestAnimationFrame(() => {
      const overflow = wrapEl.scrollHeight > wrapEl.clientHeight + 2;
      if (fadeEl) fadeEl.style.display = overflow ? '' : 'none';
      wrapEl.style.cursor = overflow ? 'pointer' : '';
      wrapEl.dataset.overflows = overflow ? '1' : '';
      if (overflow && !wrapEl._ftv_scroll_bound) {
        wrapEl._ftv_scroll_bound = true;
        wrapEl.addEventListener('scroll', () => {
          if (!fadeEl) return;
          const atBottom = wrapEl.scrollTop + wrapEl.clientHeight >= wrapEl.scrollHeight - 4;
          fadeEl.style.display = atBottom ? 'none' : '';
        });
      }
    });
  }

  _showInfoOverlay() {
    const infoDiv = this.querySelector('.ftv-info');
    const wrapEl = this.querySelector('.ftv-progress-wrap');
    if (!infoDiv) return;
    if (wrapEl) wrapEl.style.background = 'transparent';

    const overlay = document.createElement('div');
    overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;z-index:99999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.45);';
    const panel = document.createElement('div');
    panel.style.cssText = 'position:relative;background:rgba(12,12,12,0.94);border:1px solid rgba(255,255,255,0.15);border-radius:12px;padding:20px 20px 16px;max-width:480px;width:90%;max-height:80vh;overflow-y:auto;color:white;box-shadow:0 12px 40px rgba(0,0,0,0.7);';
    const close = document.createElement('button');
    close.innerHTML = '&times;';
    close.style.cssText = 'position:absolute;top:10px;right:12px;background:transparent;border:none;color:rgba(255,255,255,0.5);font-size:22px;cursor:pointer;line-height:1;padding:0;';
    panel.innerHTML = infoDiv.innerHTML;
    panel.prepend(close);
    overlay.appendChild(panel);
    const dismiss = () => {
      try { document.body.removeChild(overlay); } catch (_) {}
      if (wrapEl) wrapEl.style.background = '';
    };
    close.addEventListener('click', (e) => { e.stopPropagation(); dismiss(); });
    overlay.addEventListener('click', (e) => { if (e.target === overlay) dismiss(); });
    document.body.appendChild(overlay);
  }

  _render() {
    if (!this._hass) return;
    const { file } = this._getSelectedData();
    const normalizedFile = String(file || '').trim().toLowerCase();
    const artworkAttrs = this._getAttrs(this._config.selected_artwork_file_entity);
    const inArtMode = artworkAttrs && artworkAttrs.in_art_mode !== undefined
      ? artworkAttrs.in_art_mode !== false
      : true;
    const isNotInArtMode = !inArtMode;
    const isUnavailable =
      isNotInArtMode || !normalizedFile ||
      normalizedFile === 'unknown' || normalizedFile === 'unavailable' || normalizedFile === 'none';
    const bgUrl = this._getBackgroundUrl();
    const hasArtwork = bgUrl !== null;
    const isCompressed = (this._config.layout_mode || 'fixed') !== 'dynamic';
    const artworkText = this._buildArtworkText(file, isUnavailable);

    this.style.setProperty('--ha-card-border-radius', '21px');
    if (isNotInArtMode) this.style.setProperty('--ha-card-box-shadow', 'none');

    this.innerHTML = `
      <ha-card>
        <style>
          ha-card { overflow: visible; }
          .ftv-card {
            padding: 12px;
            position: relative;
            border-radius: 21px;
            ${isCompressed && !isNotInArtMode ? 'aspect-ratio: 16/9; display: flex; flex-direction: column; box-sizing: border-box;' : ''}
            ${isNotInArtMode ? '' : hasArtwork ? `background: linear-gradient(rgba(0,0,0,0.1), rgba(0,0,0,0.1)), url("${bgUrl}"); background-size: cover; background-position: center;` : ''}
          }
          .ftv-header {
            ${isNotInArtMode ? `
            display: grid;
            grid-template-areas: "i n" "i l";
            grid-template-columns: min-content auto;
            grid-template-rows: min-content min-content;
            padding: 0;
            ` : `
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 0;
            margin-bottom: 12px;
            ${hasArtwork ? 'color: white;' : ''}
            `}
          }
          .ftv-title-wrap { display: flex; flex-direction: column; line-height: 1.3; }
          .ftv-title-wrap > span {
            font-size: 14px; font-weight: bold; color: var(--primary-text-color);
          }
          .ftv-name {
            grid-area: n;
            font-size: 1em; font-weight: 500;
            color: var(--primary-text-color);
            align-self: end; justify-self: start;
            margin-left: 6px; margin-bottom: 2px; line-height: 1.1;
          }
          .ftv-subtitle {
            grid-area: l;
            font-size: 0.85em; font-weight: bolder; filter: opacity(40%);
            align-self: start; justify-self: start;
            margin-left: 6px; margin-top: 0px; line-height: 1.1;
          }
          .ftv-header .spacer { flex: 1; }
          .ftv-gear {
            margin-left: auto;
            background: transparent; border: none;
            cursor: pointer; color: inherit;
            display: inline-flex; align-items: center; justify-content: center;
            width: 32px; height: 32px; border-radius: 6px;
          }
          .ftv-gear:hover { background: rgba(255,255,255,0.12); }
          .ftv-icon-wrap {
            width: 42px; height: 42px; border-radius: 50%;
            background: ${isNotInArtMode ? 'rgba(var(--color-theme),0.05)' : 'rgba(var(--rgb-primary-color, 3, 169, 244), 0.2)'};
            display: flex; align-items: center; justify-content: center;
            flex-shrink: 0;
            ${isNotInArtMode ? 'grid-area: i; place-self: center;' : ''}
          }
          .ftv-icon-wrap ha-icon {
            --mdc-icon-size: 24px;
            color: ${isNotInArtMode ? 'rgba(var(--color-theme),0.2)' : 'var(--primary-color, #03a9f4)'};
          }
          .ftv-progress-wrap {
            ${hasArtwork ? 'background: rgba(0,0,0,0.55); border-radius: 8px; transition: background 0.25s;' : ''}
            ${isCompressed ? 'flex: 1; min-height: 0; position: relative;' : (hasArtwork ? 'overflow: hidden;' : '')}
            ${isUnavailable ? 'display: flex; flex-direction: column; overflow-y: hidden; padding-bottom: 8px;' : isCompressed ? 'overflow-y: auto; padding-bottom: 8px;' : ''}
          }
          .ftv-info {
            display: block; width: 100%; box-sizing: border-box; padding: 12px;
            ${isUnavailable ? 'flex: 0 0 auto;' : ''}
            ${hasArtwork ? 'color: white;' : ''}
          }
          ${isCompressed ? `
          .ftv-info-fade {
            display: none; position: absolute;
            bottom: 0; left: 0; right: 0; height: 48px;
            background: linear-gradient(to bottom, transparent, ${hasArtwork && !isNotInArtMode ? 'rgba(0,0,0,0.85)' : 'var(--ha-card-background-color, var(--card-background-color, #fff))'});
            pointer-events: none;
          }` : ''}
        </style>
        <div class="ftv-card">
          <div class="ftv-header">
            <div class="ftv-icon-wrap">
              <ha-icon icon="${this._config.icon}"></ha-icon>
            </div>
            ${isNotInArtMode ? `
            <span class="ftv-name">${this._config.title}</span>
            <span class="ftv-subtitle">TV is not in art mode</span>
            ` : `
            <div class="ftv-title-wrap">
              <span>${this._config.title}</span>
            </div>
            <span class="spacer"></span>
            <button class="ftv-gear" id="ftv-gear" title="${this._config.web_ui_url ? 'Open Samsung TV Art Uploader' : 'Settings (configure web_ui_url to enable)'}">
              <ha-icon icon="mdi:cog"></ha-icon>
            </button>
            `}
          </div>
          ${isNotInArtMode ? '' : `
          <div class="ftv-progress-wrap">
            <div class="ftv-info">${artworkText}</div>
            ${isCompressed ? '<div class="ftv-info-fade"></div>' : ''}
          </div>
          `}
        </div>
      </ha-card>
    `;

    // Tap progress-wrap to open overflow overlay (fixed/compressed mode only)
    if (isCompressed) {
      const progressWrap = this.querySelector('.ftv-progress-wrap');
      if (progressWrap) {
        progressWrap.addEventListener('click', () => {
          if (progressWrap.dataset.overflows) this._showInfoOverlay();
        });
      }
    }

    // Cog → open web UI in a new tab
    const gear = this.querySelector('#ftv-gear');
    if (gear) {
      gear.addEventListener('click', (e) => {
        e.stopPropagation();
        const url = this._config.web_ui_url;
        if (!url) {
          // eslint-disable-next-line no-console
          console.warn('[frame-tv-art-card] No web_ui_url configured; cog click ignored.');
          return;
        }
        window.open(url, '_blank', 'noopener,noreferrer');
      });
    }

    this._syncInfoFade();
  }
}

console.info('%c FRAME-TV-ART-CARD %c v0.4.2 ', 'color: white; background: #03a9f4; font-weight: bold;', '');

try {
  if (!customElements.get('frame-tv-art-card')) {
    customElements.define('frame-tv-art-card', FrameTVArtCard);
  }
} catch (e) {
  console.warn('Failed to register frame-tv-art-card:', e);
}
