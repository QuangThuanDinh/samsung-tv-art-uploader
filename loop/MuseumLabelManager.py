"""Generate museum-label derivatives for downloaded artwork."""

import csv
import hashlib
import json
import math
import os
import re
import tempfile
import threading
from urllib.parse import quote_plus

import qrcode
from PIL import Image, ImageDraw, ImageFont, ImageOps


DERIVATIVE_MARKER = '.museum-label'
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.tif', '.tiff'}
MANIFEST_NAME = '.museum-label-manifest.json'
RENDER_VERSION = 14


class MuseumLabelManager:
    """Render and select idempotent, metadata-aware museum-label images."""

    render_version = RENDER_VERSION

    # Ratios measured from examples/Museum_Label.png.
    DESCRIPTION_MAX_LABEL_WIDTH_RATIO = 0.254
    LABEL_HEIGHT_RATIO = 0.084
    RIGHT_MARGIN_RATIO = 0.040
    BOTTOM_MARGIN_RATIO = 0.038
    TITLE_BOTTOM_GAP_RATIO = 0.10

    def __init__(self, host=None, media_root=None, log=None):
        self.host = host
        self.media_root = media_root or getattr(host, 'media_root', '')
        parent_log = log or getattr(host, 'log', None)
        self.log = (
            parent_log.getChild('MuseumLabel')
            if parent_log is not None
            else None
        )
        self.enabled = os.environ.get(
            'SAMSUNG_TV_ART_MUSEUM_LABEL',
            'false',
        ).lower() in ('1', 'true', 'yes')
        self.font_path = os.environ.get(
            'SAMSUNG_TV_ART_MUSEUM_LABEL_FONT',
            '/app/assets/fonts/LiberationSans-Regular.ttf',
        )
        self.bold_font_path = os.environ.get(
            'SAMSUNG_TV_ART_MUSEUM_LABEL_BOLD_FONT',
            '/app/assets/fonts/LiberationSans-Bold.ttf',
        )
        self.italic_font_path = os.environ.get(
            'SAMSUNG_TV_ART_MUSEUM_LABEL_ITALIC_FONT',
            '/app/assets/fonts/LiberationSans-Italic.ttf',
        )
        self._processing_lock = threading.RLock()

    @staticmethod
    def is_derivative(filename):
        stem, _extension = os.path.splitext(str(filename or ''))
        return stem.endswith(DERIVATIVE_MARKER)

    @staticmethod
    def derivative_filename(filename):
        stem, _extension = os.path.splitext(filename)
        return f'{stem}{DERIVATIVE_MARKER}.jpg'

    @staticmethod
    def source_filename(filename):
        stem, extension = os.path.splitext(filename)
        if not stem.endswith(DERIVATIVE_MARKER):
            return filename
        return f'{stem[:-len(DERIVATIVE_MARKER)]}{extension}'

    def preferred_filenames(self, directory, filenames):
        """Expose derivatives when enabled, while never showing both variants."""
        image_names = [
            name for name in filenames
            if os.path.splitext(name)[1].lower() in IMAGE_EXTENSIONS
        ]
        sources = [name for name in image_names if not self.is_derivative(name)]
        if not self.enabled:
            return sources

        available = set(image_names)
        preferred = []
        for source in sources:
            derivative = self.derivative_filename(source)
            preferred.append(derivative if derivative in available else source)
        return preferred

    def metadata_for_path(self, relative_path, by_path, by_file):
        row = by_path.get(relative_path) or by_file.get(
            os.path.basename(relative_path),
            {},
        )
        if row or not self.is_derivative(relative_path):
            return row
        derivative_name = os.path.basename(relative_path)
        source_stem = os.path.splitext(derivative_name)[0][
            :-len(DERIVATIVE_MARKER)
        ]
        directory = os.path.dirname(relative_path).replace('\\', '/')
        prefix = f'{directory}/' if directory else ''
        for source_path, source_row in by_path.items():
            normalized_source = source_path.replace('\\', '/')
            if not normalized_source.startswith(prefix):
                continue
            if os.path.dirname(normalized_source) != directory:
                continue
            if os.path.splitext(os.path.basename(normalized_source))[0] == source_stem:
                return source_row
        for source_name, source_row in by_file.items():
            if os.path.splitext(source_name)[0] != source_stem:
                continue
            source_path = f'{directory}/{source_name}' if directory else source_name
            return by_path.get(source_path) or source_row
        return {}

    def process_image(self, source_path, metadata, destination=None):
        """Write an atomic same-size labeled JPEG and return its path."""
        if not self.enabled:
            return source_path
        if self.is_derivative(source_path):
            return source_path

        destination = destination or os.path.join(
            os.path.dirname(source_path),
            self.derivative_filename(os.path.basename(source_path)),
        )
        title, description, target = self._label_values(
            source_path,
            metadata,
        )

        with Image.open(source_path) as opened:
            image = ImageOps.exif_transpose(opened).convert('RGB')
        self._render(image, title, description, target)

        os.makedirs(os.path.dirname(destination), exist_ok=True)
        fd, temp_path = tempfile.mkstemp(
            prefix='.museum-label-',
            suffix='.jpg',
            dir=os.path.dirname(destination),
        )
        os.close(fd)
        try:
            image.save(
                temp_path,
                format='JPEG',
                quality=95,
                subsampling=0,
                optimize=False,
            )
            os.replace(temp_path, destination)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        return destination

    def _label_values(self, source_path, metadata):
        title = self._clean_text(
            metadata.get('artwork_title')
            or metadata.get('title')
            or os.path.splitext(os.path.basename(source_path))[0]
        )
        artist = self._clean_text(metadata.get('artist_name') or '')
        year = self._clean_text(metadata.get('artwork_year') or '')
        normalized_source = source_path.replace('\\', '/')
        is_bing = '/Bing_DailyWallpaper/' in f'/{normalized_source}'
        if not is_bing and (artist or year):
            description = ' / '.join(
                value for value in (artist, year) if value
            )
            query = f'{title} - {description}'
            target = (
                f'https://www.google.com/search?q={quote_plus(query)}'
            )
        else:
            description = self._clean_text(
                metadata.get('artwork_description')
                or metadata.get('copyright')
                or ''
            )
            target = self._clean_text(metadata.get('copyrightlink') or '')
            if not target:
                target = (
                    f'https://www.google.com/search?q={quote_plus(title)}'
                )
        return title, description, target

    def image_signature(self, source_path, metadata):
        return self._signature(source_path, metadata)

    def process_git_collections(self):
        """Process source images in Git-managed collection repositories."""
        if not self.enabled or not os.path.isdir(self.media_root):
            return {'processed': 0, 'unchanged': 0, 'removed': 0, 'errors': 0}

        totals = {'processed': 0, 'unchanged': 0, 'removed': 0, 'errors': 0}
        with self._processing_lock:
            for repository in sorted(os.listdir(self.media_root)):
                repository_path = os.path.join(self.media_root, repository)
                if not os.path.isdir(os.path.join(repository_path, '.git')):
                    continue
                for directory, child_dirs, filenames in os.walk(repository_path):
                    child_dirs[:] = [
                        name for name in child_dirs
                        if name != '.git' and not name.startswith('.')
                    ]
                    stats = self._process_directory(directory, filenames)
                    for key in totals:
                        totals[key] += stats[key]
        if self.log:
            self.log.info(
                'Museum Label processed=%d unchanged=%d removed=%d errors=%d',
                totals['processed'],
                totals['unchanged'],
                totals['removed'],
                totals['errors'],
            )
        return totals

    def paths_for_collections(self, collections):
        """Return preferred image paths from the requested collection folders."""
        paths = []
        root = os.path.realpath(self.media_root)
        for collection in collections:
            normalized = os.path.normpath(str(collection or '').strip())
            if (
                not normalized
                or os.path.isabs(normalized)
                or normalized == '..'
                or normalized.startswith(f'..{os.sep}')
            ):
                continue
            directory = os.path.realpath(os.path.join(root, normalized))
            if (
                os.path.commonpath((root, directory)) != root
                or not os.path.isdir(directory)
            ):
                continue
            filenames = [
                filename
                for filename in os.listdir(directory)
                if (
                    os.path.isfile(os.path.join(directory, filename))
                    and os.path.splitext(filename)[1].lower()
                    in IMAGE_EXTENSIONS
                )
            ]
            for filename in sorted(
                self.preferred_filenames(directory, filenames)
            ):
                paths.append(
                    os.path.join(normalized, filename).replace('\\', '/')
                )
        return paths

    def regenerate_paths(
        self,
        relative_paths,
        by_path,
        by_file,
        progress_callback=None,
    ):
        """Force-regenerate selected downloaded images without touching the TV."""
        if not self.enabled:
            raise RuntimeError(
                'Museum Label is disabled; set '
                'SAMSUNG_TV_ART_MUSEUM_LABEL=true and restart'
            )

        generated = []
        skipped = []
        errors = []
        total = len(relative_paths)
        if self.log:
            self.log.info(
                'Museum Label generation started for %d selected image(s)',
                total,
            )
        with self._processing_lock:
            for index, relative_path in enumerate(relative_paths, start=1):
                try:
                    normalized, source_path = self._resolve_source(relative_path)
                    if not self._is_downloaded_collection(normalized):
                        skipped.append(normalized)
                        continue
                    if self.log:
                        self.log.info(
                            'Museum Label generating %d/%d: %s',
                            index,
                            total,
                            normalized,
                        )
                    metadata = self.metadata_for_path(
                        str(relative_path).replace('\\', '/'),
                        by_path,
                        by_file,
                    )
                    if not metadata:
                        metadata = self.metadata_for_path(
                            normalized,
                            by_path,
                            by_file,
                        )
                    destination = self.process_image(source_path, metadata)
                    destination_name = os.path.basename(destination)
                    output_relative = os.path.join(
                        os.path.dirname(normalized),
                        destination_name,
                    ).replace('\\', '/')
                    signature = self._signature(source_path, metadata)
                    self._update_manifest(
                        os.path.dirname(source_path),
                        os.path.basename(source_path),
                        destination_name,
                        signature,
                    )
                    generated.append({
                        'source_path': os.path.relpath(
                            source_path,
                            self.media_root,
                        ).replace('\\', '/'),
                        'path': output_relative,
                        'signature': signature,
                        'modified': int(os.path.getmtime(destination)),
                    })
                except Exception as exc:
                    if self.log:
                        self.log.warning(
                            'Museum Label failed %d/%d: %s: %s',
                            index,
                            total,
                            relative_path,
                            exc,
                        )
                    errors.append({
                        'path': str(relative_path),
                        'error': str(exc),
                    })
                if progress_callback:
                    progress_callback(index, total)
        if self.log:
            self.log.info(
                'Museum Label generation finished: generated=%d skipped=%d '
                'errors=%d',
                len(generated),
                len(skipped),
                len(errors),
            )
        return {
            'generated': generated,
            'skipped': skipped,
            'errors': errors,
        }

    def _resolve_source(self, relative_path):
        normalized = os.path.normpath(str(relative_path or ''))
        if (
            not normalized
            or os.path.isabs(normalized)
            or normalized == '..'
            or normalized.startswith(f'..{os.sep}')
        ):
            raise ValueError('Invalid artwork path')
        candidate = os.path.realpath(os.path.join(self.media_root, normalized))
        root = os.path.realpath(self.media_root)
        if os.path.commonpath((root, candidate)) != root:
            raise ValueError('Artwork path escapes the media directory')

        if not self.is_derivative(candidate):
            if not os.path.isfile(candidate):
                raise FileNotFoundError(f'Artwork source not found: {normalized}')
            return normalized.replace('\\', '/'), candidate

        derivative_stem = os.path.splitext(os.path.basename(candidate))[0]
        source_stem = derivative_stem[:-len(DERIVATIVE_MARKER)]
        directory = os.path.dirname(candidate)
        for filename in sorted(os.listdir(directory)):
            if (
                not self.is_derivative(filename)
                and os.path.splitext(filename)[0] == source_stem
                and os.path.splitext(filename)[1].lower() in IMAGE_EXTENSIONS
            ):
                source = os.path.join(directory, filename)
                source_relative = os.path.relpath(
                    source,
                    self.media_root,
                ).replace('\\', '/')
                return source_relative, source
        raise FileNotFoundError(f'Original artwork not found for {normalized}')

    def _is_downloaded_collection(self, relative_source):
        normalized = relative_source.replace('\\', '/')
        if normalized.startswith('Bing_DailyWallpaper/'):
            return True
        top_level = normalized.split('/', 1)[0]
        return os.path.isdir(
            os.path.join(self.media_root, top_level, '.git')
        )

    def _update_manifest(
        self,
        directory,
        source_name,
        destination_name,
        signature,
    ):
        path = os.path.join(directory, MANIFEST_NAME)
        manifest = self._load_manifest(path)
        if manifest.get('version') != RENDER_VERSION:
            manifest = {'version': RENDER_VERSION, 'images': {}}
        images = manifest.setdefault('images', {})
        images[source_name] = {
            'signature': signature,
            'output': destination_name,
        }
        self._write_json_atomic(path, manifest)

    def _process_directory(self, directory, filenames):
        sources = sorted(
            name for name in filenames
            if os.path.splitext(name)[1].lower() in IMAGE_EXTENSIONS
            and not self.is_derivative(name)
            and not name.startswith('.')
        )
        if not sources:
            return {'processed': 0, 'unchanged': 0, 'removed': 0, 'errors': 0}

        metadata = self._load_directory_metadata(directory)
        manifest_path = os.path.join(directory, MANIFEST_NAME)
        manifest = self._load_manifest(manifest_path)
        render_version_matches = manifest.get('version') == RENDER_VERSION
        next_manifest = {'version': RENDER_VERSION, 'images': {}}
        stats = {'processed': 0, 'unchanged': 0, 'removed': 0, 'errors': 0}
        processing_total = sum(
            not os.path.isfile(os.path.join(
                directory,
                self.derivative_filename(source_name),
            ))
            or not render_version_matches
            for source_name in sources
        )
        processing_index = 0
        if self.log and processing_total:
            self.log.info(
                'Museum Label processing %d image(s) in %s',
                processing_total,
                os.path.relpath(directory, self.media_root),
            )

        for source_name in sources:
            source_path = os.path.join(directory, source_name)
            destination_name = self.derivative_filename(source_name)
            destination = os.path.join(directory, destination_name)
            row = metadata.get(source_name, {})
            previous = manifest.get('images', {}).get(source_name, {})
            if os.path.isfile(destination) and render_version_matches:
                stats['unchanged'] += 1
                signature = previous.get('signature', '')
            else:
                processing_index += 1
                try:
                    if self.log:
                        self.log.info(
                            'Museum Label generating %d/%d: %s',
                            processing_index,
                            processing_total,
                            os.path.relpath(source_path, self.media_root),
                        )
                    self.process_image(source_path, row, destination)
                    signature = self._signature(source_path, row)
                    stats['processed'] += 1
                except Exception as exc:
                    stats['errors'] += 1
                    if self.log:
                        self.log.warning(
                            'Museum Label skipped %s: %s',
                            source_path,
                            exc,
                        )
                    if os.path.isfile(destination):
                        next_manifest['images'][source_name] = {
                            'signature': previous.get('signature', ''),
                            'output': destination_name,
                        }
                    continue
            next_manifest['images'][source_name] = {
                'signature': signature,
                'output': destination_name,
            }

        expected_outputs = {
            record['output'] for record in next_manifest['images'].values()
        }
        for filename in filenames:
            if (
                self.is_derivative(filename)
                and filename not in expected_outputs
                and os.path.isfile(os.path.join(directory, filename))
            ):
                os.remove(os.path.join(directory, filename))
                stats['removed'] += 1
        self._write_json_atomic(manifest_path, next_manifest)
        return stats

    def _render(self, image, title, description, target):
        width, height = image.size
        box_height = max(34, round(height * self.LABEL_HEIGHT_RATIO))
        right = width - round(width * self.RIGHT_MARGIN_RATIO)
        bottom = height - round(height * self.BOTTOM_MARGIN_RATIO)
        top = max(0, bottom - box_height)
        padding = max(4, round(box_height * 0.14))
        radius = max(2, round(box_height * 0.12))
        original_qr_size = max(20, box_height - (padding * 2))
        desired_qr_size = max(20, round(original_qr_size * 0.85))
        qr_padding = max(4, round((box_height - desired_qr_size) / 2))
        qr_size = max(20, box_height - (qr_padding * 2))

        draw = ImageDraw.Draw(image)
        title_size = max(7, round(box_height * 0.23))
        body_size = max(6, round(box_height * 0.17))
        copyright_size = max(5, round(body_size * 0.8))
        title_font = self._font(self.bold_font_path, title_size)
        body_font = self._font(self.italic_font_path, body_size)
        copyright_font = self._font(self.italic_font_path, copyright_size)
        description_text, copyright_text = self._split_copyright(description)
        fixed_width = (padding * 2.5) + qr_size + qr_padding
        description_max_box_width = max(
            96,
            round(width * self.DESCRIPTION_MAX_LABEL_WIDTH_RATIO),
        )
        description_max_width = max(
            1,
            description_max_box_width - fixed_width,
        )
        content_width = max(
            self._text_width(draw, title, title_font),
            min(
                self._text_width(draw, description_text, body_font),
                description_max_width,
            ),
            min(
                self._text_width(draw, copyright_text, copyright_font),
                description_max_width,
            ),
        )
        max_box_width = max(
            96,
            width - (round(width * self.RIGHT_MARGIN_RATIO) * 2),
        )
        box_width = self._dynamic_box_width(
            fixed_width,
            content_width,
            max_box_width,
        )
        left = max(0, right - box_width)
        draw.rounded_rectangle(
            (left, top, right, bottom),
            radius=radius,
            fill=(255, 255, 255),
        )

        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=4,
            border=0,
        )
        qr.add_data(target)
        qr.make(fit=True)
        qr_image = qr.make_image(
            fill_color='black',
            back_color='white',
        ).convert('RGB').resize((qr_size, qr_size), Image.Resampling.NEAREST)
        qr_left = right - qr_padding - qr_size
        image.paste(qr_image, (qr_left, top + qr_padding))

        text_left = left + (padding * 1.5)
        text_right = qr_left - padding
        text_width = max(1, text_right - text_left)
        description_fits = (
            self._text_width(draw, description_text, body_font) <= text_width
        )
        lines = [(
            self._fit_line(draw, title, title_font, text_width),
            title_font,
            'title',
            (20, 20, 20),
        )]
        if description_fits:
            lines.append((
                self._fit_line(draw, description_text, body_font, text_width),
                body_font,
                (
                    'description_before_copyright'
                    if copyright_text
                    else 'description'
                ),
                (20, 20, 20),
            ))
            if copyright_text:
                lines.append((
                    self._fit_line(
                        draw,
                        copyright_text,
                        copyright_font,
                        text_width,
                    ),
                    copyright_font,
                    'copyright',
                    (110, 110, 110),
                ))
        else:
            description_lines = self._description_lines(
                draw,
                description_text,
                body_font,
                text_width,
                count=2,
            )
            lines.extend(
                (
                    line,
                    body_font,
                    'description',
                    (20, 20, 20),
                )
                for line in description_lines
            )
        line_gap = max(0, round(box_height * 0.01))
        title_bottom_gap = max(2, round(box_height * self.TITLE_BOTTOM_GAP_RATIO))
        description_bottom_gap = max(2, round(box_height * 0.06))
        y = top + padding
        for line, font, role, color in lines:
            draw.text((text_left, y), line, fill=color, font=font)
            y += self._line_height(draw, font)
            if role == 'title':
                y += title_bottom_gap
            elif role == 'description_before_copyright':
                y += description_bottom_gap
            else:
                y += line_gap

    def _font(self, path, size):
        try:
            return ImageFont.truetype(path, size=size)
        except OSError:
            return ImageFont.load_default()

    @staticmethod
    def _line_height(draw, font):
        bounds = draw.textbbox((0, 0), 'Ag', font=font)
        return max(1, bounds[3] - bounds[1])

    def _description_lines(self, draw, text, font, max_width, count):
        words = text.split()
        lines = []
        while words and len(lines) < count:
            line_words = []
            while words:
                candidate = ' '.join(line_words + [words[0]])
                if line_words and self._text_width(draw, candidate, font) > max_width:
                    break
                line_words.append(words.pop(0))
            line = ' '.join(line_words)
            if len(lines) == count - 1 and words:
                line = f"{line} {' '.join(words)}".strip()
                words = []
            lines.append(self._fit_line(draw, line, font, max_width))
        return lines + [''] * (count - len(lines))

    @staticmethod
    def _split_copyright(text):
        match = re.search(
            r'(\(©[^)]*\)|\(\(c\)[^)]*\)|\(c\)\s+.+)$',
            text.strip(),
            flags=re.IGNORECASE,
        )
        if not match:
            return text, ''
        copyright_text = match.group(1).replace('(', '').replace(')', '').strip()
        return text[:match.start()].strip(), copyright_text

    @staticmethod
    def _text_width(draw, text, font):
        bounds = draw.textbbox((0, 0), text, font=font)
        return bounds[2] - bounds[0]

    @staticmethod
    def _dynamic_box_width(fixed_width, content_width, max_box_width):
        return min(
            max_box_width,
            max(96, math.ceil(fixed_width + content_width)),
        )

    def _fit_line(self, draw, text, font, max_width):
        text = text.strip()
        if self._text_width(draw, text, font) <= max_width:
            return text
        suffix = '...'
        while text and self._text_width(draw, text + suffix, font) > max_width:
            text = text[:-1].rstrip()
        return text + suffix if text else ''

    @staticmethod
    def _clean_text(value):
        return ' '.join(str(value or '').replace('\x00', '').split())

    def _signature(self, source_path, metadata):
        digest = hashlib.sha256()
        with open(source_path, 'rb') as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b''):
                digest.update(chunk)
        render_data = {
            'version': RENDER_VERSION,
            'title': metadata.get('artwork_title') or metadata.get('title') or '',
            'description': (
                metadata.get('artwork_description')
                or metadata.get('copyright')
                or ''
            ),
            'copyrightlink': metadata.get('copyrightlink') or '',
            'artist_name': metadata.get('artist_name') or '',
            'artwork_year': metadata.get('artwork_year') or '',
        }
        digest.update(json.dumps(render_data, sort_keys=True).encode('utf-8'))
        return digest.hexdigest()

    @staticmethod
    def _load_directory_metadata(directory):
        csv_paths = sorted(
            os.path.join(directory, name)
            for name in os.listdir(directory)
            if name.lower().endswith('.csv')
            and os.path.isfile(os.path.join(directory, name))
        )
        if not csv_paths:
            return {}
        rows = {}
        with open(csv_paths[0], 'r', encoding='utf-8-sig', newline='') as source:
            for row in csv.DictReader(source):
                filename = str(
                    row.get('artwork_file')
                    or row.get('file')
                    or row.get('filename')
                    or ''
                ).strip()
                if filename:
                    rows[filename] = row
        return rows

    @staticmethod
    def _load_manifest(path):
        try:
            with open(path, 'r', encoding='utf-8') as source:
                value = json.load(source)
            return value if isinstance(value, dict) else {}
        except (FileNotFoundError, OSError, ValueError):
            return {}

    @staticmethod
    def _write_json_atomic(path, value):
        fd, temp_path = tempfile.mkstemp(
            prefix='.museum-label-manifest-',
            suffix='.json',
            dir=os.path.dirname(path),
            text=True,
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as output:
                json.dump(value, output, ensure_ascii=False, indent=2)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


def main():
    import logging
    import sys

    logging.basicConfig(level=logging.INFO)
    root = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
        'SAMSUNG_TV_ART_MEDIA_ROOT',
        '/app/frame_tv_art_collections',
    )
    manager = MuseumLabelManager(
        media_root=root,
        log=logging.getLogger('MuseumLabel'),
    )
    result = manager.process_git_collections()
    print(
        'Museum Label: '
        f"processed={result['processed']} "
        f"unchanged={result['unchanged']} "
        f"removed={result['removed']}"
    )


if __name__ == '__main__':
    main()
