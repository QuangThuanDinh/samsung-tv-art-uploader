#!/usr/bin/env python3

import json
import os


def _resolve_media_file(media_root, standby_path, allowed_extensions, relative_path):
    if not relative_path:
        return None
    candidate = os.path.realpath(os.path.join(media_root, relative_path))
    try:
        if os.path.commonpath([media_root, candidate]) != media_root:
            return None
    except ValueError:
        return None
    if candidate == standby_path or not os.path.isfile(candidate):
        return None
    extension = os.path.splitext(candidate)[1].lower().lstrip('.')
    return candidate if extension in allowed_extensions else None


def _resolve_source(
    media_root,
    standby_path,
    allowed_extensions,
    selected_collections,
    cached_selected_collections,
    slideshow_override,
    preferred_paths,
    map_collection,
):
    explicit_paths = list(preferred_paths) if preferred_paths else list(slideshow_override or [])
    for relative_path in explicit_paths:
        candidate = _resolve_media_file(
            media_root, standby_path, allowed_extensions, relative_path
        )
        if candidate:
            return candidate

    collections = list(selected_collections or cached_selected_collections)
    if not collections:
        return None
    collection = map_collection(collections[0]) or collections[0]
    collection_path = os.path.realpath(os.path.join(media_root, collection))
    try:
        if os.path.commonpath([media_root, collection_path]) != media_root:
            return None
    except ValueError:
        return None
    if not os.path.isdir(collection_path):
        return None

    for name in sorted(os.listdir(collection_path), key=str.casefold):
        candidate = _resolve_media_file(
            media_root,
            standby_path,
            allowed_extensions,
            os.path.join(collection, name),
        )
        if candidate:
            return candidate
    return None


def refresh_dynamic_standby(
    *,
    enabled,
    media_root,
    standby,
    state_path,
    allowed_extensions,
    selected_collections,
    cached_selected_collections,
    slideshow_override,
    preferred_paths,
    map_collection,
    log,
):
    """Generate standby as a PNG from the first selected media image.

    Returns True only when the standby file was replaced.
    """
    if not enabled:
        return False
    try:
        from PIL import Image
    except ImportError:
        log.warning('Standby: dynamic generation requires Pillow; using existing image')
        return False
    if not media_root or not standby:
        return False

    media_root = os.path.realpath(media_root)
    standby_path = os.path.realpath(
        standby if os.path.isabs(standby) else os.path.join(media_root, standby)
    )
    source_path = _resolve_source(
        media_root,
        standby_path,
        frozenset(allowed_extensions),
        selected_collections,
        cached_selected_collections,
        slideshow_override,
        preferred_paths,
        map_collection,
    )
    if not source_path:
        log.info('Standby: no selected media image is available; using the existing fallback')
        return False

    stat = os.stat(source_path)
    signature = {
        'source': os.path.relpath(source_path, media_root),
        'size': stat.st_size,
        'mtime_ns': stat.st_mtime_ns,
    }
    try:
        with open(state_path, 'r', encoding='utf-8') as state_file:
            previous_signature = json.load(state_file)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        previous_signature = None
    if previous_signature == signature and os.path.isfile(standby_path):
        log.info('Standby: selected source is unchanged; reusing %s', signature['source'])
        return False

    log.info(
        'Standby: generating %s from selected image %s',
        os.path.relpath(standby_path, media_root),
        signature['source'],
    )
    os.makedirs(os.path.dirname(standby_path), exist_ok=True)
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    standby_tmp = standby_path + '.tmp'
    state_tmp = state_path + '.tmp'
    try:
        with Image.open(source_path) as source:
            source.convert('RGB').save(standby_tmp, format='PNG', optimize=True)
        os.replace(standby_tmp, standby_path)
        with open(state_tmp, 'w', encoding='utf-8') as state_file:
            json.dump(signature, state_file)
        os.replace(state_tmp, state_path)
    finally:
        for temp_path in (standby_tmp, state_tmp):
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass

    log.info('Standby: PNG ready at %s', standby_path)
    return True
