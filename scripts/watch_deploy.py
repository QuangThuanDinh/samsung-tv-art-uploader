#!/usr/bin/env python3
"""Cross-platform watch-and-deploy helper.

Replaces the bash/inotifywait/rsync/entr one-liners so the VS Code "Watch &
Deploy" tasks work identically on Linux, macOS and Windows. It depends only on
the Python standard library plus an SSH client:

  * rsync (preferred, used automatically when found on PATH), or
  * scp + ssh (the OpenSSH client shipped with Windows 10/11 and macOS).

All connection/runtime values come from the VS Code task, which passes them in
from `${config:...}` settings (i.e. your per-machine .vscode/settings.json).

Examples
--------
Python sources (sync over SSH, then restart a container):

  python scripts/watch_deploy.py --label python \
    --host HOST --port 22 --remote-path /srv/app \
    --restart-container art-uploader --docker-bin /usr/local/bin/docker \
    --sync-dir loop/:loop/

Static assets (sync over SSH, no restart):

  python scripts/watch_deploy.py --label assets \
    --host HOST --port 22 --remote-path /srv/app \
    --watch www/index.html www/favicon.png

Local copy (e.g. a Home Assistant www folder), including a directory tree:

  python scripts/watch_deploy.py --label card \
    --local-dest /path/to/ha/www/community/card/ \
    --watch ha-card/samsung-tv-art-card.js ha-card/README.md \
    --sync-dir ha-card/images/:images/
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path, PurePosixPath


def log(label: str, msg: str) -> None:
    print(f"[deploy:{label}] {msg}", flush=True)


def has(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watch files and deploy on change.")
    p.add_argument("--label", default="deploy", help="Short name used in log output.")
    p.add_argument("--watch", nargs="+", default=[], help="Files (relative to repo root) to watch + sync.")
    p.add_argument("--sync-dir", action="append", default=[],
                   help="Extra directory to sync as SRC:DEST (DEST relative to destination root). Repeatable.")
    p.add_argument("--debounce", type=float, default=2.0, help="Seconds of quiet before deploying.")
    p.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds.")
    # Remote (SSH) destination
    p.add_argument("--host", help="SSH host (omit for a local-only copy).")
    p.add_argument("--port", default="22", help="SSH port.")
    p.add_argument("--remote-path", help="Destination path on the remote host.")
    p.add_argument("--restart-container", help="If set, restart this Docker container after deploy.")
    p.add_argument("--docker-bin", default="docker", help="Path to the docker binary on the remote host.")
    # Local destination
    p.add_argument("--local-dest", help="Local destination directory (instead of --host/--remote-path).")
    p.add_argument("--once", action="store_true", help="Deploy once and exit (no watching).")
    return p.parse_args()


def snapshot(paths: list[Path]) -> dict[str, float]:
    """Map every existing watched path (files, and files within dirs) to its mtime."""
    state: dict[str, float] = {}
    for path in paths:
        if path.is_dir():
            for child in path.rglob("*"):
                if child.is_file():
                    try:
                        state[str(child)] = child.stat().st_mtime
                    except OSError:
                        pass
        elif path.is_file():
            try:
                state[str(path)] = path.stat().st_mtime
            except OSError:
                pass
    return state


def ssh_cmd(port: str) -> list[str]:
    return ["ssh", "-p", str(port)]


def run(cmd: list[str], label: str) -> bool:
    log(label, "$ " + " ".join(cmd))
    try:
        completed = subprocess.run(cmd)
    except FileNotFoundError as exc:
        log(label, f"command not found: {exc}")
        return False
    if completed.returncode != 0:
        log(label, f"command failed (exit {completed.returncode})")
        return False
    return True


def sync_remote(args: argparse.Namespace, files: list[Path], sync_dirs: list[tuple[Path, str]]) -> bool:
    host = args.host
    remote_path = args.remote_path.rstrip("/")
    ok = True

    if has("rsync"):
        # rsync -R preserves the given relative path (e.g. www/index.html -> remote/www/index.html).
        ssh = " ".join(ssh_cmd(args.port))
        if files:
            cmd = ["rsync", "-avR", "-e", ssh, *[str(f) for f in files], f"{host}:{remote_path}/"]
            ok = run(cmd, args.label) and ok
        for src, dest in sync_dirs:
            cmd = ["rsync", "-av", "-e", ssh,
                   str(src).rstrip("/") + "/", f"{host}:{remote_path}/{dest}".rstrip("/") + "/"]
            ok = run(cmd, args.label) and ok
        return ok

    # Fallback: plain OpenSSH scp + ssh mkdir -p (works natively on Windows 10/11).
    log(args.label, "rsync not found; using scp fallback")
    rel_dirs: set[str] = set()
    for f in files:
        rel_dirs.add(PurePosixPath(f.as_posix()).parent.as_posix())
    for _src, dest in sync_dirs:
        rel_dirs.add(PurePosixPath(dest).as_posix())
    mkdirs = " ".join(sorted(f"'{remote_path}/{d}'" for d in rel_dirs if d not in (".", "")))
    if mkdirs:
        ok = run([*ssh_cmd(args.port), host, f"mkdir -p {mkdirs}"], args.label) and ok
    for f in files:
        rel = f.as_posix()
        ok = run(["scp", "-O", "-P", str(args.port), str(f), f"{host}:{remote_path}/{rel}"], args.label) and ok
    for src, dest in sync_dirs:
        ok = run(["scp", "-O", "-P", str(args.port), "-r", str(src).rstrip("/") + "/.",
                  f"{host}:{remote_path}/{dest}".rstrip("/")], args.label) and ok
    return ok


def sync_local(args: argparse.Namespace, files: list[Path], sync_dirs: list[tuple[Path, str]]) -> bool:
    dest_root = Path(args.local_dest)
    dest_root.mkdir(parents=True, exist_ok=True)
    for f in files:
        target = dest_root / f.name
        shutil.copy2(f, target)
        log(args.label, f"copied {f} -> {target}")
    for src, dest in sync_dirs:
        target_dir = dest_root / dest
        if src.is_dir():
            shutil.copytree(src, target_dir, dirs_exist_ok=True)
            log(args.label, f"copied tree {src} -> {target_dir}")
    return True


def deploy(args: argparse.Namespace, files: list[Path], sync_dirs: list[tuple[Path, str]]) -> None:
    log(args.label, "change settled, deploying…")
    if args.local_dest:
        ok = sync_local(args, files, sync_dirs)
    else:
        ok = sync_remote(args, files, sync_dirs)
    if ok and args.restart_container and not args.local_dest:
        run([*ssh_cmd(args.port), args.host,
             f"{args.docker_bin} restart {args.restart_container}"], args.label)
    log(args.label, "done" if ok else "finished with errors")


def main() -> int:
    args = parse_args()

    # Resolve relative to the repo root (the script lives in scripts/).
    repo_root = Path(__file__).resolve().parent.parent
    os.chdir(repo_root)

    files = [Path(w) for w in args.watch]
    sync_dirs: list[tuple[Path, str]] = []
    for spec in args.sync_dir:
        if ":" not in spec:
            log(args.label, f"ignoring malformed --sync-dir '{spec}' (expected SRC:DEST)")
            continue
        src, dest = spec.split(":", 1)
        sync_dirs.append((Path(src), dest))

    if not args.local_dest and not (args.host and args.remote_path):
        log(args.label, "error: provide either --local-dest or both --host and --remote-path")
        return 2

    watched = files + [src for src, _ in sync_dirs]
    if not watched:
        log(args.label, "error: nothing to watch (use --watch and/or --sync-dir)")
        return 2

    if args.once:
        deploy(args, files, sync_dirs)
        return 0

    log(args.label, "watching " + ", ".join(str(p) for p in watched))
    prev = snapshot(watched)
    pending_since: float | None = None
    while True:
        time.sleep(args.interval)
        current = snapshot(watched)
        if current != prev:
            prev = current
            pending_since = time.monotonic()
            continue
        if pending_since is not None and (time.monotonic() - pending_since) >= args.debounce:
            pending_since = None
            deploy(args, files, sync_dirs)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
