#!/usr/bin/env bash
# Adds (or updates) the "github" remote for this repository.
# Safe to run repeatedly and on fresh clones on any device.
set -euo pipefail

REMOTE_NAME="github"
REMOTE_URL="https://github.com/kohlerryan/samsung-tv-art-uploader.git"

if git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
    echo "Remote '$REMOTE_NAME' already exists; updating URL to $REMOTE_URL"
    git remote set-url "$REMOTE_NAME" "$REMOTE_URL"
else
    echo "Adding remote '$REMOTE_NAME' -> $REMOTE_URL"
    git remote add "$REMOTE_NAME" "$REMOTE_URL"
fi

echo "Fetching from '$REMOTE_NAME'..."
git fetch "$REMOTE_NAME" --tags

echo "Done. Current remotes:"
git remote -v
