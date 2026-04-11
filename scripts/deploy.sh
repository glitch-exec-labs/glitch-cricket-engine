#!/usr/bin/env bash
# deploy.sh — pull latest code from GitHub and restart the bot
# Usage: ./scripts/deploy.sh
#        Can also be triggered automatically by the systemd ExecStartPre hook.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$REPO_DIR/logs/deploy.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

cd "$REPO_DIR"

log "=== Deploy started ==="

# Pull latest code from GitHub
log "Pulling latest code from origin/main..."
git fetch origin main
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    log "Already up to date ($LOCAL). No restart needed."
    exit 0
fi

log "Updating $LOCAL -> $REMOTE"
git pull --ff-only origin main

# Install any new/changed dependencies
log "Syncing dependencies..."
"$REPO_DIR/venv/bin/pip" install -q -r "$REPO_DIR/requirements.txt"

log "Deploy complete. Bot will start with updated code."
