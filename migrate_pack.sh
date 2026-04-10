#!/bin/bash
# ═══════════════════════════════════════════════════════════
# IPL Edge Spotter — Migration Pack Script
# Run this on the OLD server to create a transfer archive
# ═══════════════════════════════════════════════════════════

set -e

BOT_DIR="/home/support/workspace/ipl_bot"
ARCHIVE_NAME="ipl_bot_migration_$(date +%Y%m%d_%H%M%S).tar.gz"
ARCHIVE_PATH="/home/support/${ARCHIVE_NAME}"

echo "═══════════════════════════════════════════════"
echo "  IPL Edge Spotter — Migration Packer"
echo "═══════════════════════════════════════════════"
echo ""

# 1. Stop the bot safely
echo "[1/5] Stopping bot services..."
sudo systemctl stop ipl-bot.service 2>/dev/null || true
sudo systemctl stop ipl-liveline.service 2>/dev/null || true
sleep 3
echo "  Done — services stopped"

# 2. Save pip requirements
echo "[2/5] Saving pip requirements..."
cd "$BOT_DIR"
venv/bin/pip freeze > requirements.txt
echo "  Saved $(wc -l < requirements.txt) packages to requirements.txt"

# 3. Save systemd service files
echo "[3/5] Saving systemd service files..."
mkdir -p "$BOT_DIR/systemd"
cp /etc/systemd/system/ipl-bot.service "$BOT_DIR/systemd/" 2>/dev/null || true
cp /etc/systemd/system/ipl-liveline.service "$BOT_DIR/systemd/" 2>/dev/null || true
echo "  Saved service files to systemd/"

# 4. Create archive (exclude venv, __pycache__, large logs)
echo "[4/5] Creating archive..."
cd /home/support/workspace
tar -czf "$ARCHIVE_PATH" \
    --exclude='ipl_bot/venv' \
    --exclude='ipl_bot/__pycache__' \
    --exclude='ipl_bot/modules/__pycache__' \
    --exclude='ipl_bot/series/__pycache__' \
    --exclude='ipl_bot/logs/spotter.err.log' \
    --exclude='ipl_bot/logs/spotter.out.log' \
    --exclude='ipl_bot/logs/liveline.err.log' \
    --exclude='ipl_bot/logs/liveline.out.log' \
    ipl_bot/

ARCHIVE_SIZE=$(du -h "$ARCHIVE_PATH" | cut -f1)
echo "  Archive: $ARCHIVE_PATH ($ARCHIVE_SIZE)"

# 5. Restart bot on old server (so it keeps running while you migrate)
echo "[5/5] Restarting bot on old server..."
sudo systemctl start ipl-bot.service 2>/dev/null || true
sudo systemctl start ipl-liveline.service 2>/dev/null || true
echo "  Bot restarted on old server"

echo ""
echo "═══════════════════════════════════════════════"
echo "  Archive ready: $ARCHIVE_PATH"
echo ""
echo "  Transfer to new server:"
echo "    gcloud compute scp $ARCHIVE_PATH NEW_INSTANCE:/home/support/"
echo "  OR:"
echo "    scp $ARCHIVE_PATH user@NEW_SERVER_IP:/home/support/"
echo "═══════════════════════════════════════════════"
