#!/bin/bash
# ═══════════════════════════════════════════════════════════
# IPL Edge Spotter — New Server Setup Script
# Run this on the NEW server after transferring the archive
# ═══════════════════════════════════════════════════════════

set -e

ARCHIVE_PATH="${1:-/home/support/ipl_bot_migration_*.tar.gz}"
BOT_DIR="/home/support/workspace/ipl_bot"

echo "═══════════════════════════════════════════════"
echo "  IPL Edge Spotter — New Server Setup"
echo "═══════════════════════════════════════════════"
echo ""

# 0. Check archive exists
ARCHIVE=$(ls -t $ARCHIVE_PATH 2>/dev/null | head -1)
if [ -z "$ARCHIVE" ]; then
    echo "ERROR: No archive found at $ARCHIVE_PATH"
    echo "Usage: ./migrate_setup.sh /path/to/ipl_bot_migration_*.tar.gz"
    exit 1
fi
echo "Using archive: $ARCHIVE"
echo ""

# 1. Install system dependencies
echo "[1/7] Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-venv python3-pip sqlite3
echo "  Python version: $(python3 --version)"

# 2. Extract archive
echo "[2/7] Extracting archive..."
mkdir -p /home/support/workspace
cd /home/support/workspace
tar -xzf "$ARCHIVE"
echo "  Extracted to $BOT_DIR"

# 3. Create virtual environment
echo "[3/7] Creating virtual environment..."
cd "$BOT_DIR"
python3 -m venv venv
echo "  venv created"

# 4. Install Python packages
echo "[4/7] Installing Python packages..."
venv/bin/pip install --upgrade pip -q
venv/bin/pip install -r requirements.txt -q
echo "  Installed $(venv/bin/pip freeze | wc -l) packages"

# 5. Create log directory
echo "[5/7] Setting up directories..."
mkdir -p logs
touch logs/spotter.err.log logs/spotter.out.log
touch logs/liveline.err.log logs/liveline.out.log
chmod 666 logs/*.log
echo "  Logs directory ready"

# 6. Install systemd services
echo "[6/7] Installing systemd services..."
if [ -f systemd/ipl-bot.service ]; then
    # Update paths if username is different
    CURRENT_USER=$(whoami)
    sed -i "s|User=support|User=$CURRENT_USER|g" systemd/ipl-bot.service
    sed -i "s|/home/support|/home/$CURRENT_USER|g" systemd/ipl-bot.service
    sudo cp systemd/ipl-bot.service /etc/systemd/system/
    echo "  ipl-bot.service installed"
fi
if [ -f systemd/ipl-liveline.service ]; then
    sed -i "s|User=support|User=$CURRENT_USER|g" systemd/ipl-liveline.service
    sed -i "s|/home/support|/home/$CURRENT_USER|g" systemd/ipl-liveline.service
    sudo cp systemd/ipl-liveline.service /etc/systemd/system/
    echo "  ipl-liveline.service installed"
fi
sudo systemctl daemon-reload
echo "  systemd reloaded"

# 7. Test the bot
echo "[7/7] Testing bot..."
cd "$BOT_DIR"
COMPILE_OK=$(python3 -c "
import py_compile, sys
errors = 0
for f in ['spotter.py', 'config.py', 'modules/bet_executor.py', 'modules/predictor.py']:
    try:
        py_compile.compile(f, doraise=True)
    except:
        errors += 1
        print(f'  FAIL: {f}')
print(f'{errors} compile errors')
sys.exit(errors)
" 2>&1)
echo "  $COMPILE_OK"

echo ""
echo "═══════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  Start the bot:"
echo "    sudo systemctl start ipl-bot.service"
echo "    sudo systemctl enable ipl-bot.service"
echo ""
echo "  Start liveline listener:"
echo "    sudo systemctl start ipl-liveline.service"
echo "    sudo systemctl enable ipl-liveline.service"
echo ""
echo "  Check status:"
echo "    sudo systemctl status ipl-bot.service"
echo "    tail -f logs/spotter.err.log"
echo ""
echo "  IMPORTANT: If Telethon session needs re-auth:"
echo "    cd $BOT_DIR && venv/bin/python auth_liveline.py"
echo "═══════════════════════════════════════════════"
