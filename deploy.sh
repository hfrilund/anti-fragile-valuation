#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/afv"
SERVICE="afv-api"

log() { echo "[deploy] $*"; }

cd "$APP_DIR"

log "Pulling latest code…"
git pull --ff-only

log "Updating Python dependencies…"
source venv/bin/activate
pip install -q -r requirements.txt

log "Running DB migrations…"
python main.py db update-schema

log "Building UI…"
cd ui
npm ci --silent
npm run build
cd "$APP_DIR"

log "Restarting $SERVICE…"
sudo systemctl restart "$SERVICE"
sudo systemctl is-active --quiet "$SERVICE" && log "Service is running." || { echo "[deploy] ERROR: service failed to start"; sudo systemctl status "$SERVICE" --no-pager; exit 1; }

log "Done."
