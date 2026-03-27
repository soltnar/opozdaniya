#!/bin/bash
set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv || exit 1
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"
python -m pip install -r requirements.txt || exit 1

# Гарантированно освобождаем порт backend, чтобы не остался старый процесс без /api/stop
EXISTING_PIDS="$(lsof -ti tcp:8765 2>/dev/null || true)"
if [[ -n "$EXISTING_PIDS" ]]; then
  echo "Останавливаю старый backend на 8765: $EXISTING_PIDS"
  kill $EXISTING_PIDS >/dev/null 2>&1 || true
  sleep 0.5
fi

echo "Web UI: http://127.0.0.1:8765"
echo "Остановить: Ctrl+C"
python webui_server.py
