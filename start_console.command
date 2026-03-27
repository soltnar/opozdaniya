#!/bin/bash
set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

HOST="127.0.0.1"
PORT="8765"
HEALTH_URL="http://${HOST}:${PORT}/api/health"

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv || exit 1
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"
python -m pip install -r requirements.txt >/dev/null 2>&1 || {
  echo "Ошибка установки зависимостей"
  exit 1
}

is_backend_up() {
  curl -fsS "$HEALTH_URL" >/dev/null 2>&1
}

# Всегда перезапускаем backend, чтобы не оставался старый процесс без актуальных API (например /api/stop)
EXISTING_PIDS="$(lsof -ti tcp:${PORT} 2>/dev/null || true)"
if [[ -n "$EXISTING_PIDS" ]]; then
  echo "Останавливаю старый backend на ${PORT}: $EXISTING_PIDS"
  kill $EXISTING_PIDS >/dev/null 2>&1 || true
  sleep 0.5
fi

nohup python webui_server.py > ".webui.log" 2>&1 &
BACK_PID=$!

for _ in {1..30}; do
  if is_backend_up; then
    break
  fi
  sleep 0.2
done

if ! is_backend_up; then
  echo "Не удалось запустить backend. Лог: $SCRIPT_DIR/.webui.log"
  if kill -0 "$BACK_PID" >/dev/null 2>&1; then
    kill "$BACK_PID" >/dev/null 2>&1 || true
  fi
  exit 1
fi

echo "Backend: $HEALTH_URL"
echo "Открываю UI..."
open "http://${HOST}:${PORT}"
