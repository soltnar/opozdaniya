#!/bin/bash
set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

echo "=== Saby: выгрузка истории заказов ==="
echo

read -r -p "Введите дату для сканирования (YYYY-MM-DD): " SCAN_DATE
if [[ -z "${SCAN_DATE}" ]]; then
  echo "Дата не введена."
  read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
  echo
  exit 1
fi

if ! python3 - "$SCAN_DATE" <<'PY'
import datetime
import sys

value = sys.argv[1]
try:
    datetime.datetime.strptime(value, "%Y-%m-%d")
except ValueError:
    print("Неверный формат даты. Используйте YYYY-MM-DD.")
    raise SystemExit(1)
PY
then
  read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
  echo
  exit 1
fi

if [[ ! -d ".venv" ]]; then
  echo "Создаю виртуальное окружение .venv..."
  python3 -m venv .venv || {
    echo "Не удалось создать .venv"
    read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
    echo
    exit 1
  }
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"

echo "Проверяю зависимости Python..."
if ! python - <<'PY' >/dev/null 2>&1
import openpyxl
import playwright
PY
then
  echo "Устанавливаю зависимости из requirements.txt..."
  python -m pip install -r requirements.txt || {
    echo "Ошибка установки зависимостей."
    read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
    echo
    exit 1
  }
fi

echo "Проверяю браузер Chromium для Playwright..."
if ! python - <<'PY' >/dev/null 2>&1
import os
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    path = p.chromium.executable_path
    if not path or not os.path.exists(path):
        raise SystemExit(1)
PY
then
  echo "Устанавливаю Chromium для Playwright..."
  python -m playwright install chromium || {
    echo "Ошибка установки Chromium."
    read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
    echo
    exit 1
  }
fi

echo
echo "Запускаю выгрузку за дату ${SCAN_DATE}..."
python export_delivery_statuses.py --date "${SCAN_DATE}"
STATUS=$?

echo
if [[ ${STATUS} -eq 0 ]]; then
  echo "Готово."
else
  echo "Выгрузка завершилась с ошибкой (код ${STATUS})."
fi

read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
echo
exit ${STATUS}
