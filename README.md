# Локальный сервис выгрузки истории статусов заказов Saby

Скрипт выгружает:
- реестр заказов со страницы `https://rest.saby.ru/page/delivery` в статусе `Done` за выбранную дату;
- дату/время смен каждого статуса из истории заказа;
- результат в Excel (`.xlsx`).

## Что использует
- Python 3.11+
- Playwright (локальный браузер Chromium)
- openpyxl (запись Excel)
- HAR-шаблон запросов (ваш файл)

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## Запуск

```bash
python export_delivery_statuses.py --date 2026-03-25
```

Или в 1 клик на macOS:
- запустите файл [run_export.command](/Users/macbook/Documents/История заказа/run_export.command)
- скрипт спросит дату, подготовит окружение и запустит выгрузку

По умолчанию:
- HAR: `/Users/macbook/Downloads/rest.saby.ru history.har`
- Excel: `exports/order_status_history_2026-03-25.xlsx`
- профиль браузера: `.saby_profile`

Если сессии нет, скрипт откроет браузер и попросит войти вручную.

## Полезные параметры

```bash
python export_delivery_statuses.py \
  --date 2026-03-25 \
  --har "/Users/macbook/Downloads/rest.saby.ru history.har" \
  --output "/Users/macbook/Documents/История заказа/exports/my_report.xlsx" \
  --order-page-limit 25 \
  --history-page-limit 24
```

Параметры для отладки:
- `--max-orders 5`
- `--max-order-pages 2`
- `--max-history-pages 2`
- `--headless`

## Веб-версия

Запуск локальной веб-консоли:

```bash
./run_web.command
```

После старта откройте:

```text
http://127.0.0.1:8765
```

Быстрый вход через файл:
- откройте [index.html](/Users/macbook/Documents/История заказа/index.html) двойным кликом
- если backend не запущен, сначала запустите `run_web.command`

Запуск в один файл (рекомендовано):
- запустите [start_console.command](/Users/macbook/Documents/История заказа/start_console.command)
- он сам поднимет backend и откроет UI

В веб-консоли:
- выбираете дату;
- запускаете выгрузку;
- видите поток логов и итоговый путь к файлу.

## Сборка Windows `.exe` через GitHub Actions

В репозиторий добавлен workflow:

- `.github/workflows/build-windows-exe.yml`

Что он собирает:

- `saby_export_console.exe` (веб-консоль)
- `export_delivery_statuses.exe` (worker-экспорт)

Как получить готовый файл:

1. Загрузите проект в GitHub-репозиторий (ветка `main`).
2. Откройте вкладку `Actions` -> workflow `Build Windows EXE`.
3. Нажмите `Run workflow`.
4. После завершения скачайте artifact `saby_export_console_windows`.

Внутри будет архив `saby_export_console_windows.zip` с готовыми `.exe`.

Запуск на Windows:

1. Обязательно распакуйте архив целиком в папку.
2. Запускайте `start_windows_console.bat` (рекомендовано) или `saby_export_console.exe`.
3. Если окно закрывается, откройте `console.log` в той же папке.

Отдельный файл версии интерфейса:
- `web/version.js`

Журнал изменений:
- `versions.md`

## Листы в Excel
- `Реестр`: список найденных заказов (id, key, номер, клиент, сумма и т.д.)
- `Статусы`: события вида `Изменен статус заказа: "..." -> "..."` с точным временем
