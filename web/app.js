const dateInput = document.getElementById('scan-date');
const runBtn = document.getElementById('run-btn');
const stopBtn = document.getElementById('stop-btn');
const downloadBtn = document.getElementById('download-btn');
const downloadPdfBtn = document.getElementById('download-pdf-btn');
const clearBtn = document.getElementById('clear-btn');
const copyBtn = document.getElementById('copy-btn');
const toggleLogBtn = document.getElementById('toggle-log-btn');
const restaurantFilterEl = document.getElementById('restaurant-filter');
const sortFilterEl = document.getElementById('sort-filter');
const statusEl = document.getElementById('status');
const logBox = document.getElementById('log-box');
const resultEl = document.getElementById('result');
const versionEl = document.getElementById('version');

const analyticsPanel = document.getElementById('analytics-panel');
const refreshAnalyticsBtn = document.getElementById('refresh-analytics-btn');
const analyticsMetaEl = document.getElementById('analytics-meta');
const kpiGridEl = document.getElementById('kpi-grid');
const stageBarsEl = document.getElementById('stage-bars');
const bottleneckBarsEl = document.getElementById('bottleneck-bars');
const loadBarsEl = document.getElementById('load-bars');
const hotspotsTableEl = document.getElementById('hotspots-table');
const restaurantTotalsTableEl = document.getElementById('restaurant-totals-table');
const problemTableEl = document.getElementById('problem-table');
const ordersTableEl = document.getElementById('orders-table');

let activeJobId = null;
let pollTimer = null;
let logOffset = 0;
let currentAnalyticsDate = null;
const SELECTED_DATE_KEY = 'saby_selected_date';
const SELECTED_RESTAURANT_KEY = 'saby_selected_restaurant';
const SELECTED_SORT_KEY = 'saby_selected_sort';

function todayIso() {
  const now = new Date();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${now.getFullYear()}-${m}-${d}`;
}

function setStatus(text, cls = '') {
  statusEl.textContent = text;
  statusEl.className = cls;
}

function setRunningUi(isRunning) {
  runBtn.disabled = isRunning;
  stopBtn.disabled = !isRunning;
  if (isRunning) {
    downloadBtn.disabled = true;
    downloadPdfBtn.disabled = true;
  }
}

function appendLogs(lines) {
  if (!lines || !lines.length) return;
  const chunk = lines.join('\n') + '\n';
  logBox.textContent += chunk;
  logBox.scrollTop = logBox.scrollHeight;
}

function showTemporaryButtonText(btn, text, timeoutMs = 1200) {
  const original = btn.textContent;
  btn.textContent = text;
  setTimeout(() => {
    btn.textContent = original;
  }, timeoutMs);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

function escapeHtml(value) {
  const text = String(value ?? '');
  return text
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function fmtNum(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return Number(value).toFixed(digits);
}

function fmtMin(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return `${Number(value).toFixed(1)} мин`;
}

function resetAnalyticsUi() {
  analyticsPanel.classList.add('hidden');
  analyticsMetaEl.textContent = '';
  kpiGridEl.innerHTML = '';
  stageBarsEl.innerHTML = '';
  bottleneckBarsEl.innerHTML = '';
  loadBarsEl.innerHTML = '';
  hotspotsTableEl.innerHTML = '';
  restaurantTotalsTableEl.innerHTML = '';
  problemTableEl.innerHTML = '';
  ordersTableEl.innerHTML = '';
  currentAnalyticsDate = null;
  downloadPdfBtn.disabled = true;
}

function selectedRestaurant() {
  return String(restaurantFilterEl?.value || '').trim();
}

function selectedSort() {
  return String(sortFilterEl?.value || 'restaurant_asc').trim();
}

function renderBars(container, rows, valueKey, maxValue, valueFormatter) {
  if (!rows || !rows.length) {
    container.innerHTML = '<div class="hint">Нет данных</div>';
    return;
  }
  const max = maxValue > 0 ? maxValue : 1;
  container.innerHTML = rows.map((row) => {
    const value = Number(row[valueKey] ?? 0);
    const width = Math.max(2, Math.min(100, (value / max) * 100));
    return `
      <div class="bar-row">
        <div class="bar-label">${escapeHtml(row.name || row.stage || '—')}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
        <div class="bar-value">${escapeHtml(valueFormatter(value, row))}</div>
      </div>
    `;
  }).join('');
}

function renderKpis(kpi, thresholds) {
  const overdueThreshold = thresholds?.overdue_total_min ?? 60;
  const overdueRate = Number(kpi?.overdue_rate ?? 0);
  const noDeliveryRate = Number(kpi?.no_delivery_stage_rate ?? 0);
  const noDeliveryCount = Number(kpi?.no_delivery_stage_count ?? 0);

  kpiGridEl.innerHTML = `
    <article class="kpi">
      <div class="label">Заказов в анализе</div>
      <div class="value">${escapeHtml(fmtNum(kpi?.orders, 0))}</div>
      <div class="sub">фактическая выборка</div>
    </article>
    <article class="kpi">
      <div class="label">Среднее время заказа (итого)</div>
      <div class="value">${escapeHtml(fmtNum(kpi?.avg_total_min, 1))}</div>
      <div class="sub">минут</div>
    </article>
    <article class="kpi">
      <div class="label">P90 времени заказа</div>
      <div class="value">${escapeHtml(fmtNum(kpi?.p90_total_min, 1))}</div>
      <div class="sub">90% заказов быстрее этого времени</div>
    </article>
    <article class="kpi">
      <div class="label">Опаздывающие заказы</div>
      <div class="value">${escapeHtml(fmtNum(kpi?.overdue_count, 0))}</div>
      <div class="sub">>${overdueThreshold} мин (${fmtNum(overdueRate, 1)}%)</div>
    </article>
    <article class="kpi">
      <div class="label">Средняя доставка / P90</div>
      <div class="value">${escapeHtml(fmtNum(kpi?.avg_delivery_min, 1))} / ${escapeHtml(fmtNum(kpi?.p90_delivery_min, 1))}</div>
      <div class="sub">минут · доставка: ${escapeHtml(fmtNum(kpi?.delivery_orders, 0))}, самовывоз: ${escapeHtml(fmtNum(kpi?.pickup_orders, 0))}</div>
    </article>
    <article class="kpi">
      <div class="label">Без этапа "Доставка"</div>
      <div class="value">${escapeHtml(fmtNum(noDeliveryCount, 0))}</div>
      <div class="sub">${escapeHtml(fmtNum(noDeliveryRate, 1))}% заказов доставки</div>
    </article>
  `;
}

function renderHotspots(rows) {
  if (!rows || !rows.length) {
    hotspotsTableEl.innerHTML = '<div class="hint">Недостаточно данных по ресторанам.</div>';
    return;
  }
  const head = `
    <table class="analytics-table">
      <thead>
        <tr>
          <th>Ресторан</th>
          <th>Заказы</th>
          <th>Avg доставка</th>
          <th>P90 доставка</th>
          <th>Доля опозданий</th>
        </tr>
      </thead>
      <tbody>
  `;
  const body = rows.map((r) => {
    const late = Number(r.late_share ?? 0);
    const lateCls = late >= 25 ? 'bad' : '';
    return `
      <tr>
        <td>${escapeHtml(r.restaurant)}</td>
        <td>${escapeHtml(fmtNum(r.orders, 0))}</td>
        <td>${escapeHtml(fmtMin(r.avg_delivery))}</td>
        <td>${escapeHtml(fmtMin(r.p90_delivery))}</td>
        <td class="${lateCls}">${escapeHtml(fmtNum(late, 1))}%</td>
      </tr>
    `;
  }).join('');
  hotspotsTableEl.innerHTML = `${head}${body}</tbody></table>`;
}

function renderRestaurantTotals(rows) {
  if (!rows || !rows.length) {
    restaurantTotalsTableEl.innerHTML = '<div class="hint">Нет данных по итогам ресторанов.</div>';
    return;
  }
  const head = `
    <table class="analytics-table">
      <thead>
        <tr>
          <th>Ресторан</th>
          <th>Заказы</th>
          <th>Опозданий</th>
          <th>Доля опозданий</th>
          <th>Avg итого</th>
          <th>P90 итого</th>
          <th>Avg обработка</th>
          <th>Avg готовка</th>
          <th>Avg сборка</th>
          <th>Avg доставка/выдача</th>
        </tr>
      </thead>
      <tbody>
  `;
  const body = rows.map((r) => {
    const lateShare = Number(r.overdue_share ?? 0);
    const lateCls = lateShare >= 25 ? 'bad' : '';
    return `
      <tr>
        <td>${escapeHtml(r.restaurant || '—')}</td>
        <td>${escapeHtml(fmtNum(r.orders, 0))}</td>
        <td>${escapeHtml(fmtNum(r.overdue_count, 0))}</td>
        <td class="${lateCls}">${escapeHtml(fmtNum(lateShare, 1))}%</td>
        <td>${escapeHtml(fmtMin(r.avg_total_min))}</td>
        <td>${escapeHtml(fmtMin(r.p90_total_min))}</td>
        <td>${escapeHtml(fmtMin(r.avg_processing_min))}</td>
        <td>${escapeHtml(fmtMin(r.avg_cooking_min))}</td>
        <td>${escapeHtml(fmtMin(r.avg_assembly_min))}</td>
        <td>${escapeHtml(fmtMin(r.avg_last_mile_min))}</td>
      </tr>
    `;
  }).join('');
  restaurantTotalsTableEl.innerHTML = `${head}${body}</tbody></table>`;
}

function renderProblems(rows, thresholds) {
  if (!rows || !rows.length) {
    problemTableEl.innerHTML = '<div class="hint">Проблемные заказы не найдены.</div>';
    return;
  }
  const overdueThreshold = Number(thresholds?.overdue_total_min ?? 60);
  const head = `
    <table class="analytics-table">
      <thead>
        <tr>
          <th>Заказ</th>
          <th>Тип</th>
          <th>Ресторан</th>
          <th>К какому времени</th>
          <th>Δ план/факт</th>
          <th>Курьер / оператор</th>
          <th>Итого</th>
          <th>Этапы (обраб/готов/сбор/дост)</th>
          <th>Узкое место</th>
          <th>Причина</th>
        </tr>
      </thead>
      <tbody>
  `;
  const body = rows.map((r) => {
    const total = Number(r.total_min ?? 0);
    const rowCls = total >= overdueThreshold ? 'late-row' : '';
    const promisedDelta = Number(r.promised_delta_min ?? 0);
    const hasPromisedDelta = r.promised_delta_min !== null && r.promised_delta_min !== undefined;
    const promisedDeltaText = hasPromisedDelta ? `${promisedDelta >= 0 ? '+' : ''}${fmtNum(promisedDelta, 1)} мин` : '—';
    const promisedDeltaCls = hasPromisedDelta && promisedDelta > 0 ? 'bad' : '';
    return `
      <tr class="${rowCls}">
        <td>${escapeHtml(r.number || r.sale || '—')}</td>
        <td>${escapeHtml(r.order_type || '—')}</td>
        <td>${escapeHtml(r.restaurant || '—')}</td>
        <td>${escapeHtml(r.promised_time || '—')}</td>
        <td class="${promisedDeltaCls}">${escapeHtml(promisedDeltaText)}</td>
        <td>${escapeHtml(r.courier || '—')} / ${escapeHtml(r.operator || '—')}</td>
        <td>${escapeHtml(fmtMin(r.total_min))}</td>
        <td>${escapeHtml(fmtNum(r.processing_min, 1))} / ${escapeHtml(fmtNum(r.cooking_min, 1))} / ${escapeHtml(fmtNum(r.assembly_min, 1))} / ${escapeHtml(fmtNum(r.delivery_min, 1))}</td>
        <td>${escapeHtml(r.bottleneck_stage || '—')}${r.bottleneck_min ? ` (${escapeHtml(fmtNum(r.bottleneck_min, 1))})` : ''}</td>
        <td>${escapeHtml(r.delay_reason || '—')}</td>
      </tr>
    `;
  }).join('');
  problemTableEl.innerHTML = `${head}${body}</tbody></table>`;
}

function renderLoadByHour(rows) {
  if (!rows || !rows.length) {
    loadBarsEl.innerHTML = '<div class="hint">Нет данных по почасовой нагрузке.</div>';
    return;
  }
  const maxCount = rows.reduce((acc, row) => Math.max(acc, Number(row.count || 0)), 0) || 1;
  loadBarsEl.innerHTML = rows.map((row) => {
    const count = Number(row.count || 0);
    const overdue = Number(row.overdue_count || 0);
    const width = Math.max(2, Math.min(100, (count / maxCount) * 100));
    const overdueShare = count ? Math.max(2, Math.min(100, (overdue / count) * 100)) : 0;
    return `
      <div class="bar-row">
        <div class="bar-label">${escapeHtml(row.hour || '—')}</div>
        <div class="bar-track load-track">
          <div class="bar-fill" style="width:${width}%"></div>
          ${overdue > 0 ? `<div class="bar-fill bad-fill" style="width:calc(${width}% * ${overdueShare / 100})"></div>` : ''}
        </div>
        <div class="bar-value">${escapeHtml(fmtNum(count, 0))} шт · просрочено ${escapeHtml(fmtNum(overdue, 0))} · avg ${escapeHtml(fmtNum(row.avg_total_min, 1))}м</div>
      </div>
    `;
  }).join('');
}

function renderOrders(rows, thresholds) {
  if (!rows || !rows.length) {
    ordersTableEl.innerHTML = '<div class="hint">Нет заказов за выбранную дату.</div>';
    return;
  }
  const overdueThreshold = Number(thresholds?.overdue_total_min ?? 60);
  const head = `
    <table class="analytics-table">
      <thead>
        <tr>
          <th>Заказ</th>
          <th>Тип</th>
          <th>Ресторан</th>
          <th>Старт</th>
          <th>К какому времени</th>
          <th>Завершен</th>
          <th>Δ план/факт, мин</th>
          <th>Итого, мин</th>
          <th>Обработка</th>
          <th>Готовка</th>
          <th>Сборка</th>
          <th>Доставка/Выдача</th>
          <th>Курьер</th>
          <th>Оператор</th>
          <th>Причина задержки</th>
        </tr>
      </thead>
      <tbody>
  `;
  const body = rows.map((r) => {
    const total = Number(r.total_min ?? 0);
    const rowCls = total > overdueThreshold ? 'late-row' : '';
    const lastStage = r.order_type === 'Самовывоз' ? r.pickup_wait_min : r.delivery_min;
    const promisedDelta = Number(r.promised_delta_min ?? 0);
    const hasPromisedDelta = r.promised_delta_min !== null && r.promised_delta_min !== undefined;
    const promisedDeltaText = hasPromisedDelta ? `${promisedDelta >= 0 ? '+' : ''}${fmtNum(promisedDelta, 1)}` : '—';
    const promisedDeltaCls = hasPromisedDelta && promisedDelta > 0 ? 'bad' : '';
    return `
      <tr class="${rowCls}">
        <td>${escapeHtml(r.number || r.sale || '—')}</td>
        <td>${escapeHtml(r.order_type || '—')}</td>
        <td>${escapeHtml(r.restaurant || '—')}</td>
        <td>${escapeHtml(r.start_time || '—')}</td>
        <td>${escapeHtml(r.promised_time || '—')}</td>
        <td>${escapeHtml(r.done_time || '—')}</td>
        <td class="${promisedDeltaCls}">${escapeHtml(promisedDeltaText)}</td>
        <td class="${total > overdueThreshold ? 'bad' : ''}">${escapeHtml(fmtNum(r.total_min, 1))}</td>
        <td>${escapeHtml(fmtNum(r.processing_min, 1))}</td>
        <td>${escapeHtml(fmtNum(r.cooking_min, 1))}</td>
        <td>${escapeHtml(fmtNum(r.assembly_min, 1))}</td>
        <td>${escapeHtml(fmtNum(lastStage, 1))}</td>
        <td>${escapeHtml(r.courier || '—')}</td>
        <td>${escapeHtml(r.operator || '—')}</td>
        <td>${escapeHtml(r.delay_reason || '—')}</td>
      </tr>
    `;
  }).join('');
  ordersTableEl.innerHTML = `${head}${body}</tbody></table>`;
}

async function loadRestaurantsForDate(dateValue) {
  const date = String(dateValue || '').trim();
  if (!date) return;
  const current = selectedRestaurant();
  try {
    const payload = await api(`/api/restaurants?date=${encodeURIComponent(date)}`);
    const list = Array.isArray(payload.restaurants) ? payload.restaurants : [];
    const preferred = localStorage.getItem(SELECTED_RESTAURANT_KEY) || current || '';
    const nextValue = list.includes(preferred) ? preferred : '';
    restaurantFilterEl.innerHTML = [
      '<option value="">Все рестораны</option>',
      ...list.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`),
    ].join('');
    restaurantFilterEl.value = nextValue;
  } catch (err) {
    appendLogs([`[web] Список ресторанов недоступен: ${err.message}`]);
  }
}

function renderAnalytics(data) {
  analyticsPanel.classList.remove('hidden');
  currentAnalyticsDate = data.date || null;
  const restaurantCaption = data.restaurant_filter || selectedRestaurant() || 'Все рестораны';
  const sortCaptionMap = {
    restaurant_asc: 'ресторан A→Я',
    restaurant_desc: 'ресторан Я→A',
    total_desc: 'дольше всего',
    promised_delta_desc: 'сильнее опоздали к плану',
    promised_time_asc: 'по плановому времени',
  };
  const sortCaption = sortCaptionMap[data.sort_mode || selectedSort()] || 'ресторан A→Я';
  const baseMeta = `Дата: ${data.date || '—'} · ресторан: ${restaurantCaption} · сортировка: ${sortCaption} · файл: ${data.output_path || '—'}`;
  analyticsMetaEl.textContent = data.notice ? `${baseMeta} · ${data.notice}` : baseMeta;

  renderKpis(data.kpi || {}, data.thresholds || {});

  const stages = Array.isArray(data.stages) ? data.stages : [];
  const stageMax = stages.reduce((acc, row) => Math.max(acc, Number(row.p90 || 0)), 0);
  renderBars(
    stageBarsEl,
    stages.map((x) => ({ ...x, name: x.name })),
    'p90',
    stageMax,
    (value, row) => `P90 ${fmtNum(value, 1)} · avg ${fmtNum(row.avg, 1)}`,
  );

  const bottlenecks = Array.isArray(data.bottlenecks) ? data.bottlenecks : [];
  const bottleneckMax = bottlenecks.reduce((acc, row) => Math.max(acc, Number(row.share || 0)), 0);
  renderBars(
    bottleneckBarsEl,
    bottlenecks,
    'share',
    bottleneckMax,
    (value, row) => `${fmtNum(value, 1)}% (${fmtNum(row.count, 0)})`,
  );

  renderLoadByHour(Array.isArray(data.load_by_hour) ? data.load_by_hour : []);

  renderHotspots(Array.isArray(data.hotspots) ? data.hotspots : []);
  renderRestaurantTotals(Array.isArray(data.restaurant_totals) ? data.restaurant_totals : []);
  renderProblems(Array.isArray(data.problem_orders) ? data.problem_orders : [], data.thresholds || {});
  renderOrders(Array.isArray(data.orders) ? data.orders : [], data.thresholds || {});
}

async function loadAnalytics(options = {}) {
  const selectedDate = String(dateInput.value || '').trim();
  const restaurant = selectedRestaurant();
  const sort = selectedSort();
  const useSelectedDate = Boolean(options.useSelectedDate) && Boolean(selectedDate);
  if (!activeJobId && !useSelectedDate) return false;
  try {
    const params = new URLSearchParams();
    if (useSelectedDate) {
      params.set('date', selectedDate);
    } else {
      params.set('job_id', String(activeJobId));
    }
    if (restaurant) {
      params.set('restaurant', restaurant);
    }
    if (sort) {
      params.set('sort', sort);
    }
    const query = params.toString();
    const payload = await api(`/api/analytics?${query}`);
    if (payload.output_path) {
      downloadPdfBtn.disabled = false;
    }
    if (useSelectedDate) {
      activeJobId = payload.job_id || activeJobId;
      if (payload.output_path) {
        resultEl.innerHTML = `Готово: <code>${payload.output_path}</code>`;
        downloadBtn.disabled = false;
        downloadPdfBtn.disabled = false;
      }
      setStatus('SUCCESS', 'success');
      setRunningUi(false);
    }
    if (restaurant) {
      localStorage.setItem(SELECTED_RESTAURANT_KEY, restaurant);
    }
    if (sort) {
      localStorage.setItem(SELECTED_SORT_KEY, sort);
    }
    renderAnalytics(payload);
    return true;
  } catch (err) {
    appendLogs([`[web] Аналитика недоступна: ${err.message}`]);
    return false;
  }
}

async function restoreLatestJob() {
  try {
    const payload = await api('/api/latest');
    const job = payload?.job;
    if (!job || !job.id) return;

    activeJobId = job.id;
    const savedDate = localStorage.getItem(SELECTED_DATE_KEY);
    if (job.date && !savedDate) {
      dateInput.value = String(job.date);
    }
    await loadRestaurantsForDate(dateInput.value);

    if (job.status === 'success') {
      setStatus('SUCCESS', 'success');
      setRunningUi(false);
      if (job.output_path) {
        resultEl.innerHTML = `Готово: <code>${job.output_path}</code>`;
        downloadBtn.disabled = false;
      }
      const loadedSelected = await loadAnalytics({ useSelectedDate: true });
      if (!loadedSelected) {
        if (job.date) {
          dateInput.value = String(job.date);
        }
        await loadAnalytics();
      }
      return;
    }

    if (job.status === 'running') {
      setStatus('RUNNING', 'running');
      setRunningUi(true);
      const data = await api(`/api/job/${job.id}?from=0`);
      logBox.textContent = '';
      appendLogs(data.logs || []);
      logOffset = data.log_size || 0;
      startPolling();
      return;
    }

    if (job.status === 'stopped') {
      setStatus('STOPPED', 'stopped');
      setRunningUi(false);
      return;
    }

    if (job.status === 'error') {
      setStatus('ERROR', 'error');
      setRunningUi(false);
      if (job.error) {
        resultEl.textContent = `Ошибка: ${job.error}`;
      }
    }
  } catch (err) {
    appendLogs([`[web] Не удалось восстановить последнюю задачу: ${err.message}`]);
  }
}

async function runExport() {
  const date = dateInput.value;
  if (!date) {
    alert('Укажите дату.');
    return;
  }

  runBtn.disabled = true;
  stopBtn.disabled = false;
  resultEl.textContent = '';
  logBox.textContent = '';
  logOffset = 0;
  downloadBtn.disabled = true;
  resetAnalyticsUi();
  setStatus('RUNNING', 'running');

  try {
    localStorage.setItem(SELECTED_DATE_KEY, date);
    const payload = await api('/api/run', {
      method: 'POST',
      body: JSON.stringify({
        date,
      }),
    });

    activeJobId = payload.job_id;
    appendLogs([`[web] Запущена задача ${activeJobId} за ${date}`]);
    startPolling();
  } catch (err) {
    setStatus('ERROR', 'error');
    appendLogs([`[web] Ошибка запуска: ${err.message}`]);
    setRunningUi(false);
  }
}

async function stopExport() {
  if (!activeJobId) return;
  stopBtn.disabled = true;
  try {
    await api('/api/stop', {
      method: 'POST',
      body: JSON.stringify({ job_id: activeJobId }),
    });
    appendLogs([`[web] Отправлен запрос остановки задачи ${activeJobId}`]);
  } catch (err) {
    appendLogs([`[web] Ошибка остановки: ${err.message}`]);
    if (String(err.message).includes('HTTP 404')) {
      appendLogs(['[web] Подсказка: backend устарел (нет /api/stop). Перезапустите UI через start_console.command или run_web.command.']);
    }
    stopBtn.disabled = false;
  }
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

function startPolling() {
  stopPolling();

  pollTimer = setInterval(async () => {
    if (!activeJobId) return;
    try {
      const data = await api(`/api/job/${activeJobId}?from=${logOffset}`);
      appendLogs(data.logs || []);
      logOffset = data.log_size || logOffset;

      if (data.status === 'success') {
        setStatus('SUCCESS', 'success');
        setRunningUi(false);
        stopPolling();
        if (data.output_path) {
          resultEl.innerHTML = `Готово: <code>${data.output_path}</code>`;
          downloadBtn.disabled = false;
        }
        await loadAnalytics();
      } else if (data.status === 'stopped') {
        setStatus('STOPPED', 'stopped');
        setRunningUi(false);
        stopPolling();
        resultEl.textContent = 'Выполнение остановлено пользователем.';
      } else if (data.status === 'error') {
        setStatus('ERROR', 'error');
        setRunningUi(false);
        stopPolling();
        if (data.error) {
          resultEl.textContent = `Ошибка: ${data.error}`;
        }
      }
    } catch (err) {
      appendLogs([`[web] Ошибка опроса: ${err.message}`]);
    }
  }, 1500);
}

runBtn.addEventListener('click', runExport);
stopBtn.addEventListener('click', stopExport);
downloadBtn.addEventListener('click', () => {
  const date = currentAnalyticsDate || String(dateInput.value || '').trim();
  if (!date) return;
  const params = new URLSearchParams();
  params.set('date', date);
  const restaurant = selectedRestaurant();
  if (restaurant) {
    params.set('restaurant', restaurant);
  }
  const sort = selectedSort();
  if (sort) {
    params.set('sort', sort);
  }
  window.open(`/api/download?${params.toString()}`, '_blank');
});
downloadPdfBtn.addEventListener('click', () => {
  const date = currentAnalyticsDate || String(dateInput.value || '').trim();
  if (!date) return;
  const params = new URLSearchParams();
  params.set('date', date);
  const restaurant = selectedRestaurant();
  if (restaurant) {
    params.set('restaurant', restaurant);
  }
  const sort = selectedSort();
  if (sort) {
    params.set('sort', sort);
  }
  window.open(`/api/report_pdf?${params.toString()}`, '_blank');
});

refreshAnalyticsBtn.addEventListener('click', () => loadAnalytics({ useSelectedDate: true }));
dateInput.addEventListener('change', async () => {
  const selected = String(dateInput.value || '').trim();
  if (selected) {
    localStorage.setItem(SELECTED_DATE_KEY, selected);
  }
  if (String(statusEl.textContent || '').trim() === 'RUNNING') return;
  await loadRestaurantsForDate(selected);
  loadAnalytics({ useSelectedDate: true });
});
restaurantFilterEl.addEventListener('change', () => {
  const restaurant = selectedRestaurant();
  if (restaurant) {
    localStorage.setItem(SELECTED_RESTAURANT_KEY, restaurant);
  } else {
    localStorage.removeItem(SELECTED_RESTAURANT_KEY);
  }
  if (String(statusEl.textContent || '').trim() === 'RUNNING') return;
  loadAnalytics({ useSelectedDate: true });
});
sortFilterEl.addEventListener('change', () => {
  const sort = selectedSort();
  if (sort) {
    localStorage.setItem(SELECTED_SORT_KEY, sort);
  } else {
    localStorage.removeItem(SELECTED_SORT_KEY);
  }
  if (String(statusEl.textContent || '').trim() === 'RUNNING') return;
  loadAnalytics({ useSelectedDate: true });
});

clearBtn.addEventListener('click', async () => {
  logBox.textContent = '';
  resultEl.textContent = '';
  if (!activeJobId) {
    logOffset = 0;
    return;
  }
  try {
    const data = await api(`/api/job/${activeJobId}?from=0`);
    logOffset = data.log_size || 0;
  } catch (_) {
    logOffset = 0;
  }
});

copyBtn.addEventListener('click', async () => {
  const text = logBox.textContent || '';
  if (!text.trim()) {
    showTemporaryButtonText(copyBtn, 'Пусто');
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
    showTemporaryButtonText(copyBtn, 'Скопировано');
  } catch (_) {
    showTemporaryButtonText(copyBtn, 'Ошибка');
  }
});

toggleLogBtn.addEventListener('click', () => {
  const hidden = logBox.classList.toggle('hidden');
  toggleLogBtn.textContent = hidden ? 'Показать лог' : 'Скрыть лог';
});

dateInput.value = localStorage.getItem(SELECTED_DATE_KEY) || todayIso();
sortFilterEl.value = localStorage.getItem(SELECTED_SORT_KEY) || 'restaurant_asc';
loadRestaurantsForDate(dateInput.value);
if (window.APP_META) {
  versionEl.textContent = `version ${window.APP_META.version} · updated ${window.APP_META.updatedAt}`;
}
setStatus('IDLE');
setRunningUi(false);
downloadBtn.disabled = true;
downloadPdfBtn.disabled = true;
resetAnalyticsUi();
restoreLatestJob();
