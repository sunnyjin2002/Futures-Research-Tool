const state = {
  config: null,
  researchData: null,
  activePredictionId: null,
  timerInterval: null,
  pollInterval: null,
  timerStart: null,
  selectedIndicators: new Set(),
  selectedModelIds: new Set(),
  zoomDays: 30,
  customWeightedVisible: false,
  customModelWeights: {},
  databaseEditMode: false,
  databaseSelection: {
    predictions: new Set(),
    marketData: new Set(),
  },
};

const els = {
  tabs: Array.from(document.querySelectorAll(".tab")),
  tabPages: Array.from(document.querySelectorAll(".tab-page")),
  form: document.getElementById("prediction-form"),
  productSelect: document.getElementById("product-select"),
  modelSelect: document.getElementById("model-select"),
  refreshMarketData: document.getElementById("refresh-market-data"),
  marketDataStatus: document.getElementById("market-data-status"),
  runButton: document.getElementById("run-button"),
  statusCard: document.getElementById("status-card"),
  statusTitle: document.getElementById("status-title"),
  statusDetail: document.getElementById("status-detail"),
  taskId: document.getElementById("task-id"),
  createdAt: document.getElementById("created-at"),
  timerDisplay: document.getElementById("timer-display"),
  indicatorList: document.getElementById("indicator-list"),
  modelParameterList: document.getElementById("model-parameter-list"),
  addCustomWeighted: document.getElementById("add-custom-weighted"),
  refreshResearch: document.getElementById("refresh-research"),
  chartStage: document.querySelector(".chart-stage"),
  chartCanvas: document.getElementById("candlestick-chart"),
  zoomRange: document.getElementById("zoom-range"),
  zoomValue: document.getElementById("zoom-value"),
  chartLegend: document.getElementById("chart-legend"),
  chartBadge: document.getElementById("chart-badge"),
  researchProduct: document.getElementById("research-product"),
  researchTime: document.getElementById("research-time"),
  researchModel: document.getElementById("research-model"),
  forecastTableBody: document.getElementById("forecast-table-body"),
  predictionsCount: document.getElementById("predictions-count"),
  marketDataCount: document.getElementById("marketdata-count"),
  predictionsTableBody: document.getElementById("predictions-table-body"),
  marketDataTableBody: document.getElementById("marketdata-table-body"),
  editDatabase: document.getElementById("edit-database"),
  cancelDatabase: document.getElementById("cancel-database"),
  deleteDatabase: document.getElementById("delete-database"),
  databaseSelectionSummary: document.getElementById("database-selection-summary"),
  refreshDatabase: document.getElementById("refresh-database"),
};

const chartColors = {
  ma5: "#d19c34",
  ma10: "#3667b2",
  ma20: "#7b59a3",
  bollUpper: "#bf8b2d",
  bollMiddle: "#8e66b7",
  bollLower: "#bf8b2d",
  arima: "#d9485f",
  garch: "#2a76c9",
  multi_model_system: "#1f8f6a",
  custom_weighted: "#bb6a1e",
};

const CUSTOM_WEIGHTED_MODEL_ID = "custom_weighted";

const MIN_ZOOM_DAYS = 30;
const MAX_ZOOM_DAYS = 365;
const ZOOM_STEP = 5;

function formatElapsed(seconds) {
  const mins = String(Math.floor(seconds / 60)).padStart(2, "0");
  const secs = String(Math.floor(seconds % 60)).padStart(2, "0");
  return `${mins}:${secs}`;
}

function productLabel(product) {
  return `${product.name} (${product.code}) - ${product.exchange}`;
}

function populateSelect(selectEl, items, labelFn, valueKey) {
  selectEl.innerHTML = "";
  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item[valueKey];
    option.textContent = labelFn(item);
    selectEl.appendChild(option);
  });
}

function formatUpdateTime(value) {
  return value || "尚无结果";
}

function formatMarketRefreshStatus(status) {
  if (!status || !status.latestUpdatedAt) {
    return "最新数据更新时间：尚未获取";
  }
  const latestTradingDate = status.latestTradingDate || "未知交易日";
  return `最新数据更新时间：${status.latestUpdatedAt}，最新交易日：${latestTradingDate}`;
}

function updateMarketRefreshStatus(status, prefix = "") {
  const base = formatMarketRefreshStatus(status);
  els.marketDataStatus.textContent = prefix ? `${prefix}${base}` : base;
}

function clampZoomDays(value) {
  return Math.max(MIN_ZOOM_DAYS, Math.min(MAX_ZOOM_DAYS, value));
}

function updateZoomUI() {
  els.zoomRange.value = String(state.zoomDays);
  els.zoomValue.textContent = `${state.zoomDays} 天`;
}

function formatAxisLabel(label) {
  if (/^\d{4}-\d{2}-\d{2}$/.test(label)) {
    return label.slice(5);
  }
  return label;
}

function addCalendarDays(dateText, days) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateText)) {
    return null;
  }
  const [year, month, day] = dateText.split("-").map(Number);
  const date = new Date(year, month - 1, day);
  date.setDate(date.getDate() + days);
  const nextYear = date.getFullYear();
  const nextMonth = String(date.getMonth() + 1).padStart(2, "0");
  const nextDay = String(date.getDate()).padStart(2, "0");
  return `${nextYear}-${nextMonth}-${nextDay}`;
}

function shouldRenderPredictionAxisLabel(dayOffset) {
  return dayOffset === 1 || dayOffset % 5 === 0;
}

function formatPredictionAxisLabel(point) {
  const actualDate = addCalendarDays(point.sourceDate, Number(point.dayOffset || 0));
  if (!actualDate) {
    return {
      dateText: point.label,
      offsetText: "",
    };
  }
  return {
    dateText: actualDate.slice(5).replace("-", "/"),
    offsetText: `(T+${point.dayOffset})`,
  };
}

function setActiveTab(targetId) {
  els.tabs.forEach((tab) => {
    tab.classList.toggle("is-active", tab.dataset.tabTarget === targetId);
  });
  els.tabPages.forEach((page) => {
    page.classList.toggle("is-active", page.id === targetId);
  });
}

function renderIndicators(indicators) {
  els.indicatorList.innerHTML = "";
  state.selectedIndicators = new Set(
    indicators.filter((indicator) => indicator.default).map((indicator) => indicator.id),
  );

  indicators.forEach((indicator) => {
    const wrapper = document.createElement("div");
    wrapper.className = "parameter-item";

    const label = document.createElement("label");
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = indicator.default;
    input.dataset.indicatorId = indicator.id;
    input.addEventListener("change", () => {
      if (input.checked) {
        state.selectedIndicators.add(indicator.id);
      } else {
        state.selectedIndicators.delete(indicator.id);
      }
      loadResearch();
    });

    const main = document.createElement("div");
    main.className = "parameter-main";
    const copy = document.createElement("div");
    copy.className = "parameter-copy";
    const title = document.createElement("span");
    title.textContent = indicator.label;
    const meta = document.createElement("span");
    meta.className = "parameter-meta";
    meta.textContent = "技术指标";
    copy.appendChild(title);
    copy.appendChild(meta);
    main.appendChild(input);
    main.appendChild(copy);
    label.appendChild(main);

    const side = document.createElement("span");
    side.className = "parameter-meta";
    side.textContent = indicator.default ? "默认开启" : "可选";
    label.appendChild(side);

    wrapper.appendChild(label);
    els.indicatorList.appendChild(wrapper);
  });
}

function getAvailableModelStatuses(dataset) {
  if (!dataset) {
    return [];
  }
  return dataset.modelStatuses || [];
}

function ensureCustomWeightDefaults(modelStatuses) {
  modelStatuses.forEach((modelStatus) => {
    if (!(modelStatus.id in state.customModelWeights)) {
      state.customModelWeights[modelStatus.id] = modelStatus.prediction ? 100 : 0;
    }
  });
}

function updateCustomWeightedButton() {
  if (!els.addCustomWeighted) {
    return;
  }
  els.addCustomWeighted.disabled = state.customWeightedVisible;
  els.addCustomWeighted.textContent = state.customWeightedVisible ? "已添加定制加权" : "增加定制加权";
}

function getCustomWeightedPrediction(dataset) {
  if (!dataset || !state.customWeightedVisible) {
    return null;
  }

  const sourceModels = getAvailableModelStatuses(dataset).filter((item) => item.prediction?.predictionPayload?.length);
  const weightedModels = sourceModels.filter((item) => Number(state.customModelWeights[item.id] || 0) > 0);
  if (weightedModels.length === 0) {
    return null;
  }

  const totalWeight = weightedModels.reduce((sum, item) => sum + Number(state.customModelWeights[item.id] || 0), 0);
  if (totalWeight <= 0) {
    return null;
  }

  const basePrediction = weightedModels[0].prediction;
  const payload = basePrediction.predictionPayload.map((point) => {
    let weightedPrice = 0;
    weightedModels.forEach((modelStatus) => {
      const weight = Number(state.customModelWeights[modelStatus.id] || 0) / totalWeight;
      const modelPoint = modelStatus.prediction.predictionPayload.find((item) => item.dayOffset === point.dayOffset);
      weightedPrice += Number(modelPoint?.price || point.price) * weight;
    });

    return {
      ...point,
      price: Number(weightedPrice.toFixed(2)),
      modelId: CUSTOM_WEIGHTED_MODEL_ID,
    };
  });

  return {
    id: `${CUSTOM_WEIGHTED_MODEL_ID}-${dataset.productId}`,
    modelId: CUSTOM_WEIGHTED_MODEL_ID,
    modelLabel: "定制加权",
    createdAt: weightedModels
      .map((item) => item.prediction.createdAt)
      .filter(Boolean)
      .sort()
      .slice(-1)[0] || null,
    completedAt: weightedModels
      .map((item) => item.prediction.completedAt || item.prediction.createdAt)
      .filter(Boolean)
      .sort()
      .slice(-1)[0] || null,
    predictionPayload: payload,
  };
}

function buildModelStatuses(dataset) {
  const baseStatuses = getAvailableModelStatuses(dataset);
  if (!state.customWeightedVisible) {
    return baseStatuses;
  }

  const customPrediction = getCustomWeightedPrediction(dataset);
  const totalWeight = baseStatuses.reduce((sum, item) => sum + Number(state.customModelWeights[item.id] || 0), 0);
  const customStatus = {
    id: CUSTOM_WEIGHTED_MODEL_ID,
    label: "定制加权",
    lastUpdated: customPrediction?.completedAt || null,
    prediction: customPrediction,
    selectionMeta:
      totalWeight > 0 ? `权重合计：${totalWeight}%` : "请先给已有模型分配权重",
  };

  return [...baseStatuses, customStatus];
}

function renderCustomWeightCard(modelStatuses) {
  const card = document.createElement("div");
  card.className = "custom-weight-card";

  const header = document.createElement("div");
  header.className = "custom-weight-header";
  const title = document.createElement("strong");
  title.textContent = "定制加权配置";
  const total = document.createElement("span");
  total.className = "custom-weight-total";
  const totalWeight = modelStatuses.reduce((sum, item) => sum + Number(state.customModelWeights[item.id] || 0), 0);
  total.textContent = `当前合计 ${totalWeight}%`;
  header.appendChild(title);
  header.appendChild(total);
  card.appendChild(header);

  const grid = document.createElement("div");
  grid.className = "custom-weight-grid";

  modelStatuses.forEach((modelStatus) => {
    const row = document.createElement("div");
    row.className = "custom-weight-row";
    if (!modelStatus.prediction) {
      row.classList.add("is-disabled");
    }

    const label = document.createElement("label");
    label.textContent = modelStatus.label;
    row.appendChild(label);

    const inputWrap = document.createElement("div");
    inputWrap.className = "custom-weight-input-wrap";
    const input = document.createElement("input");
    input.type = "number";
    input.min = "0";
    input.max = "100";
    input.step = "1";
    input.value = String(state.customModelWeights[modelStatus.id] || 0);
    input.disabled = !modelStatus.prediction;
    input.addEventListener("input", () => {
      const nextValue = Math.max(0, Math.min(100, Number(input.value || 0)));
      state.customModelWeights[modelStatus.id] = nextValue;
      if (state.researchData) {
        renderModelParameters(state.researchData);
        renderForecastTable(state.researchData);
        drawChart(state.researchData);
        updateResearchSummary(state.researchData);
      }
    });
    const suffix = document.createElement("span");
    suffix.textContent = "%";
    inputWrap.appendChild(input);
    inputWrap.appendChild(suffix);
    row.appendChild(inputWrap);
    grid.appendChild(row);
  });

  card.appendChild(grid);
  return card;
}

function renderModelParameters(dataset) {
  const baseModelStatuses = getAvailableModelStatuses(dataset);
  ensureCustomWeightDefaults(baseModelStatuses);
  updateCustomWeightedButton();
  els.modelParameterList.innerHTML = "";
  const nextSelected = new Set();
  const modelStatuses = buildModelStatuses(dataset);

  modelStatuses.forEach((modelStatus, index) => {
    const wrapper = document.createElement("div");
    wrapper.className = "parameter-item is-model";

    const label = document.createElement("label");
    const input = document.createElement("input");
    input.type = "checkbox";
    input.dataset.modelId = modelStatus.id;

    const shouldDefaultSelect =
      state.selectedModelIds.has(modelStatus.id) ||
      (state.selectedModelIds.size === 0 && index === 0);
    input.checked = shouldDefaultSelect;
    if (input.checked) {
      nextSelected.add(modelStatus.id);
    }

    input.addEventListener("change", () => {
      if (input.checked) {
        state.selectedModelIds.add(modelStatus.id);
      } else {
        state.selectedModelIds.delete(modelStatus.id);
      }
      if (state.researchData) {
        drawChart(state.researchData);
        updateResearchSummary(state.researchData);
      }
    });

    const main = document.createElement("div");
    main.className = "parameter-main";
    const copy = document.createElement("div");
    copy.className = "parameter-copy";
    const title = document.createElement("span");
    title.textContent = modelStatus.label;
    const meta = document.createElement("span");
    meta.className = "parameter-meta";
    meta.textContent =
      modelStatus.id === CUSTOM_WEIGHTED_MODEL_ID
        ? modelStatus.selectionMeta || "定制组合"
        : `最后更新时间：${formatUpdateTime(modelStatus.lastUpdated)}`;
    copy.appendChild(title);
    copy.appendChild(meta);
    main.appendChild(input);
    main.appendChild(copy);
    label.appendChild(main);

    const side = document.createElement("span");
    side.className = "parameter-meta";
    side.textContent = modelStatus.prediction ? "可叠加" : "暂无结果";
    label.appendChild(side);

    wrapper.appendChild(label);
    if (modelStatus.id === CUSTOM_WEIGHTED_MODEL_ID) {
      wrapper.appendChild(renderCustomWeightCard(baseModelStatuses));
    }
    els.modelParameterList.appendChild(wrapper);
  });

  state.selectedModelIds = nextSelected;
}

function updateStatus(prediction) {
  els.taskId.textContent = `#${prediction.id}`;
  els.createdAt.textContent = prediction.createdAt;

  els.statusCard.classList.remove("idle", "running", "completed");
  els.statusCard.classList.add(prediction.status);

  if (prediction.status === "running") {
    els.statusTitle.textContent = "模型运算中";
    els.statusDetail.textContent = "模型运算中，请稍候。系统已开始本地写入 SQLite，并将在完成后提示前往研究页面。";
    state.timerStart = new Date(prediction.startedAt.replace(" ", "T")).getTime();
    startTimer();
    return;
  }

  if (prediction.status === "failed") {
    stopTimer();
    els.timerDisplay.textContent = "00:00";
    els.statusTitle.textContent = "模型运行失败";
    els.statusDetail.textContent = prediction.errorMessage || "模型运行失败，请检查依赖、行情数据或模型实现。";
    return;
  }

  stopTimer();
  const runtimeSeconds = prediction.runtimeSeconds || 0;
  els.timerDisplay.textContent = formatElapsed(runtimeSeconds);
  els.statusTitle.textContent = "模型运算完成，请前往研究页面检查结果";
  els.statusDetail.textContent = `本次模型运行耗时 ${runtimeSeconds.toFixed(2)} 秒，结果已保存到本地 SQLite 数据库。`;
  state.selectedModelIds.add(prediction.modelId);
  loadResearch(prediction.futuresId);
}

function startTimer() {
  stopTimer();
  const tick = () => {
    if (!state.timerStart) {
      els.timerDisplay.textContent = "00:00";
      return;
    }
    const seconds = Math.max(0, (Date.now() - state.timerStart) / 1000);
    els.timerDisplay.textContent = formatElapsed(seconds);
  };
  tick();
  state.timerInterval = window.setInterval(tick, 1000);
}

function stopTimer() {
  if (state.timerInterval) {
    window.clearInterval(state.timerInterval);
    state.timerInterval = null;
  }
}

function stopPolling() {
  if (state.pollInterval) {
    window.clearInterval(state.pollInterval);
    state.pollInterval = null;
  }
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "请求失败");
  }
  return data;
}

async function startPrediction(event) {
  event.preventDefault();
  els.runButton.disabled = true;

  try {
    const payload = {
      productId: els.productSelect.value,
      modelId: els.modelSelect.value,
    };
    const data = await fetchJSON("/api/predictions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    state.activePredictionId = data.prediction.id;
    updateStatus(data.prediction);
    pollPrediction();
  } catch (error) {
    els.statusTitle.textContent = "任务启动失败";
    els.statusDetail.textContent = error.message;
  } finally {
    els.runButton.disabled = false;
  }
}

async function refreshSelectedMarketData() {
  const productId = els.productSelect.value;
  if (!productId) {
    return;
  }

  els.refreshMarketData.disabled = true;
  updateMarketRefreshStatus(state.researchData?.marketRefreshStatus, "正在更新数据，");

  try {
    const data = await fetchJSON("/api/market/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productId }),
    });
    updateMarketRefreshStatus(data.marketRefreshStatus, "更新完成，");
    await loadResearch(productId);
  } catch (error) {
    els.marketDataStatus.textContent = `最新数据更新时间：更新失败，${error.message}`;
  } finally {
    els.refreshMarketData.disabled = false;
  }
}

function pollPrediction() {
  stopPolling();
  state.pollInterval = window.setInterval(async () => {
    if (!state.activePredictionId) {
      return;
    }

    try {
      const data = await fetchJSON(`/api/predictions/${state.activePredictionId}`);
      updateStatus(data.prediction);
      if (data.prediction.status === "completed") {
        stopPolling();
      }
    } catch (error) {
      stopPolling();
      stopTimer();
      els.statusTitle.textContent = "状态轮询失败";
      els.statusDetail.textContent = error.message;
    }
  }, 1200);
}

function calcMinMax(values) {
  return {
    min: Math.min(...values),
    max: Math.max(...values),
  };
}

function priceToY(price, minPrice, maxPrice, top, bottom) {
  const range = maxPrice - minPrice || 1;
  return bottom - ((price - minPrice) / range) * (bottom - top);
}

function drawLine(ctx, points, color, width = 2, dash = []) {
  const filtered = points.filter(Boolean);
  if (filtered.length < 2) {
    return;
  }
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.setLineDash(dash);
  ctx.beginPath();
  filtered.forEach((point, index) => {
    if (index === 0) {
      ctx.moveTo(point.x, point.y);
    } else {
      ctx.lineTo(point.x, point.y);
    }
  });
  ctx.stroke();
  ctx.restore();
}

function drawBollinger(ctx, boll, step, chartLeft, chartTop, chartBottom, min, max) {
  const configs = [
    { key: "upper", color: "#bf8b2d", dash: [6, 4] },
    { key: "middle", color: "#8e66b7", dash: [4, 4] },
    { key: "lower", color: "#bf8b2d", dash: [6, 4] },
  ];
  configs.forEach((config) => {
    const points = boll[config.key].map((value, index) => {
      if (value === null) {
        return null;
      }
      return {
        x: chartLeft + step * (index + 1),
        y: priceToY(value, min, max, chartTop, chartBottom),
      };
    });
    drawLine(ctx, points, config.color, 1.8, config.dash);
  });
}

function buildPredictionSeries(dataset) {
  return buildModelStatuses(dataset)
    .filter((item) => state.selectedModelIds.has(item.id) && item.prediction?.predictionPayload?.length)
    .map((item) => item.prediction);
}

function getVisibleMarket(dataset) {
  const windowSize = clampZoomDays(state.zoomDays);
  const candles = dataset.market.candles.slice(-windowSize);
  const startIndex = dataset.market.candles.length - candles.length;
  const indicators = {
    ma5: dataset.market.indicators.ma5.slice(startIndex),
    ma10: dataset.market.indicators.ma10.slice(startIndex),
    ma20: dataset.market.indicators.ma20.slice(startIndex),
    boll: {
      upper: dataset.market.indicators.boll.upper.slice(startIndex),
      middle: dataset.market.indicators.boll.middle.slice(startIndex),
      lower: dataset.market.indicators.boll.lower.slice(startIndex),
    },
  };
  return { candles, indicators };
}

function renderLegend(dataset, predictionSeries) {
  els.chartLegend.innerHTML = "";
  const items = [
    { label: "阳线", color: "#e16a65", swatchClass: "candle-up" },
    { label: "阴线", color: "#3f9965", swatchClass: "candle-down" },
  ];

  if (state.selectedIndicators.has("ma5")) {
    items.push({ label: "MA5", color: chartColors.ma5 });
  }
  if (state.selectedIndicators.has("ma10")) {
    items.push({ label: "MA10", color: chartColors.ma10 });
  }
  if (state.selectedIndicators.has("ma20")) {
    items.push({ label: "MA20", color: chartColors.ma20 });
  }
  if (state.selectedIndicators.has("boll")) {
    items.push({ label: "布林带", color: chartColors.bollUpper, dashed: true });
  }

  predictionSeries.forEach((prediction) => {
    items.push({
      label: `${prediction.modelLabel} 预测`,
      color: chartColors[prediction.modelId] || "#d9485f",
      dashed: true,
    });
  });

  items.forEach((item) => {
    const legend = document.createElement("div");
    legend.className = "legend-item";
    const swatch = document.createElement("span");
    swatch.className = `legend-swatch${item.swatchClass ? ` ${item.swatchClass}` : ""}${item.dashed ? " dashed" : ""}`;
    swatch.style.color = item.color;
    if (!item.swatchClass) {
      swatch.style.borderTopColor = item.color;
    }
    legend.appendChild(swatch);
    const label = document.createElement("span");
    label.textContent = item.label;
    legend.appendChild(label);
    els.chartLegend.appendChild(legend);
  });
}

function buildForecastSummaryRows(dataset) {
  const latestClose = dataset.market.candles.length
    ? dataset.market.candles[dataset.market.candles.length - 1].close
    : null;
  const checkpoints = [5, 10, 20, 30];
  return buildModelStatuses(dataset).map((modelStatus) => {
    const prediction = modelStatus.prediction;
    const summary = {};

    checkpoints.forEach((day) => {
      const point = prediction?.predictionPayload?.find((item) => item.dayOffset === day);
      if (!point || latestClose === null) {
        summary[day] = null;
        return;
      }
      const changePercent = ((point.price - latestClose) / latestClose) * 100;
      summary[day] = {
        price: Number(point.price),
        changePercent,
      };
    });

    return {
      modelLabel: modelStatus.label,
      summary,
    };
  });
}

function renderForecastTable(dataset) {
  const rows = buildForecastSummaryRows(dataset);
  els.forecastTableBody.innerHTML = "";

  rows.forEach((row) => {
    const tr = document.createElement("tr");

    const nameCell = document.createElement("td");
    nameCell.className = "forecast-model-name";
    nameCell.textContent = row.modelLabel;
    tr.appendChild(nameCell);

    [5, 10, 20, 30].forEach((day) => {
      const td = document.createElement("td");
      const value = row.summary[day];

      if (!value) {
        td.className = "forecast-empty";
        td.textContent = "";
        tr.appendChild(td);
        return;
      }

      const wrapper = document.createElement("div");
      wrapper.className = "forecast-cell";

      const price = document.createElement("span");
      price.className = "forecast-price";
      price.textContent = `${value.price.toFixed(2)}`;
      wrapper.appendChild(price);

      const change = document.createElement("span");
      const directionClass =
        value.changePercent > 0 ? " is-up" : value.changePercent < 0 ? " is-down" : "";
      change.className = `forecast-change${directionClass}`;
      const sign = value.changePercent > 0 ? "+" : "";
      change.textContent = `${sign}${value.changePercent.toFixed(2)}%`;
      wrapper.appendChild(change);

      td.appendChild(wrapper);
      tr.appendChild(td);
    });

    els.forecastTableBody.appendChild(tr);
  });
}

function renderDatabaseTableRows(tbody, rows, renderer) {
  tbody.innerHTML = "";
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    renderer(tr, row);
    tbody.appendChild(tr);
  });
}

function appendTextCell(tr, text) {
  const td = document.createElement("td");
  td.textContent = text;
  tr.appendChild(td);
}

function createSelectionCell(tableKey, rowId) {
  const td = document.createElement("td");
  td.className = "db-select-cell";

  if (!state.databaseEditMode) {
    td.textContent = "";
    return td;
  }

  const input = document.createElement("input");
  input.type = "checkbox";
  input.checked = state.databaseSelection[tableKey].has(rowId);
  input.addEventListener("change", () => {
    if (input.checked) {
      state.databaseSelection[tableKey].add(rowId);
    } else {
      state.databaseSelection[tableKey].delete(rowId);
    }
    updateDatabaseToolbar();
    input.closest("tr")?.classList.toggle("is-selected", input.checked);
  });
  td.appendChild(input);
  return td;
}

function updateDatabaseToolbar() {
  const predictionCount = state.databaseSelection.predictions.size;
  const marketDataCount = state.databaseSelection.marketData.size;
  const total = predictionCount + marketDataCount;

  if (!state.databaseEditMode) {
    els.databaseSelectionSummary.textContent = "当前为浏览模式";
    els.editDatabase.hidden = false;
    els.cancelDatabase.hidden = true;
    els.deleteDatabase.hidden = true;
    els.deleteDatabase.disabled = true;
    return;
  }

  els.databaseSelectionSummary.textContent =
    total > 0
      ? `已选择 ${total} 条记录，其中预测 ${predictionCount} 条，行情 ${marketDataCount} 条`
      : "编辑模式已开启，请勾选要删除的数据";
  els.editDatabase.hidden = true;
  els.cancelDatabase.hidden = false;
  els.deleteDatabase.hidden = false;
  els.deleteDatabase.disabled = total === 0;
}

function setDatabaseMessage(message) {
  els.databaseSelectionSummary.textContent = message;
}

function clearDatabaseSelection() {
  state.databaseSelection.predictions.clear();
  state.databaseSelection.marketData.clear();
}

function setDatabaseEditMode(enabled) {
  state.databaseEditMode = enabled;
  if (!enabled) {
    clearDatabaseSelection();
  }
  updateDatabaseToolbar();
}

function renderDatabaseSnapshot(snapshot) {
  els.predictionsCount.textContent = `${snapshot.predictions.count} 条`;
  els.marketDataCount.textContent = `${snapshot.marketData.count} 条`;

  renderDatabaseTableRows(els.predictionsTableBody, snapshot.predictions.rows, (tr, row) => {
    const rowId = Number(row.id);
    tr.classList.toggle("is-selected", state.databaseSelection.predictions.has(rowId));
    tr.appendChild(createSelectionCell("predictions", rowId));
    appendTextCell(tr, String(row.id));
    appendTextCell(tr, `${row.futuresName} (${row.futuresCode})`);
    appendTextCell(tr, row.modelLabel);
    appendTextCell(tr, row.status);
    appendTextCell(tr, row.createdAt || "-");
    appendTextCell(tr, row.completedAt || "-");
    appendTextCell(tr, row.runtimeSeconds == null ? "-" : `${Number(row.runtimeSeconds).toFixed(2)} 秒`);
  });

  renderDatabaseTableRows(els.marketDataTableBody, snapshot.marketData.rows, (tr, row) => {
    const rowId = Number(row.id);
    tr.classList.toggle("is-selected", state.databaseSelection.marketData.has(rowId));
    tr.appendChild(createSelectionCell("marketData", rowId));
    appendTextCell(tr, String(row.id));
    appendTextCell(tr, row.futuresId);
    appendTextCell(tr, row.tradingDate);
    appendTextCell(tr, row.openPrice == null ? "-" : Number(row.openPrice).toFixed(2));
    appendTextCell(tr, row.highPrice == null ? "-" : Number(row.highPrice).toFixed(2));
    appendTextCell(tr, row.lowPrice == null ? "-" : Number(row.lowPrice).toFixed(2));
    appendTextCell(tr, row.closePrice == null ? "-" : Number(row.closePrice).toFixed(2));
    appendTextCell(tr, row.source || "-");
  });

  updateDatabaseToolbar();
}

async function loadDatabaseSnapshot() {
  const snapshot = await fetchJSON("/api/database");
  renderDatabaseSnapshot(snapshot);
}

async function deleteSelectedDatabaseRows() {
  const predictionIds = [...state.databaseSelection.predictions];
  const marketDataIds = [...state.databaseSelection.marketData];
  const total = predictionIds.length + marketDataIds.length;

  if (total === 0) {
    return;
  }

  const confirmed = window.confirm(`确定删除已选中的 ${total} 条数据库记录吗？此操作不可撤销。`);
  if (!confirmed) {
    return;
  }

  try {
    const result = await fetchJSON("/api/database/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        predictionIds,
        marketDataIds,
      }),
    });

    setDatabaseEditMode(false);
    await loadDatabaseSnapshot();

    const deletedTotal = Number(result.deletedPredictions || 0) + Number(result.deletedMarketData || 0);
    setDatabaseMessage(`已删除 ${deletedTotal} 条记录。`);
  } catch (error) {
    setDatabaseMessage(`删除失败：${error.message}`);
    throw error;
  }
}

function isDatabaseTabActive() {
  return document.getElementById("tab-database")?.classList.contains("is-active");
}

function handleDatabaseKeydown(event) {
  if (!state.databaseEditMode || !isDatabaseTabActive()) {
    return;
  }

  const target = event.target;
  const tagName = target?.tagName?.toLowerCase?.() || "";
  if (tagName === "input" || tagName === "textarea" || target?.isContentEditable) {
    return;
  }

  if (event.key === "Delete" || event.key === "Backspace") {
    const hasSelection =
      state.databaseSelection.predictions.size > 0 || state.databaseSelection.marketData.size > 0;
    if (!hasSelection) {
      return;
    }
    event.preventDefault();
    deleteSelectedDatabaseRows().catch(() => {});
  }
}

function drawChart(dataset) {
  const canvas = els.chartCanvas;
  const ctx = canvas.getContext("2d");
  const { candles, indicators } = getVisibleMarket(dataset);
  const predictionSeries = buildPredictionSeries(dataset);
  renderLegend(dataset, predictionSeries);

  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#f9fbf8";
  ctx.fillRect(0, 0, width, height);

  if (candles.length === 0) {
    return;
  }

  const allPrices = candles.flatMap((item) => [item.high, item.low]);
  predictionSeries.forEach((prediction) => {
    prediction.predictionPayload.forEach((item) => allPrices.push(item.price));
  });
  const { min, max } = calcMinMax(allPrices);

  const padding = { top: 28, right: 38, bottom: 50, left: 30 };
  const chartLeft = padding.left;
  const chartRight = width - padding.right;
  const chartTop = padding.top;
  const chartBottom = height - padding.bottom;
  const candleAreaWidth = chartRight - chartLeft;
  const maxPredictionLength = predictionSeries.reduce(
    (maxLength, prediction) => Math.max(maxLength, prediction.predictionPayload.length),
    0,
  );
  const step = candleAreaWidth / (candles.length + maxPredictionLength + 2);
  const candleWidth = Math.max(8, step * 0.56);

  ctx.strokeStyle = "rgba(29, 107, 87, 0.10)";
  ctx.lineWidth = 1;
  for (let i = 0; i < 5; i += 1) {
    const y = chartTop + ((chartBottom - chartTop) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(chartLeft, y);
    ctx.lineTo(chartRight, y);
    ctx.stroke();
  }

  candles.forEach((candle, index) => {
    const x = chartLeft + step * (index + 1);
    const openY = priceToY(candle.open, min, max, chartTop, chartBottom);
    const closeY = priceToY(candle.close, min, max, chartTop, chartBottom);
    const highY = priceToY(candle.high, min, max, chartTop, chartBottom);
    const lowY = priceToY(candle.low, min, max, chartTop, chartBottom);
    const rising = candle.close >= candle.open;

    ctx.strokeStyle = rising ? "#b54844" : "#2d7950";
    ctx.lineWidth = 1.6;
    ctx.beginPath();
    ctx.moveTo(x, highY);
    ctx.lineTo(x, lowY);
    ctx.stroke();

    ctx.fillStyle = rising ? "#e16a65" : "#3f9965";
    const bodyTop = Math.min(openY, closeY);
    const bodyHeight = Math.max(3, Math.abs(closeY - openY));
    ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
  });

  const lineConfigs = [
    { id: "ma5", color: "#d19c34" },
    { id: "ma10", color: "#3667b2" },
    { id: "ma20", color: "#7b59a3" },
  ];

  lineConfigs.forEach((config) => {
    if (!state.selectedIndicators.has(config.id)) {
      return;
    }
    const points = indicators[config.id].map((value, index) => {
      if (value === null) {
        return null;
      }
      return {
        x: chartLeft + step * (index + 1),
        y: priceToY(value, min, max, chartTop, chartBottom),
      };
    });
    drawLine(ctx, points, config.color, 2.2);
  });

  if (state.selectedIndicators.has("boll")) {
    drawBollinger(ctx, indicators.boll, step, chartLeft, chartTop, chartBottom, min, max);
  }

  const predictionColors = {
    arima: chartColors.arima,
    garch: chartColors.garch,
    multi_model_system: chartColors.multi_model_system,
    custom_weighted: chartColors.custom_weighted,
  };

  predictionSeries.forEach((prediction) => {
    const color = predictionColors[prediction.modelId] || "#d9485f";
    const predictionPoints = prediction.predictionPayload.map((item, index) => ({
      x: chartLeft + step * (candles.length + index + 1),
      y: priceToY(item.price, min, max, chartTop, chartBottom),
    }));
    drawLine(ctx, predictionPoints, color, 2.4, [8, 6]);

    ctx.save();
    ctx.fillStyle = color;
    predictionPoints.forEach((point) => {
      ctx.beginPath();
      ctx.arc(point.x, point.y, 3.5, 0, Math.PI * 2);
      ctx.fill();
    });
    ctx.restore();
  });

  ctx.fillStyle = "#5f7167";
  ctx.font = '12px "PingFang SC", sans-serif';
  const labelInterval = Math.max(1, Math.ceil(candles.length / 8));
  for (let i = 0; i < candles.length; i += labelInterval) {
    const x = chartLeft + step * (i + 1);
    const label = formatAxisLabel(candles[i].label);
    ctx.fillText(label, x - 18, height - 20);
  }

  if (predictionSeries.length > 0) {
    ctx.save();
    ctx.font = '11px "PingFang SC", sans-serif';
    ctx.textAlign = "center";
    predictionSeries[0].predictionPayload.forEach((item, index) => {
      if (!shouldRenderPredictionAxisLabel(item.dayOffset)) {
        return;
      }
      const x = chartLeft + step * (candles.length + index + 1);
      const label = formatPredictionAxisLabel(item);
      ctx.fillText(label.dateText, x, height - 28);
      if (label.offsetText) {
        ctx.fillText(label.offsetText, x, height - 14);
      }
    });
    ctx.restore();
  }
}

function updateResearchSummary(data) {
  const selectedProduct = state.config.products.find((product) => product.id === data.productId);
  const selectedPredictions = buildPredictionSeries(data);
  const latestSelected = [...selectedPredictions].sort((a, b) => {
    return (b.completedAt || b.createdAt).localeCompare(a.completedAt || a.createdAt);
  })[0];
  const hasMarketData = data.market.candles.length > 0;

  els.researchProduct.textContent = selectedProduct ? productLabel(selectedProduct) : data.productId;

  if (!hasMarketData) {
    els.chartBadge.textContent = "暂无本地行情数据";
    els.researchTime.textContent = "-";
  } else if (latestSelected) {
    els.chartBadge.textContent = "已按多选参数叠加研究结果";
    els.researchTime.textContent = latestSelected.completedAt || latestSelected.createdAt;
  } else if (data.latestPrediction) {
    els.chartBadge.textContent = "已有模型结果，可在右侧勾选叠加";
    els.researchTime.textContent = data.latestPrediction.completedAt || data.latestPrediction.createdAt;
  } else {
    els.chartBadge.textContent = "当前仅展示占位行情";
    els.researchTime.textContent = "-";
  }

  els.researchModel.textContent =
    selectedPredictions.length > 0
      ? selectedPredictions.map((item) => item.modelLabel).join(" / ")
      : "未选择模型";
}

async function loadResearch(forceProductId = null) {
  const productId = forceProductId || els.productSelect.value;
  if (!productId) {
    return;
  }

  const data = await fetchJSON(`/api/research?product=${encodeURIComponent(productId)}`);
  state.researchData = data;
  updateMarketRefreshStatus(data.marketRefreshStatus);
  renderModelParameters(data);
  renderForecastTable(data);
  drawChart(data);
  updateResearchSummary(data);
}

async function init() {
  const data = await fetchJSON("/api/config");
  state.config = data;

  populateSelect(els.productSelect, data.products, productLabel, "id");
  populateSelect(els.modelSelect, data.models, (model) => model.label, "id");
  renderIndicators(data.indicators);
  updateZoomUI();

  els.tabs.forEach((tab) => {
    tab.addEventListener("click", async () => {
      setActiveTab(tab.dataset.tabTarget);
      if (tab.dataset.tabTarget === "tab-database") {
        await loadDatabaseSnapshot();
      }
    });
  });
  els.form.addEventListener("submit", startPrediction);
  els.refreshMarketData.addEventListener("click", () => refreshSelectedMarketData());
  els.addCustomWeighted.addEventListener("click", () => {
    state.customWeightedVisible = true;
    state.selectedModelIds.add(CUSTOM_WEIGHTED_MODEL_ID);
    updateCustomWeightedButton();
    if (state.researchData) {
      renderModelParameters(state.researchData);
      renderForecastTable(state.researchData);
      drawChart(state.researchData);
      updateResearchSummary(state.researchData);
    }
  });
  els.refreshResearch.addEventListener("click", () => loadResearch());
  els.refreshDatabase.addEventListener("click", () => loadDatabaseSnapshot());
  els.editDatabase.addEventListener("click", async () => {
    setDatabaseEditMode(true);
    await loadDatabaseSnapshot();
  });
  els.cancelDatabase.addEventListener("click", async () => {
    setDatabaseEditMode(false);
    await loadDatabaseSnapshot();
  });
  els.deleteDatabase.addEventListener("click", async () => {
    await deleteSelectedDatabaseRows();
  });
  window.addEventListener("keydown", handleDatabaseKeydown);
  els.productSelect.addEventListener("change", () => loadResearch(els.productSelect.value));
  els.zoomRange.addEventListener("input", () => {
    state.zoomDays = clampZoomDays(Number(els.zoomRange.value));
    updateZoomUI();
    if (state.researchData) {
      drawChart(state.researchData);
    }
  });
  els.chartStage.addEventListener(
    "wheel",
    (event) => {
      if (!state.researchData) {
        return;
      }
      event.preventDefault();
      const direction = event.deltaY > 0 ? 1 : -1;
      state.zoomDays = clampZoomDays(state.zoomDays + direction * ZOOM_STEP);
      updateZoomUI();
      drawChart(state.researchData);
    },
    { passive: false },
  );

  await loadResearch();
  updateDatabaseToolbar();
  updateCustomWeightedButton();
}

init().catch((error) => {
  els.statusTitle.textContent = "初始化失败";
  els.statusDetail.textContent = error.message;
});
