const state = {
  config: null,
  researchData: null,
  activePredictionId: null,
  timerInterval: null,
  pollInterval: null,
  timerStart: null,
  selectedIndicators: new Set(),
  selectedModelIds: new Set(),
};

const els = {
  form: document.getElementById("prediction-form"),
  productSelect: document.getElementById("product-select"),
  modelSelect: document.getElementById("model-select"),
  runButton: document.getElementById("run-button"),
  statusCard: document.getElementById("status-card"),
  statusTitle: document.getElementById("status-title"),
  statusDetail: document.getElementById("status-detail"),
  taskId: document.getElementById("task-id"),
  createdAt: document.getElementById("created-at"),
  timerDisplay: document.getElementById("timer-display"),
  indicatorList: document.getElementById("indicator-list"),
  modelParameterList: document.getElementById("model-parameter-list"),
  refreshResearch: document.getElementById("refresh-research"),
  chartCanvas: document.getElementById("candlestick-chart"),
  chartBadge: document.getElementById("chart-badge"),
  researchProduct: document.getElementById("research-product"),
  researchTime: document.getElementById("research-time"),
  researchModel: document.getElementById("research-model"),
};

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

function renderModelParameters(modelStatuses) {
  els.modelParameterList.innerHTML = "";
  const nextSelected = new Set();

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
    meta.textContent = `最后更新时间：${formatUpdateTime(modelStatus.lastUpdated)}`;
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
  return dataset.modelStatuses
    .filter((item) => state.selectedModelIds.has(item.id) && item.prediction?.predictionPayload?.length)
    .map((item) => item.prediction);
}

function drawChart(dataset) {
  const canvas = els.chartCanvas;
  const ctx = canvas.getContext("2d");
  const { candles, indicators } = dataset.market;
  const predictionSeries = buildPredictionSeries(dataset);

  const allPrices = candles.flatMap((item) => [item.high, item.low]);
  predictionSeries.forEach((prediction) => {
    prediction.predictionPayload.forEach((item) => allPrices.push(item.price));
  });
  const { min, max } = calcMinMax(allPrices);

  const width = canvas.width;
  const height = canvas.height;
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

  ctx.clearRect(0, 0, width, height);

  ctx.fillStyle = "#f9fbf8";
  ctx.fillRect(0, 0, width, height);

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
    arima: "#d9485f",
    garch: "#2a76c9",
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
  for (let i = 0; i < candles.length; i += 6) {
    const x = chartLeft + step * (i + 1);
    ctx.fillText(candles[i].label, x - 14, height - 20);
  }

  if (predictionSeries.length > 0) {
    predictionSeries[0].predictionPayload.forEach((item, index) => {
      const x = chartLeft + step * (candles.length + index + 1);
      ctx.fillText(item.label, x - 10, height - 20);
    });
  }
}

function updateResearchSummary(data) {
  const selectedProduct = state.config.products.find((product) => product.id === data.productId);
  const selectedPredictions = buildPredictionSeries(data);
  const latestSelected = [...selectedPredictions].sort((a, b) => {
    return (b.completedAt || b.createdAt).localeCompare(a.completedAt || a.createdAt);
  })[0];

  els.researchProduct.textContent = selectedProduct ? productLabel(selectedProduct) : data.productId;

  if (latestSelected) {
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
  renderModelParameters(data.modelStatuses);
  drawChart(data);
  updateResearchSummary(data);
}

async function init() {
  const data = await fetchJSON("/api/config");
  state.config = data;

  populateSelect(els.productSelect, data.products, productLabel, "id");
  populateSelect(els.modelSelect, data.models, (model) => model.label, "id");
  renderIndicators(data.indicators);

  els.form.addEventListener("submit", startPrediction);
  els.refreshResearch.addEventListener("click", () => loadResearch());
  els.productSelect.addEventListener("change", () => loadResearch(els.productSelect.value));

  await loadResearch();
}

init().catch((error) => {
  els.statusTitle.textContent = "初始化失败";
  els.statusDetail.textContent = error.message;
});
