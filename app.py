import json
import math
import threading
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from data_extraction import (
    extract_and_store_daily_data,
    load_daily_bars,
    refresh_market_data_if_needed,
)
from data_operations import (
    delete_database_rows as delete_database_rows_db,
    fetch_prediction_by_id,
    get_database_snapshot as get_database_snapshot_db,
    get_market_refresh_status as get_market_refresh_status_db,
    init_db as init_db_storage,
    insert_prediction_record,
    list_model_statuses,
    load_market_indicators as load_market_indicators_db,
    upsert_market_indicators as upsert_market_indicators_db,
    upsert_market_refresh_log as upsert_market_refresh_log_db,
)
from prediction_runner import run_prediction_job_for_record


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = BASE_DIR / "data" / "predictions.db"
HOST = "127.0.0.1"
PORT = 8000


FUTURES_PRODUCTS = [
    {"exchange": "上海期货交易所", "code": "CU", "name": "沪铜"},
    {"exchange": "上海期货交易所", "code": "AL", "name": "沪铝"},
    {"exchange": "上海期货交易所", "code": "ZN", "name": "沪锌"},
    {"exchange": "上海期货交易所", "code": "PB", "name": "沪铅"},
    {"exchange": "上海期货交易所", "code": "NI", "name": "沪镍"},
    {"exchange": "上海期货交易所", "code": "SN", "name": "沪锡"},
    {"exchange": "上海期货交易所", "code": "AU", "name": "沪金"},
    {"exchange": "上海期货交易所", "code": "AG", "name": "沪银"},
    {"exchange": "上海期货交易所", "code": "RB", "name": "螺纹钢"},
    {"exchange": "上海期货交易所", "code": "HC", "name": "热轧卷板"},
    {"exchange": "上海期货交易所", "code": "SS", "name": "不锈钢"},
    {"exchange": "上海期货交易所", "code": "BU", "name": "沥青"},
    {"exchange": "上海期货交易所", "code": "RU", "name": "天然橡胶"},
    {"exchange": "上海期货交易所", "code": "FU", "name": "燃料油"},
    {"exchange": "上海期货交易所", "code": "SP", "name": "纸浆"},
    {"exchange": "上海期货交易所", "code": "BR", "name": "丁二烯橡胶"},
    {"exchange": "上海国际能源交易中心", "code": "SC", "name": "原油"},
    {"exchange": "上海国际能源交易中心", "code": "LU", "name": "低硫燃料油"},
    {"exchange": "上海国际能源交易中心", "code": "NR", "name": "20号胶"},
    {"exchange": "上海国际能源交易中心", "code": "BC", "name": "国际铜"},
    {"exchange": "上海国际能源交易中心", "code": "EC", "name": "集运指数（欧线）"},
    {"exchange": "大连商品交易所", "code": "A", "name": "豆一"},
    {"exchange": "大连商品交易所", "code": "B", "name": "豆二"},
    {"exchange": "大连商品交易所", "code": "M", "name": "豆粕"},
    {"exchange": "大连商品交易所", "code": "Y", "name": "豆油"},
    {"exchange": "大连商品交易所", "code": "P", "name": "棕榈油"},
    {"exchange": "大连商品交易所", "code": "C", "name": "玉米"},
    {"exchange": "大连商品交易所", "code": "CS", "name": "玉米淀粉"},
    {"exchange": "大连商品交易所", "code": "JD", "name": "鸡蛋"},
    {"exchange": "大连商品交易所", "code": "LH", "name": "生猪"},
    {"exchange": "大连商品交易所", "code": "BB", "name": "胶合板"},
    {"exchange": "大连商品交易所", "code": "FB", "name": "纤维板"},
    {"exchange": "大连商品交易所", "code": "L", "name": "聚乙烯"},
    {"exchange": "大连商品交易所", "code": "V", "name": "PVC"},
    {"exchange": "大连商品交易所", "code": "PP", "name": "聚丙烯"},
    {"exchange": "大连商品交易所", "code": "EB", "name": "苯乙烯"},
    {"exchange": "大连商品交易所", "code": "EG", "name": "乙二醇"},
    {"exchange": "大连商品交易所", "code": "I", "name": "铁矿石"},
    {"exchange": "大连商品交易所", "code": "J", "name": "焦炭"},
    {"exchange": "大连商品交易所", "code": "JM", "name": "焦煤"},
    {"exchange": "大连商品交易所", "code": "PG", "name": "液化石油气"},
    {"exchange": "大连商品交易所", "code": "PK", "name": "花生"},
    {"exchange": "郑州商品交易所", "code": "CF", "name": "棉花"},
    {"exchange": "郑州商品交易所", "code": "CY", "name": "棉纱"},
    {"exchange": "郑州商品交易所", "code": "SR", "name": "白糖"},
    {"exchange": "郑州商品交易所", "code": "TA", "name": "PTA"},
    {"exchange": "郑州商品交易所", "code": "MA", "name": "甲醇"},
    {"exchange": "郑州商品交易所", "code": "OI", "name": "菜籽油"},
    {"exchange": "郑州商品交易所", "code": "RM", "name": "菜籽粕"},
    {"exchange": "郑州商品交易所", "code": "SA", "name": "纯碱"},
    {"exchange": "郑州商品交易所", "code": "FG", "name": "玻璃"},
    {"exchange": "郑州商品交易所", "code": "SF", "name": "硅铁"},
    {"exchange": "郑州商品交易所", "code": "SM", "name": "锰硅"},
    {"exchange": "郑州商品交易所", "code": "AP", "name": "苹果"},
    {"exchange": "郑州商品交易所", "code": "CJ", "name": "红枣"},
    {"exchange": "郑州商品交易所", "code": "UR", "name": "尿素"},
    {"exchange": "郑州商品交易所", "code": "PF", "name": "短纤"},
    {"exchange": "郑州商品交易所", "code": "PK", "name": "花生"},
    {"exchange": "郑州商品交易所", "code": "PX", "name": "对二甲苯"},
    {"exchange": "郑州商品交易所", "code": "SH", "name": "烧碱"},
    {"exchange": "郑州商品交易所", "code": "PR", "name": "瓶片"},
    {"exchange": "中国金融期货交易所", "code": "IF", "name": "沪深300股指"},
    {"exchange": "中国金融期货交易所", "code": "IH", "name": "上证50股指"},
    {"exchange": "中国金融期货交易所", "code": "IC", "name": "中证500股指"},
    {"exchange": "中国金融期货交易所", "code": "IM", "name": "中证1000股指"},
    {"exchange": "中国金融期货交易所", "code": "TS", "name": "2年期国债"},
    {"exchange": "中国金融期货交易所", "code": "TF", "name": "5年期国债"},
    {"exchange": "中国金融期货交易所", "code": "T", "name": "10年期国债"},
    {"exchange": "中国金融期货交易所", "code": "TL", "name": "30年期国债"},
    {"exchange": "广州期货交易所", "code": "SI", "name": "工业硅"},
    {"exchange": "广州期货交易所", "code": "LC", "name": "碳酸锂"},
    {"exchange": "广州期货交易所", "code": "PS", "name": "多晶硅"},
]

for product in FUTURES_PRODUCTS:
    exchange_code = {
        "上海期货交易所": "SHFE",
        "上海国际能源交易中心": "INE",
        "大连商品交易所": "DCE",
        "郑州商品交易所": "CZCE",
        "中国金融期货交易所": "CFFEX",
        "广州期货交易所": "GFEX",
    }[product["exchange"]]
    product["id"] = f"{exchange_code}-{product['code']}"

MODELS = [
    {"id": "arima", "label": "ARIMA 基线模型"},
    {"id": "garch", "label": "GARCH 波动率模型"},
    {"id": "multi_model_system", "label": "多模型综合系统"},
]

INDICATORS = [
    {"id": "ma5", "label": "MA5", "default": True},
    {"id": "ma10", "label": "MA10", "default": True},
    {"id": "ma20", "label": "MA20", "default": True},
    {"id": "boll", "label": "布林带", "default": False},
]


def ensure_directories() -> None:
    (BASE_DIR / "data").mkdir(parents=True, exist_ok=True)


def init_db() -> None:
    ensure_directories()
    init_db_storage(DB_PATH)


def json_response(handler: BaseHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def file_response(handler: BaseHTTPRequestHandler, filepath: Path, content_type: str) -> None:
    if not filepath.exists():
        handler.send_error(HTTPStatus.NOT_FOUND)
        return
    body = filepath.read_bytes()
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def parse_json_body(handler: BaseHTTPRequestHandler) -> dict:
    length = int(handler.headers.get("Content-Length", "0"))
    raw_body = handler.rfile.read(length) if length else b"{}"
    return json.loads(raw_body.decode("utf-8") or "{}")


def current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_database_snapshot(limit: Optional[int] = 100) -> dict:
    return get_database_snapshot_db(DB_PATH, limit=limit)


def delete_database_rows(payload: dict) -> dict:
    return delete_database_rows_db(DB_PATH, payload)

def run_prediction_job(prediction_id: int) -> None:
    run_prediction_job_for_record(DB_PATH, prediction_id)


def moving_average(values: list, window: int) -> list:
    series = []
    for index in range(len(values)):
        if index + 1 < window:
            series.append(None)
            continue
        segment = values[index - window + 1 : index + 1]
        series.append(round(sum(segment) / len(segment), 2))
    return series


def rolling_std(values: list, window: int) -> list:
    series = []
    for index in range(len(values)):
        if index + 1 < window:
            series.append(None)
            continue
        segment = values[index - window + 1 : index + 1]
        mean = sum(segment) / len(segment)
        variance = sum((value - mean) ** 2 for value in segment) / len(segment)
        series.append(round(math.sqrt(variance), 2))
    return series


def build_bollinger_bands(values: list, window: int = 20, deviation: float = 2.0) -> dict:
    middle = moving_average(values, window)
    std = rolling_std(values, window)
    upper = []
    lower = []
    for middle_value, std_value in zip(middle, std):
        if middle_value is None or std_value is None:
            upper.append(None)
            lower.append(None)
            continue
        upper.append(round(middle_value + deviation * std_value, 2))
        lower.append(round(middle_value - deviation * std_value, 2))
    return {"middle": middle, "upper": upper, "lower": lower}


def exponential_moving_average(values: list[Optional[float]], period: int) -> list[Optional[float]]:
    alpha = 2 / (period + 1)
    series: list[Optional[float]] = []
    prev: Optional[float] = None
    for index, value in enumerate(values):
        if value is None:
            series.append(None)
            continue
        current = float(value)
        if prev is None or index == 0:
            prev = current
        else:
            prev = alpha * current + (1 - alpha) * prev
        series.append(round(prev, 6))
    return series


def build_macd(values: list[float]) -> dict:
    ema12 = exponential_moving_average(values, 12)
    ema26 = exponential_moving_average(values, 26)
    diff: list[Optional[float]] = []
    for fast, slow in zip(ema12, ema26):
        if fast is None or slow is None:
            diff.append(None)
            continue
        diff.append(round(fast - slow, 6))
    dea = exponential_moving_average([item if item is not None else 0.0 for item in diff], 9)
    hist: list[Optional[float]] = []
    for value, signal in zip(diff, dea):
        if value is None or signal is None:
            hist.append(None)
            continue
        hist.append(round((value - signal) * 2, 6))
    return {"diff": diff, "dea": dea, "hist": hist}


def build_kdj(highs: list[float], lows: list[float], closes: list[float], period: int = 9) -> dict:
    k: list[Optional[float]] = []
    d: list[Optional[float]] = []
    j: list[Optional[float]] = []
    prev_k = 50.0
    prev_d = 50.0

    for index in range(len(closes)):
        start = max(0, index - period + 1)
        window_high = max(highs[start : index + 1])
        window_low = min(lows[start : index + 1])
        close = closes[index]
        rsv = 50.0
        if window_high != window_low:
            rsv = ((close - window_low) / (window_high - window_low)) * 100
        current_k = (2 / 3) * prev_k + (1 / 3) * rsv
        current_d = (2 / 3) * prev_d + (1 / 3) * current_k
        current_j = 3 * current_k - 2 * current_d
        k.append(round(current_k, 6))
        d.append(round(current_d, 6))
        j.append(round(current_j, 6))
        prev_k = current_k
        prev_d = current_d

    return {"k": k, "d": d, "j": j}


def build_market_indicators_from_candles(candles: list[dict]) -> dict:
    closes = [float(item["close"]) for item in candles]
    highs = [float(item["high"]) for item in candles]
    lows = [float(item["low"]) for item in candles]
    return {
        "ma5": moving_average(closes, 5),
        "ma10": moving_average(closes, 10),
        "ma20": moving_average(closes, 20),
        "boll": build_bollinger_bands(closes, 20, 2.0),
        "macd": build_macd(closes),
        "kdj": build_kdj(highs, lows, closes, 9),
    }


def upsert_market_indicators(product_id: str, candles: list[dict], indicators: dict) -> None:
    upsert_market_indicators_db(DB_PATH, product_id, candles, indicators, updated_at=current_timestamp())


def load_market_indicators(product_id: str, trading_dates: list[str]) -> Optional[dict]:
    return load_market_indicators_db(DB_PATH, product_id, trading_dates)


def build_model_statuses(product_id: str) -> list:
    return list_model_statuses(DB_PATH, product_id, MODELS)


def build_market_series(product_id: str) -> dict:
    try:
        cached_bars = refresh_market_data_if_needed(DB_PATH, product_id, lookback_days=365)
    except Exception:
        cached_bars = load_daily_bars(DB_PATH, product_id, limit=365)

    if cached_bars:
        candles = [
            {
                "label": bar["date"],
                "open": round(bar["open"], 2),
                "high": round(bar["high"], 2),
                "low": round(bar["low"], 2),
                "close": round(bar["close"], 2),
                "volume": int(bar["volume"] or 0),
            }
            for bar in cached_bars
        ]
        trading_dates = [item["label"] for item in candles]
        indicators = load_market_indicators(product_id, trading_dates)
        if indicators is None:
            indicators = build_market_indicators_from_candles(candles)
            upsert_market_indicators(product_id, candles, indicators)
        return {
            "candles": candles,
            "indicators": indicators,
        }

    return {
        "candles": [],
        "indicators": {
            "ma5": [],
            "ma10": [],
            "ma20": [],
            "boll": {"middle": [], "upper": [], "lower": []},
            "macd": {"diff": [], "dea": [], "hist": []},
            "kdj": {"k": [], "d": [], "j": []},
        },
    }


def get_market_refresh_status(product_id: str) -> dict:
    return get_market_refresh_status_db(DB_PATH, product_id)


def refresh_market_data_for_product(product_id: str, lookback_days: int = 365) -> dict:
    refreshed_at = current_timestamp()
    bars = extract_and_store_daily_data(DB_PATH, product_id, lookback_days=lookback_days)
    latest_trading_date = bars[-1]["date"] if bars else None
    upsert_market_refresh_log_db(DB_PATH, product_id, refreshed_at, latest_trading_date, len(bars))
    status = get_market_refresh_status(product_id)
    status["loadedRows"] = len(bars)
    return status


def format_market_refresh_error(exc: Exception) -> str:
    raw = str(exc).strip() or exc.__class__.__name__
    lowered = raw.lower()
    if "did not match the expected pattern" in lowered:
        return "数据源返回格式异常，请稍后重试或切换其他品种"
    if "no data fetched using yahooapiparser" in lowered:
        return "Yahoo 数据源暂无可用行情，请稍后重试"
    if "read timed out" in lowered or "timeout" in lowered:
        return "数据源请求超时，请稍后重试"
    return raw


class FuturesResearchHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/":
            file_response(self, STATIC_DIR / "index.html", "text/html; charset=utf-8")
            return

        if parsed.path == "/static/styles.css":
            file_response(self, STATIC_DIR / "styles.css", "text/css; charset=utf-8")
            return

        if parsed.path == "/static/app.js":
            file_response(self, STATIC_DIR / "app.js", "application/javascript; charset=utf-8")
            return

        if parsed.path == "/api/config":
            json_response(
                self,
                {
                    "products": FUTURES_PRODUCTS,
                    "models": MODELS,
                    "indicators": INDICATORS,
                },
            )
            return

        if parsed.path.startswith("/api/predictions/"):
            prediction_id = parsed.path.rsplit("/", 1)[-1]
            prediction = fetch_prediction_by_id(DB_PATH, prediction_id)
            if not prediction:
                json_response(self, {"error": "未找到对应任务"}, status=404)
                return
            json_response(self, {"prediction": prediction})
            return

        if parsed.path == "/api/research":
            params = parse_qs(parsed.query)
            product_id = params.get("product", ["SHFE-CU"])[0]
            model_statuses = build_model_statuses(product_id)
            latest_predictions = [
                item["prediction"] for item in model_statuses if item["prediction"] is not None
            ]
            latest_predictions.sort(key=lambda item: item["completedAt"] or item["createdAt"], reverse=True)
            latest = latest_predictions[0] if latest_predictions else None

            json_response(
                self,
                {
                    "productId": product_id,
                    "market": build_market_series(product_id),
                    "marketRefreshStatus": get_market_refresh_status(product_id),
                    "latestPrediction": latest,
                    "modelStatuses": model_statuses,
                },
            )
            return

        if parsed.path == "/api/database":
            json_response(self, get_database_snapshot(limit=None))
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/api/market/refresh":
            body = parse_json_body(self)
            product_id = body.get("productId")
            product = next((item for item in FUTURES_PRODUCTS if item["id"] == product_id), None)
            if not product:
                json_response(self, {"error": "参数无效，请重新选择期货产品"}, status=400)
                return
            try:
                result = refresh_market_data_for_product(product_id, lookback_days=365)
            except Exception as exc:
                json_response(
                    self,
                    {"error": f"获取期货数据失败：{format_market_refresh_error(exc)}"},
                    status=500,
                )
                return
            json_response(self, {"marketRefreshStatus": result})
            return

        if parsed.path == "/api/database/delete":
            body = parse_json_body(self)
            result = delete_database_rows(body)
            json_response(self, result)
            return

        if parsed.path != "/api/predictions":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        body = parse_json_body(self)
        product_id = body.get("productId")
        model_id = body.get("modelId")

        product = next((item for item in FUTURES_PRODUCTS if item["id"] == product_id), None)
        model = next((item for item in MODELS if item["id"] == model_id), None)

        if not product or not model:
            json_response(self, {"error": "参数无效，请重新选择品种和模型"}, status=400)
            return

        now = current_timestamp()
        prediction_id = insert_prediction_record(DB_PATH, product, model, now)

        thread = threading.Thread(target=run_prediction_job, args=(prediction_id,), daemon=True)
        thread.start()

        prediction = fetch_prediction_by_id(DB_PATH, prediction_id)
        if not prediction:
            json_response(self, {"error": "任务创建后读取失败"}, status=500)
            return
        json_response(self, {"prediction": prediction}, status=201)

    def log_message(self, format: str, *args) -> None:
        return


def run_server() -> None:
    init_db()
    server = ThreadingHTTPServer((HOST, PORT), FuturesResearchHandler)
    print(f"期货研究工具已启动：http://{HOST}:{PORT}")
    print("按 Ctrl+C 停止服务")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
