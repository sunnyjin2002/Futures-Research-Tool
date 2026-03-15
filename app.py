import json
import math
import random
import sqlite3
import threading
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from data_extraction import ensure_market_data_table, load_daily_bars
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
]

INDICATORS = [
    {"id": "ma5", "label": "MA5", "default": True},
    {"id": "ma10", "label": "MA10", "default": True},
    {"id": "ma20", "label": "MA20", "default": True},
    {"id": "boll", "label": "布林带", "default": False},
]


def ensure_directories() -> None:
    (BASE_DIR / "data").mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    ensure_directories()
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                futures_id TEXT NOT NULL,
                futures_code TEXT NOT NULL,
                futures_name TEXT NOT NULL,
                exchange_name TEXT NOT NULL,
                model_id TEXT NOT NULL,
                model_label TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                runtime_seconds REAL,
                prediction_payload TEXT,
                error_message TEXT
            )
            """
        )
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(predictions)").fetchall()}
        if "futures_id" not in columns:
            conn.execute("ALTER TABLE predictions ADD COLUMN futures_id TEXT")
            conn.execute(
                """
                UPDATE predictions
                SET futures_id = CASE exchange_name
                    WHEN '上海期货交易所' THEN 'SHFE-' || futures_code
                    WHEN '上海国际能源交易中心' THEN 'INE-' || futures_code
                    WHEN '大连商品交易所' THEN 'DCE-' || futures_code
                    WHEN '郑州商品交易所' THEN 'CZCE-' || futures_code
                    WHEN '中国金融期货交易所' THEN 'CFFEX-' || futures_code
                    WHEN '广州期货交易所' THEN 'GFEX-' || futures_code
                    ELSE futures_code
                END
                WHERE futures_id IS NULL
                """
            )
        if "error_message" not in columns:
            conn.execute("ALTER TABLE predictions ADD COLUMN error_message TEXT")
        conn.commit()
    ensure_market_data_table(DB_PATH)


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


def serialize_prediction(row: sqlite3.Row) -> dict:
    payload = json.loads(row["prediction_payload"]) if row["prediction_payload"] else None
    return {
        "id": row["id"],
        "futuresId": row["futures_id"],
        "futuresCode": row["futures_code"],
        "futuresName": row["futures_name"],
        "exchangeName": row["exchange_name"],
        "modelId": row["model_id"],
        "modelLabel": row["model_label"],
        "status": row["status"],
        "createdAt": row["created_at"],
        "startedAt": row["started_at"],
        "completedAt": row["completed_at"],
        "runtimeSeconds": row["runtime_seconds"],
        "predictionPayload": payload,
        "errorMessage": row["error_message"],
    }

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


def build_model_statuses(product_id: str) -> list:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT p.*
            FROM predictions p
            JOIN (
                SELECT model_id, MAX(id) AS latest_id
                FROM predictions
                WHERE futures_id = ? AND status = 'completed'
                GROUP BY model_id
            ) latest
            ON p.id = latest.latest_id
            ORDER BY p.model_label
            """,
            (product_id,),
        ).fetchall()

    latest_by_model = {row["model_id"]: serialize_prediction(row) for row in rows}
    model_statuses = []
    for model in MODELS:
        latest = latest_by_model.get(model["id"])
        model_statuses.append(
            {
                "id": model["id"],
                "label": model["label"],
                "lastUpdated": latest["completedAt"] if latest else None,
                "prediction": latest,
            }
        )
    return model_statuses


def build_market_series(product_id: str) -> dict:
    cached_bars = load_daily_bars(DB_PATH, product_id, limit=60)
    if cached_bars:
        closes = [round(bar["close"], 2) for bar in cached_bars]
        candles = [
            {
                "label": bar["date"],
                "open": round(bar["open"], 2),
                "high": round(bar["high"], 2),
                "low": round(bar["low"], 2),
                "close": round(bar["close"], 2),
                "volume": int(bar["volume"] or 0),
            }
            for bar in cached_bars[-30:]
        ]
        visible_closes = [item["close"] for item in candles]
        return {
            "candles": candles,
            "indicators": {
                "ma5": moving_average(visible_closes, 5),
                "ma10": moving_average(visible_closes, 10),
                "ma20": moving_average(visible_closes, 20),
                "boll": build_bollinger_bands(visible_closes, 20, 2.0),
            },
        }

    seed = sum(ord(char) for char in product_id)
    rng = random.Random(seed)
    base = 120 + (seed % 35)
    candles = []
    closes = []

    for day in range(30):
        trend = math.sin(day / 4.5) * 3.5
        open_price = base + trend + rng.uniform(-3, 3)
        close_price = open_price + rng.uniform(-4.5, 4.5)
        high_price = max(open_price, close_price) + rng.uniform(0.8, 3.2)
        low_price = min(open_price, close_price) - rng.uniform(0.8, 3.2)
        volume = int(1000 + rng.uniform(0, 1200) + day * 18)
        candles.append(
            {
                "label": f"第{day + 1}日",
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": volume,
            }
        )
        closes.append(round(close_price, 2))
        base = close_price

    return {
        "candles": candles,
        "indicators": {
            "ma5": moving_average(closes, 5),
            "ma10": moving_average(closes, 10),
            "ma20": moving_average(closes, 20),
            "boll": build_bollinger_bands(closes, 20, 2.0),
        },
    }


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
            with get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM predictions WHERE id = ?",
                    (prediction_id,),
                ).fetchone()
            if not row:
                json_response(self, {"error": "未找到对应任务"}, status=404)
                return
            json_response(self, {"prediction": serialize_prediction(row)})
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
                    "latestPrediction": latest,
                    "modelStatuses": model_statuses,
                },
            )
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

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
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO predictions (
                    futures_id, futures_code, futures_name, exchange_name, model_id, model_label,
                    status, created_at, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product["id"],
                    product["code"],
                    product["name"],
                    product["exchange"],
                    model["id"],
                    model["label"],
                    "running",
                    now,
                    now,
                ),
            )
            conn.commit()
            prediction_id = cursor.lastrowid

        thread = threading.Thread(target=run_prediction_job, args=(prediction_id,), daemon=True)
        thread.start()

        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM predictions WHERE id = ?",
                (prediction_id,),
            ).fetchone()
        json_response(self, {"prediction": serialize_prediction(row)}, status=201)

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
