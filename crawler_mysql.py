import os
import pymysql
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock

# ===================== 你只需要填这里 =====================
DB_HOST = os.getenv("MYSQL_DB_HOST")
DB_NAME = os.getenv("MYSQL_DB_NAME")
DB_USER = os.getenv("MYSQL_DB_USER")
DB_PASS = os.getenv("MYSQL_DB_PASS")
DB_PORT = int(os.getenv("MYSQL_DB_PORT"))        # MySQL 端口（默认3306）

TABLE_NAME = "vod_dytt"          # 表名（你之前建的表）
MAX_THREAD = 20              # 并发线程数
BATCH_SIZE = 100  # 每100条提交一次（大幅降请求）

# API 基础地址（常量，以后只改这里）https://dyttzy5.tv/api.php/provide/vod/at/json/
API_BASE_URL ="https://dyttzy5.tv/api.php/provide/vod/from/dyttm3u8/at/json/"

KEEP_FIELDS = [
    "vod_id", "type_id", "type_name", "type_id_1",
    "vod_name", "vod_sub", "vod_en", "vod_letter",
    "vod_class", "vod_pic", "vod_actor", "vod_director",
    "vod_area", "vod_lang", "vod_year", "vod_douban_id",
    "vod_douban_score", "vod_content", "vod_remarks",
    "vod_score", "vod_play_url", "vod_status", "vod_time"
]
# 全局进度
progress_lock = Lock()
completed = 0
total_pages = 0

# 批量缓存
global_cache = []
cache_lock = Lock()

# 数据库连接
db_lock = Lock()
conn = None

# ===================== 请求重试 =====================
def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ===================== 数据清洗 =====================
def clean_field(val):
    if not val:
        return ""
    return val.split("$$$")[-1] if "$$$" in val else val

def clean_video_data(v):
    cleaned = {}
    for k in KEEP_FIELDS:
        value = v.get(k)
        if k == "vod_play_url":
            value = clean_field(value)
        cleaned[k] = value
    return cleaned


# ===================== MySQL 连接 =====================
def get_mysql_conn():
    global conn
    with db_lock:
        if conn is None or not conn.open:
            conn = pymysql.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS,ssl_verify_identity=True,
                database=DB_NAME, charset='utf8mb4', connect_timeout=10
            )
    return conn

# ===================== 真批量插入 =====================
def real_batch_insert(videos):
    if not videos:
        return

    db = get_mysql_conn()
    if not db:
        return

    try:
        with db_lock:
            cursor = db.cursor()

            # 先清洗第一条，拿到正确的列名
            cleaned_videos = []
            for v in videos:
                cleaned_videos.append(clean_video_data(v))

            sample = cleaned_videos[0]
            cols = list(sample.keys())
            placeholders = ", ".join(["%s"] * len(cols))

            # 拼装数据
            values = []
            for v in cleaned_videos:
                values.append([v[k] for k in cols])

            sql = f"""
            INSERT INTO {TABLE_NAME} ({', '.join(cols)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
            {', '.join([f'{c}=VALUES({c})' for c in cols])}
            """

            cursor.executemany(sql, values)
            db.commit()
            cursor.close()
            print(f"✅ 批量入库 {len(videos)} 条")
    except Exception as e:
        print(f"❌ 批量失败: {e}")

# ===================== 缓存管理 =====================
def add_to_cache(videos):
    global global_cache
    with cache_lock:
        global_cache.extend(videos)
        if len(global_cache) >= BATCH_SIZE:
            real_batch_insert(global_cache)
            global_cache = []

def flush_cache():
    global global_cache
    with cache_lock:
        if global_cache:
            real_batch_insert(global_cache)
            global_cache = []

# ===================== 爬取 =====================
def get_total_pages():
    try:
        return int(get_session().get(f"{API_BASE_URL}?pg=1&h=24", timeout=15).json().get("pagecount", 1))
    except:
        return 1

def get_ids_by_page(page):
    try:
        data = get_session().get(f"{API_BASE_URL}?pg={page}", timeout=15).json()
        return [str(x["vod_id"]) for x in data.get("list", []) if x.get("vod_id")]
    except:
        return []

def get_video_details(ids):
    try:
        url = f"{API_BASE_URL}?ac=detail&ids={','.join(ids)}"
        return get_session().get(url, timeout=20).json().get("list", [])
    except:
        return []

# ===================== 任务 =====================
def task(page):
    global completed
    try:
        time.sleep(1.2)
        ids = get_ids_by_page(page)
        details = get_video_details(ids)
        add_to_cache(details)
    finally:
        with progress_lock:
            completed += 1
            print(f"📊 进度：{completed}/{total_pages} | 第{page}页")

# ===================== 启动 =====================
def run():
    global total_pages
    total_pages = get_total_pages()
    print(f"🚀 开始爬取 | 总页数：{total_pages}")

    try:
        with ThreadPoolExecutor(MAX_THREAD) as executor:
            executor.map(task, range(1, total_pages + 1))
    finally:
        flush_cache()
        global conn
        if conn and conn.open:
            conn.close()
        print("\n🎉 全部完成！")

if __name__ == "__main__":
    run()
