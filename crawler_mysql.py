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
DB_NAME = os.getenv("MYSQL_DB_NAME", "supabase")
DB_USER = os.getenv("MYSQL_DB_USER")
DB_PASS = os.getenv("MYSQL_DB_PASS")
DB_PORT = os.getenv("MYSQL_DB_PORT", "3311")              # MySQL 端口（默认3306）

TABLE_NAME = "vod_dytt"          # 表名（你之前建的表）
MAX_THREAD = 20              # 并发线程数
BATCH_SIZE = 100  # 每100条提交一次（大幅降请求）

# API 基础地址（常量，以后只改这里）https://dyttzy5.tv/api.php/provide/vod/at/json/
API_BASE_URL ="https://dyttzy5.tv/api.php/provide/vod/from/dyttm3u8/at/json/"

# ===================== 【核心】只保留你要的字段 =====================
KEEP_FIELDS = [
    "vod_id", "type_id", "type_name", "type_id_1",
    "vod_name", "vod_sub", "vod_en", "vod_letter",
    "vod_class", "vod_pic", "vod_actor", "vod_director",
    "vod_area", "vod_lang", "vod_year", "vod_douban_id",
    "vod_douban_score", "vod_content", "vod_remarks",
    "vod_score", "vod_play_url", "vod_status", "vod_time"
]

# 全局进度变量
progress_lock = Lock()
completed = 0
total_pages = 0

# 全局MySQL单连接 + 线程锁
db_lock = Lock()
conn = None

# 带自动重试的请求会话
def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# 清洗$$$分割字段，只取后面内容
def clean_field(val):
    if not val:
        return ""
    return val.split("$$$")[-1] if "$$$" in val else val

# 清洗单条视频数据
# ===================== 【核心】只保留需要的字段 =====================
def clean_video_data(v):
    cleaned = {}
    for k in KEEP_FIELDS:
        value = v.get(k)
        if k == "vod_play_url":
            value = clean_field(value)
        cleaned[k] = value
    return cleaned

# 获取全局复用MySQL连接
def get_mysql_conn():
    global conn
    with db_lock:
        if conn is None or not conn.open:
            try:
                conn = pymysql.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASS,
                    database=DB_NAME,
                    charset='utf8mb4',
                    connect_timeout=8
                )
            except Exception as e:
                print(f"❌ MySQL连接失败: {e}")
                return None
    return conn

# 批量一页数据入库 MySQL（ON DUPLICATE KEY 查重更新）
def batch_save(videos):
    if not videos:
        return
    db = get_mysql_conn()
    if not db:
        return

    try:
        with db_lock:
            cursor = db.cursor()
            for v in videos:
                v = clean_video_data(v)
                cols = list(v.keys())
                vals = list(v.values())
                placeholders = ", ".join(["%s"] * len(vals))
                updates = ", ".join([f"{c}=%s" for c in cols])

                sql = f"""
                INSERT INTO {TABLE_NAME} ({', '.join(cols)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates}
                """
                cursor.execute(sql, vals + vals)
            db.commit()
            cursor.close()
    except Exception as e:
        print(f"❌ 入库失败: {e}")

# 获取总页数
def get_total_pages():
    try:
        resp = get_session().get(f"{API_BASE_URL}?pg=1", timeout=15)
        return int(resp.json().get("pagecount", 1))
    except:
        return 1

# 获取单页所有vod_id
def get_ids_by_page(page):
    try:
        resp = get_session().get(f"{API_BASE_URL}?pg={page}", timeout=15)
        return [str(item["vod_id"]) for item in resp.json().get("list", []) if item.get("vod_id")]
    except:
        return []

# 根据id批量获取详情
def get_video_details(ids):
    if not ids:
        return []
    try:
        url = f"{API_BASE_URL}?ac=detail&ids={','.join(ids)}"
        return get_session().get(url, timeout=20).json().get("list", [])
    except:
        return []

# 单页任务
def task(page):
    global completed
    try:
        time.sleep(1.2)
        ids = get_ids_by_page(page)
        details = get_video_details(ids)
        batch_save(details)
    finally:
        with progress_lock:
            completed += 1
            print(f"📊 进度：{completed}/{total_pages} | 已处理第 {page} 页 ✅")

# 主入口
def run():
    global total_pages
    total_pages = get_total_pages()
    print(f"🚀 开始爬取 | 总页数：{total_pages} | 并发线程：{MAX_THREAD}")

    try:
        with ThreadPoolExecutor(max_workers=MAX_THREAD) as executor:
            executor.map(task, range(1, total_pages + 1))
    finally:
        global conn
        if conn and conn.open:
            conn.close()
            print("\n🔌 MySQL 连接已关闭")

    print("\n🎉 全部爬取入库完成！")

if __name__ == "__main__":
    run()
