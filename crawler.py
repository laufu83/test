import os
import requests
import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock

# ===================== 你只需要填这里 =====================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
TABLE_NAME = "movies.vod_dytt"
MAX_THREAD = 5
# ==========================================================
API_BASE_URL = "https://api.xinlangapi.com/xinlangapi.php/provide/vod/from/xlm3u8/"
# ====================================================
# 过滤字段
FILTER_FIELDS = {
    "vod_pwd", "vod_pwd_url", "vod_pwd_play", "vod_pwd_play_url",
    "vod_pwd_down", "vod_pwd_down_url",
    "vod_down_from", "vod_down_server", "vod_down_note", "vod_down_url",
    "vod_points", "vod_points_play", "vod_points_down",
    "vod_jumpurl", "vod_tpl", "vod_tpl_play", "vod_tpl_down"
}

# 进度
progress_lock = Lock()
completed = 0
total_pages = 0

# 数据库锁
db_lock = Lock()

# ===================== 重试请求 =====================
def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ===================== 清洗 $$$ =====================
def clean_field(val):
    if not val:
        return ""
    return val.split("$$$")[-1] if "$$$" in val else val

def clean_video_data(v):
    for f in FILTER_FIELDS:
        if f in v:
            del v[f]
    v["vod_play_from"] = clean_field(v.get("vod_play_from", ""))
    v["vod_play_server"] = clean_field(v.get("vod_play_server", ""))
    v["vod_play_note"] = clean_field(v.get("vod_play_note", ""))
    v["vod_play_url"] = clean_field(v.get("vod_play_url", ""))

    # ===================== 🔥 强制超长截取（解决报错） =====================
    max_lengths = {
        "vod_name": 190,
        "vod_sub": 480,
        "vod_en": 90,
        "vod_color": 8,
        "vod_tag": 240,
        "vod_class": 90,
        "vod_pic": 240,
        "vod_pic_thumb": 240,
        "vod_pic_slide": 240,
        "vod_pic_screenshot": 240,
        "vod_director": 280,
        "vod_writer": 90,
        "vod_behind": 240,
        "vod_remarks": 40,
        "vod_pubdate": 40,
        "vod_serial": 40,
        "vod_tv": 40,
        "vod_weekday": 18,
        "vod_area": 40,
        "vod_lang": 40,
        "vod_year": 18,
        "vod_version": 40,
        "vod_state": 40,
        "vod_author": 40,
        "vod_duration": 28,
        "vod_score": 8,
        "vod_douban_score": 8,
        "vod_reurl": 240,
        "vod_rel_vod": 240,
        "vod_rel_art": 240,
        "vod_play_from": 90,
        "vod_play_server": 90,
        "vod_play_note": 90,
        "vod_plot_name": 90,
        "type_name": 40
    }

    for key, max_len in max_lengths.items():
        if key in v and v[key] and isinstance(v[key], str):
            v[key] = v[key][:max_len]

    return v

# ===================== 🔥 关键修复：每次都用新连接 =====================
def get_new_db_conn():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=10
        )
    except Exception as e:
        print(f"❌ DB连接失败: {e}")
        return None

# ===================== 入库（每条独立连接，彻底修复事务错误） =====================
def save_single_page(videos):
    if not videos:
        return

    conn = get_new_db_conn()
    if not conn:
        return

    try:
        cur = conn.cursor()
        for v in videos:
            v = clean_video_data(v)
            cols = list(v.keys())
            vals = list(v.values())
            placeholders = ",".join(["%s"] * len(vals))
            updates = ",".join([f"{c}=%s" for c in cols])

            sql = f"""
            INSERT INTO {TABLE_NAME} ({','.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (vod_id) DO UPDATE SET {updates}
            """
            cur.execute(sql, vals + vals)

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️  入库失败（已跳过）: {e}")
        conn.rollback()  # 修复错误关键
        conn.close()
        return

# ===================== 爬取逻辑 =====================
def get_total_pages():
    try:
        resp = get_session().get(f"{API_BASE_URL}?h=24", timeout=15)
        return int(resp.json().get("pagecount", 1))
    except:
        return 1

def get_ids_by_page(page):
    try:
        resp = get_session().get(f"{API_BASE_URL}?pg={page}", timeout=15)
        return [str(item["vod_id"]) for item in resp.json().get("list", []) if item.get("vod_id")]
    except:
        return []

def get_video_details(ids):
    if not ids:
        return []
    try:
        url = f"{API_BASE_URL}?ac=detail&ids={','.join(ids)}"
        return get_session().get(url, timeout=20).json().get("list", [])
    except:
        return []

# ===================== 任务 + 进度 =====================
def task(page):
    global completed
    try:
        time.sleep(1.3)
        ids = get_ids_by_page(page)
        details = get_video_details(ids)
        save_single_page(details)
    finally:
        with progress_lock:
            completed += 1
            print(f"📊 进度：{completed}/{total_pages} | 第 {page} 页 ✅")

# ===================== 启动 =====================
def run():
    global total_pages
    total_pages = get_total_pages()
    print(f"🚀 开始爬取 | 总页数：{total_pages} | 线程：{MAX_THREAD}")

    with ThreadPoolExecutor(max_workers=MAX_THREAD) as executor:
        executor.map(task, range(1, total_pages + 1))

    print("\n🎉 全部完成！")

if __name__ == "__main__":
    run()
