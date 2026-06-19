import os
import requests
import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock
from pypinyin import lazy_pinyin, Style
# ===================== 你只需要填这里 =====================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
TABLE_NAME = "movies.vod"
MAX_THREAD = 5
# ==========================================================
API_BASE_URL = "https://dyttzy5.tv/api.php/provide/vod/from/dyttm3u8/at/json/"
# ====================================================
KEEP_FIELDS = [
    "vod_id", "type_id", "type_name", "type_id_1",
    "vod_name", "vod_sub", "vod_en", "vod_letter",
    "vod_class", "vod_pic", "vod_actor", "vod_director",
    "vod_area", "vod_lang", "vod_year", "vod_douban_id",
    "vod_douban_score", "vod_content", "vod_remarks",
    "vod_score", "vod_play_url", "vod_status", "vod_time",
     "vod_name_letter"
]

# 进度
progress_lock = Lock()
completed = 0
total_pages = 0

# 数据库锁
db_lock = Lock()

def get_chinese_first_letter(text: str) -> str:
    """
    获取字符串中所有汉字拼音首字母，过滤非字母字符，只保留大写字母，最多50个字符
    :param text: 输入混合文字
    :return: 大写首字母拼接字符串（仅字母，最长50位）
    """
    if not text:
        return ""
    result = []
    # 遍历每个字符单独转拼音取首字母
    for char in text:
        if '\u4e00' <= char <= '\u9fff':  # 判断是否汉字
            pinyin_list = lazy_pinyin(char, style=Style.FIRST_LETTER)
            if pinyin_list:
                first_letter = pinyin_list[0].upper()
                # 只保留字母
                if first_letter.isalpha():
                    result.append(first_letter)
        else:
            # 非汉字只保留英文字母并转大写，数字符号直接丢弃
            if char.isalpha():
                result.append(char.upper())
            elif char.isdigit():
                result.append(char)
        # 提前截断，避免多余遍历
        if len(result) >= 50:
            break
    # 最多截取前50个大写字母返回
    return ''.join(result[:50])
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

# 清洗单条视频数据
# ===================== 【核心】只保留需要的字段 =====================
def clean_video_data(v):
    cleaned = {}
    for k in KEEP_FIELDS:
        if k == "vod_name_letter":
            # 根据影片名称生成拼音首字母
            vod_name = v.get("vod_name", "")
            cleaned[k] = get_chinese_first_letter(vod_name)
        else:
            value = v.get(k, "")
            if k == "vod_play_url":
                value = clean_field(value)
            cleaned[k] = value
    return cleaned


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
        resp = get_session().get(f"{API_BASE_URL}?pg=1&h=24", timeout=15)
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
