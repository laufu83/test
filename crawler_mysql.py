import os
import pymysql
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock
from pypinyin import lazy_pinyin, Style
# ===================== 你只需要填这里 =====================
DB_HOST = os.getenv("MYSQL_DB_HOST")
DB_NAME = os.getenv("MYSQL_DB_NAME")
DB_USER = os.getenv("MYSQL_DB_USER")
DB_PASS = os.getenv("MYSQL_DB_PASS")
DB_PORT = int(os.getenv("MYSQL_DB_PORT"))        # MySQL 端口（默认3306）

TABLE_NAME =  os.getenv("BASE_TABLE")          # 表名（你之前建的表）
MAX_THREAD = 20       # 并发线程数
BATCH_SIZE = 100  # 每100条提交一次（大幅降请求）

API_BASE_URL = os.getenv("BASE_URL")

KEEP_FIELDS = [
    "vod_id", "type_id", "type_name", "type_id_1",
    "vod_name", "vod_sub", "vod_en", "vod_letter",
    "vod_class", "vod_pic", "vod_actor", "vod_director",
    "vod_area", "vod_lang", "vod_year", "vod_douban_id",
    "vod_douban_score", "vod_content", "vod_remarks",
    "vod_score", "vod_play_url", "vod_status", "vod_time",
    "vod_name_letter"  # 新增拼音首字母字段
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
                if char.isalpha():
                    result.append(char.upper())
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

# ===================== 请求重试 =====================
def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive"
    })
    return session

# ===================== 数据清洗 =====================
def clean_field(val):
    if not val:
        return ""
    return val.split("$$$")[-1] if "$$$" in val else val

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
