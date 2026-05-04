import os
import requests
import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== 你只需要填这里 =====================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
TABLE_NAME = "movies.vod_data"
MAX_THREAD = 5
# ==========================================================
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=8
        )
        return conn
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return None

def get_total_pages():
    try:
        url = "https://api.xinlangapi.com/xinlangapi.php/provide/vod/from/xlm3u8/?h=24"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        total_page = int(data.get("pagecount", 1))
        print(f"✅ 总页数: {total_page}")
        return total_page
    except Exception as e:
        print(f"❌ 获取总页数失败: {e}")
        return 1

def get_ids_by_page(page):
    try:
        url = f"https://api.xinlangapi.com/xinlangapi.php/provide/vod/from/xlm3u8/?pg={page}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        video_list = data.get("list", [])
        ids = [str(item["vod_id"]) for item in video_list if item.get("vod_id")]
        print(f"📄 第 {page} 页获取 {len(ids)} 个ID")
        return ids
    except Exception as e:
        print(f"❌ 第 {page} 页获取ID异常: {e}")
        return []

def get_video_details(ids):
    if not ids:
        return []
    try:
        ids_str = ",".join(ids)
        url = f"https://api.xinlangapi.com/xinlangapi.php/provide/vod/from/xlm3u8/?ac=detail&ids={ids_str}"
        resp = requests.get(url, timeout=15)
        return resp.json().get("list", [])
    except Exception as e:
        print(f"❌ 获取详情失败: {e}")
        return []

def save_to_db(videos):
    if not videos:
        return

    conn = get_db_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        for v in videos:
            columns = [k for k, v in v.items()]
            values = [v for k, v in v.items()]
            placeholders = ",".join(["%s"] * len(values))
            update_str = ",".join([f"{col}=%s" for col in columns])

            sql = f"""
            INSERT INTO {TABLE_NAME} ({','.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (vod_id) DO UPDATE SET {update_str}
            """
            cur.execute(sql, values + values)

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ 成功入库 {len(videos)} 条")
    except Exception as e:
        print(f"❌ 入库失败: {e}")

def task(page):
    ids = get_ids_by_page(page)
    details = get_video_details(ids)
    save_to_db(details)
    time.sleep(0.8)

def run():
    print("🚀 开始全自动爬取入库...")
    total_page = get_total_pages()

    with ThreadPoolExecutor(max_workers=MAX_THREAD) as executor:
        futures = [executor.submit(task, p) for p in range(1, total_page + 1)]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"🔥 任务异常: {e}")

    print("\n🎉 全部任务完成！")

if __name__ == "__main__":
    run()
