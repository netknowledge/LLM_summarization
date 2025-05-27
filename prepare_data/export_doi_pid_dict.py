import pymysql
from tqdm import tqdm
import pickle

# --------- 配置区域 ---------
MYSQL_HOST = '144.214.39.113'
MYSQL_PORT = 3306
MYSQL_USER = 'key'
MYSQL_PASS = 'Keydge11'
MYSQL_DB   = 'keydge'
BATCH_SIZE = 100000  # 每批多少行
DICT_OUTPUT = 'data/doi_pid_dict.pkl'
# ---------------------------

def get_total_rows(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM paper_identifier")
        return cursor.fetchone()[0]

def export_doi_paperid():
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASS, database=MYSQL_DB, charset='utf8mb4',
        cursorclass=pymysql.cursors.SSCursor  # Stream, 防止内存爆炸
    )
    total = get_total_rows(conn)
    print(f"Total rows: {total}")

    doi2pid = {}
    with conn.cursor() as cursor:
        cursor.execute("SELECT doi, paper_id FROM paper_identifier")
        pbar = tqdm(total=total, desc="导出 DOI-paper_id 字典")
        for i, row in enumerate(cursor):
            doi, paper_id = row
            doi2pid[doi] = paper_id
            pbar.update(1)
        pbar.close()

    print(f"Total mappings: {len(doi2pid)}")
    with open(DICT_OUTPUT, 'wb') as f:
        pickle.dump(doi2pid, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"Saved to {DICT_OUTPUT}")

if __name__ == "__main__":
    export_doi_paperid()