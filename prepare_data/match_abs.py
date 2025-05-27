import os
import pymysql
import pickle
import csv
import re
from tqdm import tqdm

# --------- 配置区域 ---------
MYSQL_HOST = '144.214.39.113'
MYSQL_PORT = 3306
MYSQL_USER = 'key'
MYSQL_PASS = 'Keydge11'
MYSQL_DB = 'keydge'
DICT_FILE = 'data/doi_pid_dict.pkl'  # DOI -> paper_id 的字典路径
INPUT_DIR_TEMPLATE = 'data/paper_html_10.1038/doi_annotation/{db_num}/'  # 输入目录模板
OUTPUT_DIR_TEMPLATE = 'data/paper_html_10.1038/abs_annotation/{db_num}/'  # 输出目录模板
# ---------------------------

def load_doi_pid_dict(dict_file):
    """加载 DOI -> paper_id 的字典"""
    print("Loading DOI -> paper_id dictionary... This might take some time.")
    with open(dict_file, 'rb') as f:
        return pickle.load(f)

def query_abstracts_from_db(conn, paper_ids):
    """从数据库中一次性查询 abstracts"""
    format_strings = ','.join(['%s'] * len(paper_ids))
    query = f"SELECT paper_id, abstract FROM paper_abstract WHERE paper_id IN ({format_strings})"
    with conn.cursor() as cursor:
        cursor.execute(query, tuple(paper_ids))
        return {row[0]: row[1] for row in cursor.fetchall()}  # 返回 {paper_id: abstract} 字典

def clean_html_tags(text):
    """清除 HTML 标签并处理多余空格"""
    if text is None:
        return None
    # 移除 HTML 标签
    text = re.sub(r'<[^>]+>', ' ', text)
    # 替换多个连续空格为单个空格
    text = re.sub(r'\s+', ' ', text)
    # 去除首尾空格
    return text.strip()

def process_single_file(input_file, output_file, doi_pid_dict, conn):
    """处理单个TSV文件，根据DOI找到abstract并写入输出文件"""
    input_count = 0  # 输入文件中的条目数
    output_count = 0  # 输出文件中的条目数

    # 收集待处理的 DOIs 和 annotations
    with open(input_file, 'r') as infile:
        reader = csv.DictReader(infile, delimiter='\t', fieldnames=['doi', 'annotation'])
        all_dois = []
        annotations = []

        for row in reader:
            input_count += 1  # 每读取一行增加计数
            doi = row['doi']
            annotation = row['annotation']
            if doi in doi_pid_dict:  # 如果 DOI 在字典中
                paper_id = doi_pid_dict[doi]
                all_dois.append((doi, paper_id))
                annotations.append(annotation)

    # 如果没有需要查询的 DOI，直接返回
    if not all_dois:
        return input_count, output_count

    # 查询 abstracts
    paper_ids = [item[1] for item in all_dois]
    abstracts = query_abstracts_from_db(conn, paper_ids)

    # 写入输出文件
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, delimiter='\t', fieldnames=['doi', 'paper_id', 'abstract', 'annotation'])
        writer.writeheader()

        for (doi, paper_id), annotation in zip(all_dois, annotations):
            abstract = abstracts.get(paper_id, None)  # 如果没有 abstract，则返回 None
            if abstract:  # 只有匹配到 abstract 时才写入文件
                cleaned_abstract = clean_html_tags(abstract)  # 清理 HTML 标签和多余空格
                if len(cleaned_abstract) > 0:
                    writer.writerow({'doi': doi, 'paper_id': paper_id, 'abstract': cleaned_abstract, 'annotation': annotation})
                    output_count += 1  # 成功写入一条记录

    # 如果没有写入任何记录，则删除空文件
    if output_count == 0:
        os.remove(output_file)

    return input_count, output_count  # 返回输入条目数和输出条目数

def process_all_files(db_num, doi_pid_dict, conn):
    """处理指定目录下的所有 TSV 文件"""
    input_dir = INPUT_DIR_TEMPLATE.format(db_num=db_num)
    output_dir = OUTPUT_DIR_TEMPLATE.format(db_num=db_num)

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 获取所有 .tsv 文件
    tsv_files = [f for f in os.listdir(input_dir) if f.endswith('.tsv')]

    # 初始化进度条
    with tqdm(total=len(tsv_files), desc="Processing files") as pbar:
        written_files = 0
        pbar.set_postfix(written_files=written_files)
        for tsv_file in tsv_files:
            input_file = os.path.join(input_dir, tsv_file)
            output_file = os.path.join(output_dir, tsv_file)

            # 处理单个文件
            _, output_count = process_single_file(input_file, output_file, doi_pid_dict, conn)

            # 更新文件计数
            if output_count > 0:
                written_files += 1  # 只有成功写入的文件才计数

            # 更新进度条
            pbar.set_postfix(written_files=written_files)
            pbar.update(1)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python match_abs.py <db_num>")
        sys.exit(1)

    db_num = sys.argv[1]

    # 加载 DOI -> paper_id 字典
    doi_pid_dict = load_doi_pid_dict(DICT_FILE)

    # 连接数据库
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASS, database=MYSQL_DB, charset='utf8mb4'
    )

    # 处理所有文件
    process_all_files(db_num, doi_pid_dict, conn)

    # 关闭数据库连接
    conn.close()