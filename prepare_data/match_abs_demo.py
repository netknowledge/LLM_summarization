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
MYSQL_DB   = 'keydge'
DICT_FILE = 'data/doi_pid_dict.pkl'  # DOI -> paper_id 映射字典路径
doi = 'ni.1714'
INPUT_TSV = f'data/paper_html_10.1038/doi_annotation/demo/{doi}.tsv'    # 输入的TSV文件路径
OUTPUT_TSV = f'data/paper_html_10.1038/abs_annotation/demo/{doi}.tsv'  # 输出的TSV文件路径
# ---------------------------

def load_doi_pid_dict(dict_file):
    """加载 DOI -> paper_id 的字典"""
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

def process_tsv_file(input_tsv, output_tsv, doi_pid_dict, conn):
    """处理TSV文件，根据DOI找到abstract并写入输出文件"""
    input_count = 0  # 输入文件中的条目数
    output_count = 0  # 输出文件中的条目数

    with open(input_tsv, 'r') as infile, open(output_tsv, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile, delimiter='\t', fieldnames=['doi', 'annotation'])
        writer = csv.DictWriter(outfile, delimiter='\t', fieldnames=['doi', 'paper_id', 'abstract', 'annotation'])
        writer.writeheader()

        # 收集所有 DOIs 和 annotations
        all_dois = []
        annotations = []
        for row in tqdm(reader, desc="Collecting DOIs"):
            input_count += 1  # 每读取一行增加计数
            doi = row['doi']
            annotation = row['annotation']
            if doi in doi_pid_dict:  # 如果DOI在字典中
                paper_id = doi_pid_dict[doi]
                all_dois.append((doi, paper_id))
                annotations.append(annotation)

        # 一次性查询所有 abstracts
        paper_ids = [item[1] for item in all_dois]
        abstracts = query_abstracts_from_db(conn, paper_ids)

        # 写入输出文件
        for (doi, paper_id), annotation in zip(all_dois, annotations):
            abstract = abstracts.get(paper_id, None)  # 如果没有abstract，则返回None
            if abstract:  # 只有匹配到abstract时才写入文件
                cleaned_abstract = clean_html_tags(abstract)  # 清理HTML标签和多余空格
                writer.writerow({'doi': doi, 'paper_id': paper_id, 'abstract': cleaned_abstract, 'annotation': annotation})
                output_count += 1  # 成功写入一条增加计数

    return input_count, output_count  # 返回输入条目数和输出条目数

if __name__ == "__main__":
    # 加载 DOI -> paper_id 字典
    print("Loading DOI -> paper_id dictionary...")
    doi_pid_dict = load_doi_pid_dict(DICT_FILE)

    # 连接数据库
    print("Connecting to the database...")
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASS, database=MYSQL_DB, charset='utf8mb4'
    )

    # 处理TSV文件
    print("Processing TSV file...")
    input_count, output_count = process_tsv_file(INPUT_TSV, OUTPUT_TSV, doi_pid_dict, conn)

    # 关闭数据库连接
    conn.close()

    # 输出统计信息
    print(f"Processing completed. Results saved in {OUTPUT_TSV}.")
    print(f"Total entries in input file: {input_count}")
    print(f"Total entries written to output file: {output_count}")