import os
import csv
from glob import glob

# 配置
ROOT_DIR = "data/paper_html_10.1038/abs_annotation"
OUTPUT_FILE = "data/paper_html_10.1038/abs_annotation/abs_annotation.tsv"

def find_all_tsv_files(root_dir):
    """递归查找所有子目录下的.tsv文件"""
    # 匹配 abs_annotation/数字/*.tsv
    pattern = os.path.join(root_dir, '[0-9]*', '*.tsv')
    return glob(pattern)

def extract_annotation_doi(tsv_file):
    """从文件名提取annotation_doi，格式为 10.1038/文件名(不含扩展名)"""
    basename = os.path.basename(tsv_file)  # 获取文件名
    name_without_ext = os.path.splitext(basename)[0]  # 去掉.tsv扩展名
    return f"10.1038/{name_without_ext}"

def merge_tsv_files(tsv_files, output_file):
    header = ["abs_doi", "paper_id", "abstract", "annotation_doi", "annotation"]
    write_header = True
    with open(output_file, "w", encoding="utf-8", newline='') as outfile:
        writer = csv.writer(outfile, delimiter='\t')
        for tsv_file in tsv_files:
            annotation_doi = extract_annotation_doi(tsv_file)
            with open(tsv_file, "r", encoding="utf-8") as infile:
                reader = csv.reader(infile, delimiter='\t')
                file_header = next(reader, None)
                # 检查文件为空
                if file_header is None:
                    continue
                # 写入表头仅限第一次
                if write_header:
                    writer.writerow(header)
                    write_header = False
                # 跳过表头
                for row in reader:
                    # 检查列数是否正确
                    if len(row) != 4:
                        print(f"Warning: {tsv_file} has invalid row (wrong number of columns): {row}")
                        continue
                    # 检查每列是否为空
                    if any(cell.strip() == "" or cell.strip() in ["#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null"] for cell in row):
                        print(f"Warning: {tsv_file} has empty cell in row: {row}")
                        continue
                    # 插入annotation_doi到第4列位置（索引3）
                    new_row = row[:3] + [annotation_doi] + row[3:]
                    writer.writerow(new_row)

if __name__ == "__main__":
    tsv_files = find_all_tsv_files(ROOT_DIR)
    print(f"Found {len(tsv_files)} .tsv files, merging...")
    merge_tsv_files(tsv_files, OUTPUT_FILE)
    print(f"Merge complete. Output saved to {OUTPUT_FILE}")