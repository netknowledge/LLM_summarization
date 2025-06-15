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

def merge_tsv_files(tsv_files, output_file):
    header = ["abs_doi", "paper_id", "abstract", "annotation"]
    write_header = True
    with open(output_file, "w", encoding="utf-8", newline='') as outfile:
        writer = csv.writer(outfile, delimiter='\t')
        for tsv_file in tsv_files:
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
                    writer.writerow(row)

if __name__ == "__main__":
    tsv_files = find_all_tsv_files(ROOT_DIR)
    print(f"Found {len(tsv_files)} .tsv files, merging...")
    merge_tsv_files(tsv_files, OUTPUT_FILE)
    print(f"Merge complete. Output saved to {OUTPUT_FILE}")