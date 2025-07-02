import csv
from tqdm import tqdm
from pyalex import Works
import pyalex

# --------- 配置区域 ---------
pyalex.config.email = "lyuzhuoqi@outlook.com"
doi = 'ni.1714'
INPUT_TSV = f'data/paper_html_10.1038/doi_annotation/demo/{doi}.tsv'    # 输入的TSV文件路径
OUTPUT_TSV = f'data/paper_html_10.1038/abs_annotation/demo/{doi}.tsv'  # 输出的TSV文件路径
# ---------------------------

def process_tsv_file(input_tsv, output_tsv):
    """处理TSV文件，根据DOI找到abstract并写入输出文件"""
    input_count = 0  # 输入文件中的条目数
    output_count = 0  # 输出文件中的条目数

    with open(input_tsv, 'r') as infile, open(output_tsv, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile, delimiter='\t')
        writer = csv.DictWriter(outfile, delimiter='\t', fieldnames=['doi', 'mag_pid','title', 'abstract', 'annotation'])
        writer.writeheader()

        all_dois = []
        annotations = []
        for row in tqdm(reader, desc="Matching abstracts", unit="paper"):
            input_count += 1  # 每读取一行增加计数
            doi = row['doi']
            annotation = row['annotation']
            W = Works()[f"https://doi.org/{doi}"]
            if W:
                abstract = W.get('abstract', None)
                title = W.get('title', None)
                mag_pid = W.get('ids', {}).get('openalex', '').split('/')[-1][1:]
                print(f"Processing DOI: {doi}, MAG PID: {mag_pid}, Title: {title}, Abstract: {abstract}")
                if abstract is not None:  # 只有匹配到abstract时才写入文件
                    writer.writerow({'doi': doi, 'mag_pid': mag_pid,
                                     'title': title, 'abstract': abstract, 'annotation': annotation})
                    output_count += 1  # 成功写入一条增加计数

    return input_count, output_count  # 返回输入条目数和输出条目数

if __name__ == "__main__":
    # 处理TSV文件
    print("Processing TSV file...")
    input_count, output_count = process_tsv_file(INPUT_TSV, OUTPUT_TSV)

    # 输出统计信息
    print(f"Processing completed. Results saved in {OUTPUT_TSV}.")
    print(f"Total entries in input file: {input_count}")
    print(f"Total entries written to output file: {output_count}")