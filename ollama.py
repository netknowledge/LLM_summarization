import pandas as pd
import requests
import tqdm
import os
import argparse
import sys
import re
from datetime import datetime

OLLAMA_URL = "http://localhost:11434/api/generate"

def remove_think_content(text):
    # 去除所有<think>...</think>及其内容（跨多行也可）
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", type=str, required=True, help="Ollama模型名称")
    parser.add_argument("--start_index", type=int, default=0, help="开始处理的test数据行号（从0开始）")
    args = parser.parse_args()
    model_name = args.model_name
    start_index = args.start_index

    # 自动根据模型名生成结果和日志文件路径
    result_dir = "data/paper_html_10.1038/abs_annotation/generated_annotations"
    output_filename = f"{model_name}.txt"
    log_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{model_name}_{log_time}.log"
    OUTPUT_PATH = os.path.join(result_dir, output_filename)
    LOG_PATH = os.path.join(result_dir, log_filename)

    os.makedirs(result_dir, exist_ok=True)

    test_df = pd.read_csv("data/paper_html_10.1038/abs_annotation/test.tsv", sep="\t")
    test_abstracts = test_df["abstract"].tolist()

    with open(OUTPUT_PATH, "a", encoding="utf-8") as f_out, open(LOG_PATH, "a", encoding="utf-8") as f_log:
        try:
            for i in tqdm.tqdm(range(start_index, len(test_abstracts)), desc="Generating TLDR", total=len(test_abstracts)-start_index, unit="annotation"):
                payload = {
                    "model": model_name,
                    "prompt": test_abstracts[i],
                    "stream": False
                }
                try:
                    response = requests.post(OLLAMA_URL, json=payload)
                    response.raise_for_status()
                    result = response.json()
                    generated_tldr = result.get("response", "") or ""
                except Exception as e:
                    error_str = f"\n[ERROR] Failed at index {i}: {e}\n请下次从 --start_index={i} 继续。\n"
                    print(error_str, end="", file=sys.stderr)
                    f_log.write(error_str)
                    f_log.flush()
                    sys.exit(1)
                # 过滤掉<think>...</think>部分，只写一行TLDR到结果文件
                filtered_tldr = remove_think_content(generated_tldr)
                filtered_tldr_single_line = " ".join(filtered_tldr.split())
                f_out.write(filtered_tldr_single_line + "\n")
                f_out.flush()
        except KeyboardInterrupt:
            info_str = f"\n[INFO] 进程被中断，下次请从 --start_index={i} 继续。\n"
            print(info_str, end="", file=sys.stderr)
            f_log.write(info_str)
            f_log.flush()
            sys.exit(0)

if __name__ == "__main__":
    main()