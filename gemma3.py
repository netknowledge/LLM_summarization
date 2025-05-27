import pandas as pd
import requests
import tqdm
import os
import argparse
import sys

OLLAMA_URL = "http://localhost:11434/api/generate" 
MODEL_NAME = "gemma_TLDR"
OUTPUT_PATH = "data/paper_html_10.1038/abs_annotation/generated_annotations/gemma3.txt"
LOG_PATH = "data/paper_html_10.1038/abs_annotation/generated_annotations/gemma3.log"

def main():
    test_df = pd.read_csv("data/paper_html_10.1038/abs_annotation/test.tsv", sep="\t")
    test_abstracts = test_df["abstract"].tolist()

    parser = argparse.ArgumentParser()
    parser.add_argument("--start_index", type=int, default=0, help="开始处理的test数据行号（从0开始）")
    args = parser.parse_args()
    start_index = args.start_index

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # 打开输出文件和日志文件
    with open(OUTPUT_PATH, "a", encoding="utf-8") as f_out, open(LOG_PATH, "a", encoding="utf-8") as f_log:
        try:
            for i in tqdm.tqdm(range(start_index, len(test_abstracts)), desc="Generating TLDR", total=len(test_abstracts)-start_index, unit="annotation"):
                payload = {
                    "model": MODEL_NAME,
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
                # 只写TLDR到结果文件
                f_out.write(generated_tldr.strip() + "\n")
                f_out.flush()
        except KeyboardInterrupt:
            info_str = f"\n[INFO] 进程被中断，下次请从 --start_index={i} 继续。\n"
            print(info_str, end="", file=sys.stderr)
            f_log.write(info_str)
            f_log.flush()
            sys.exit(0)

if __name__ == "__main__":
    main()