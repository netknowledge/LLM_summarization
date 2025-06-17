from openai import OpenAI
import pandas as pd
import tqdm
import os
import argparse

def load_api_key(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.readline().strip()

client = OpenAI(api_key=load_api_key("api_key/deepseek.txt"), base_url="https://api.deepseek.com")

with open("sys_prompt.txt", "r", encoding="utf-8") as f:
    sys_msg = f.read().strip()

meta_messages = [{
        "role": "system",
        "content": sys_msg
    }]

train_df = pd.read_csv("data/paper_html_10.1038/abs_annotation/train.tsv", sep="\t")
train_abstracts = train_df["abstract"].tolist()
train_annotations = train_df["annotation"].tolist()

for i in range(len(train_df)):
    meta_messages.append({
        "role": "user",
        "content": train_abstracts[i]
    })
    meta_messages.append({
        "role": "assistant",
        "content": train_annotations[i]
    })

parser = argparse.ArgumentParser()
parser.add_argument("--start_index", type=int, default=0, help="开始处理的test数据行号（从0开始）")
parser.add_argument("--abstract_type", type=str, default="full", choices=["sent_shuffle", "tail"], help="使用的摘要类型，默认为'full'")
args = parser.parse_args()
start_index = args.start_index
abstract_type = args.abstract_type

if abstract_type != "full":
    TESTSET_PATH = f"data/paper_html_10.1038/abs_annotation/test_{abstract_type}.tsv"
    OUTPUT_PATH = f"data/paper_html_10.1038/abs_annotation/generated_annotations/deepseek_v3_{abstract_type}.txt"
else:
    TESTSET_PATH = "data/paper_html_10.1038/abs_annotation/test.tsv"
    OUTPUT_PATH = "data/paper_html_10.1038/abs_annotation/generated_annotations/deepseek_v3.txt"

test_df = pd.read_csv(TESTSET_PATH, sep="\t")
test_abstracts = test_df["abstract"].tolist()
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

for i in tqdm.tqdm(range(start_index, len(test_df)), desc="Generating annotations", total=len(test_df)-start_index, unit="annotation"):
    query_messages = [{
        "role": "user",
        "content": test_abstracts[i]
    }]

    completion = client.chat.completions.create(
        model="deepseek-chat",
        messages=meta_messages + query_messages,
        stream=False
    )

    generated_annotation = completion.choices[0].message.content or ""

    with open(OUTPUT_PATH, "a", encoding="utf-8") as f:
        f.write(generated_annotation + "\n")