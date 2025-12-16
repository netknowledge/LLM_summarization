import pandas as pd
import requests
import tqdm
import os
import argparse
import re
import nltk
from nltk.tokenize import word_tokenize

nltk.download('punkt', quiet=True)
OLLAMA_URL = "http://localhost:11434/api/generate"

def remove_think_content(text):
    # Remove all <think>...</think> tags and their content (including multi-line)
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()

def word_count(text):
    words = word_tokenize(text)
    return len(words)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", type=str, required=True, help="Ollama model name")
    parser.add_argument("--start_index", type=int, default=0, help="Start processing test data row number (starting from 0)")
    parser.add_argument("--abstract_type", type=str, default="full", choices=["sent_shuffle", "tail"], help="Type of abstract to use, default is 'full'")
    args = parser.parse_args()
    model_name = args.model_name
    start_index = args.start_index
    abstract_type = args.abstract_type

    # automatically generate result file path based on model name and abstract type
    result_dir = "data/paper_html_10.1038/abs_annotation/generated_annotations"
    output_filename = f"{model_name+ f"_{abstract_type}" if abstract_type != "full" else model_name}.txt"
    OUTPUT_PATH = os.path.join(result_dir, output_filename)

    os.makedirs(result_dir, exist_ok=True)

    if abstract_type == 'full':
        TESTSET_PATH = "data/paper_html_10.1038/abs_annotation/test.tsv"
    else:
        TESTSET_PATH = f"data/paper_html_10.1038/abs_annotation/test_{abstract_type}.tsv"
    test_df = pd.read_csv(TESTSET_PATH, sep="\t")
    test_abstracts = test_df["abstract"].tolist()
    test_annotations = test_df['annotation'].tolist()

    with open(OUTPUT_PATH, "a", encoding="utf-8") as f_out:
        for i in tqdm.tqdm(range(start_index, len(test_abstracts)), desc="Generating TLDR", 
                           total=len(test_abstracts)-start_index, unit="annotation"):
            payload = {
                "model": model_name + "_TLDR",
                "prompt": '[Abstract] ' + test_abstracts[i] + f'[Word count: {word_count(test_annotations[i])}]',
                "stream": False
            }
            if model_name in ['qwen3', 'gpt_oss', 'deepseek_r1']:
                payload['think'] = False

            response = requests.post(OLLAMA_URL, json=payload)
            response.raise_for_status()
            result = response.json()
            generated_tldr = result.get("response", "") or ""

            # Remove <think>...</think> parts and write a single-line TLDR to the result file
            filtered_tldr = remove_think_content(generated_tldr)
            filtered_tldr_single_line = " ".join(filtered_tldr.split())
            f_out.write(filtered_tldr_single_line + "\n")
            f_out.flush()

if __name__ == "__main__":
    main()