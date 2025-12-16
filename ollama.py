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
    # delete all <think>...</think> and its content (across multiple lines)
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()

def word_count(text):
    words = word_tokenize(text)
    return len(words)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", type=str, required=True, help="Ollama model name")
    parser.add_argument("--start_index", type=int, default=0, help="Start index of test data row (0-based)")
    parser.add_argument("--abstract_type", type=str, default="full", choices=["sent_shuffle", "tail"], help="Abstract type to use, default is 'full'")
    args = parser.parse_args()
    model_name = args.model_name
    start_index = args.start_index
    abstract_type = args.abstract_type

    result_dir = "data/paper_html_10.1038/abs_annotation/generated_annotations" # automatically generating output file paths based on model used
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
            if model_name  in ['qwen3', 'gpt-oss']:
                payload['think'] = False

            response = requests.post(OLLAMA_URL, json=payload)
            response.raise_for_status()
            result = response.json()
            generated_tldr = result.get("response", "") or ""

            filtered_tldr = remove_think_content(generated_tldr) # remove the <think>...<\think> part, and only write one line to the output file
            filtered_tldr_single_line = " ".join(filtered_tldr.split())
            f_out.write(filtered_tldr_single_line + "\n")
            f_out.flush()

if __name__ == "__main__":
    main()