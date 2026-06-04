import pandas as pd
import requests
import tqdm
import os
import argparse
import re
import nltk
from nltk.tokenize import word_tokenize

nltk.download('punkt', quiet=True)
OLLAMA_PORT = '9903' # don't use default 11434 to avoid being misused by other local users.
OLLAMA_URL = f"http://127.0.0.1:{OLLAMA_PORT}/api/generate"

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
    parser.add_argument("--temperature", type=float, default=None,
                        help="Override model file temperature (e.g. 0.0 for greedy, 1.0 for default)")
    parser.add_argument("--max_new_tokens", type=int, default=None,
                        help="Override max tokens to generate (Ollama: num_predict)")
    args = parser.parse_args()
    model_name = args.model_name
    start_index = args.start_index
    abstract_type = args.abstract_type

    # Build Ollama options overrides (only include keys explicitly set by user)
    ollama_options = {}
    if args.temperature is not None:
        ollama_options["temperature"] = args.temperature
    if args.max_new_tokens is not None:
        ollama_options["num_predict"] = args.max_new_tokens

    # automatically generate result file path based on model name and abstract type
    result_dir = "data/paper_html_10.1038/abs_annotation/generated_annotations"
    base_name = model_name if abstract_type == "full" else f"{model_name}_{abstract_type}"
    prefix_parts = []
    if args.temperature is not None:
        prefix_parts.append(f"tem{args.temperature}")
    if args.max_new_tokens is not None:
        prefix_parts.append(f"max{args.max_new_tokens}")
    prefix = "_".join(prefix_parts) + "_" if prefix_parts else ""
    output_filename = f"{prefix}{base_name}.txt"
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
            if ollama_options:
                payload["options"] = ollama_options
            if model_name in ['qwen3', 'gpt_oss', 'deepseek_r1', 'gemma4', 'qwen3.6']:
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