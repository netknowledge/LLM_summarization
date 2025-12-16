import pandas as pd
import tqdm
import os
import argparse
import re
import json
import spacy
nlp = spacy.load("en_core_web_sm")

import ollama

def remove_think_content(text):
    # remove all <think>...</think> and its content (including multi-line)
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()

parser = argparse.ArgumentParser()
parser.add_argument("--start_index", type=int, default=0, help="Start processing test data row number (starting from 0)")
parser.add_argument("--abstract_type", type=str, default="full", choices=["sent_shuffle", "tail"], help="Type of abstract to use, default is 'full'")
args = parser.parse_args()
start_index = args.start_index
abstract_type = args.abstract_type

# Automatically generate result file path based on model name
result_dir = "data/paper_html_10.1038/abs_annotation"
output_filename = "abs_sent_rhetorical_tag" + f"_{abstract_type}" if abstract_type != 'full' else 'abs_sent_rhetorical_tag' + ".jsonl"
OUTPUT_PATH = os.path.join(result_dir, output_filename)

if abstract_type == 'full':
    TESTSET_PATH = "data/paper_html_10.1038/abs_annotation/test.tsv"
else:
    TESTSET_PATH = f"data/paper_html_10.1038/abs_annotation/test_{abstract_type}.tsv"
test_df = pd.read_csv(TESTSET_PATH, sep="\t")
test_abstracts = test_df["abstract"].tolist()

with open(OUTPUT_PATH, "a", encoding="utf-8") as f_out:
    for i in tqdm.tqdm(range(start_index, len(test_abstracts)), desc="Tagging abstract sentences", 
                        total=len(test_abstracts)-start_index, unit="annotation"):
        doc = nlp(test_abstracts[i])
        assert doc.has_annotation("SENT_START")
        sents = [sent.text for sent in doc.sents]

        prompt_payload = {
        "sentence_count": len(sents),
        "sentences": sents
    }

        payload = {
            "model": 'rhetorical_tagger',
            "prompt": json.dumps(prompt_payload),
            "stream": False,
            "think": False
        }

        response = ollama.generate(model=payload['model'], 
                                   prompt=payload['prompt'], 
                                   stream=payload['stream'], 
                                   think=payload['think'])
        
        content = response['response']
        # filering out <think>...</think> part, write only one line TLDR to result file
        filtered_content = remove_think_content(content)

        labels = json.loads(filtered_content)

        if labels and isinstance(labels, list) and len(labels) == len(sents):
            result_record = {
                "sentences": sents,
                "labels": labels
            }
        else:
            result_record = {
                "sentences": sents,
                "labels": []
            }
                    
        f_out.write(json.dumps(result_record, ensure_ascii=False) + "\n")
        f_out.flush()
