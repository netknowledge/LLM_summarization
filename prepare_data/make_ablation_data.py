import os
import sys
import csv
import random
import argparse
from tqdm import tqdm
import re

def split_sentences(text):
    # Simple sentence splitter based on punctuation. This is not perfect but works for most English scientific abstracts.
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences = [s for s in sentences if s]
    return sentences

def main(input_path, type_):
    assert type_ in {"sent_shuffle", "tail"}, "type must be one of 'sent_shuffle' or 'tail'"

    output_path = input_path.replace('.tsv', f'_{type_}.tsv')
    if output_path == input_path:
        output_path = input_path + f'_{type_}'

    with open(input_path, newline='', encoding='utf-8') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile, delimiter='\t')
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()

        rows = list(reader)
        for row in tqdm(rows, desc="Processing"):
            abstract = row['abstract']
            sentences = split_sentences(abstract)
            if type_ == "sent_shuffle":
                random.shuffle(sentences)
                new_abstract = ' '.join(sentences)
            elif type_ == "tail":
                half = len(sentences) // 2
                new_abstract = ' '.join(sentences[half:])
            row['abstract'] = new_abstract
            writer.writerow(row)

    print(f"Output written to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process abstracts in a TSV file with sentence shuffle or tail.")
    parser.add_argument("type", choices=["sent_shuffle", "tail"], help="Type of processing to perform")
    parser.add_argument("--input", default="/home/zqlyu2/projects/TLDR/data/paper_html_10.1038/abs_annotation/test.tsv", help="Input TSV file path")
    args = parser.parse_args()

    main(args.input, args.type)