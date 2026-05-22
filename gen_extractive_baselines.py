"""
Extractive baselines for bibliography annotation.

Generates four baselines for each abstract in the test set:
  - lead1     : first sentence
  - last1     : last sentence
  - textrank  : highest-PageRank sentence (TF-IDF cosine similarity graph)
  - ext_oracle: sentence with highest ROUGE-L vs reference (extractive upper bound)

Outputs one .txt file per baseline in generated_annotations/, matching the
line-per-row format used by gen_llm_sum.py and gen_bart_sum.py.
"""

import argparse
import os
import re
import numpy as np
import pandas as pd
import networkx as nx
from nltk.tokenize import sent_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from evaluate import load as load_metric
import tqdm

RESULT_DIR = "data/paper_html_10.1038/abs_annotation/generated_annotations"


# ── Sentence splitting ─────────────────────────────────────────────────────────
def split_sentences(text: str) -> list[str]:
    sents = sent_tokenize(text)
    return [s.strip() for s in sents if s.strip()]


# ── TextRank ───────────────────────────────────────────────────────────────────
def textrank_best(sentences: list[str]) -> str:
    if len(sentences) == 1:
        return sentences[0]
    try:
        tfidf = TfidfVectorizer().fit_transform(sentences)
        sim = cosine_similarity(tfidf)
        np.fill_diagonal(sim, 0)
        G = nx.from_numpy_array(sim)
        scores = nx.pagerank(G, max_iter=200)
        return sentences[max(scores, key=scores.get)]
    except Exception:
        return sentences[0]


# ── Oracle best sentence (ROUGE-L) ────────────────────────────────────────────
def oracle_best(sentences: list[str], reference: str, rouge) -> str:
    if len(sentences) == 1:
        return sentences[0]
    scores = [
        rouge.compute(predictions=[s], references=[reference], use_stemmer=True)["rougeL"]
        for s in sentences
    ]
    return sentences[int(np.argmax(scores))]


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--abstract_type", default="full",
                        choices=["full", "sent_shuffle", "tail"])
    parser.add_argument("--baselines", nargs="+",
                        default=["lead1", "last1", "textrank", "ext_oracle"],
                        help="Which baselines to generate")
    args = parser.parse_args()

    if args.abstract_type == "full":
        test_path = "data/paper_html_10.1038/abs_annotation/test.tsv"
    else:
        test_path = f"data/paper_html_10.1038/abs_annotation/test_{args.abstract_type}.tsv"

    df = pd.read_csv(test_path, sep="\t")
    abstracts   = df["abstract"].tolist()
    annotations = df["annotation"].tolist()
    suffix = f"_{args.abstract_type}" if args.abstract_type != "full" else ""

    os.makedirs(RESULT_DIR, exist_ok=True)

    # Only load ROUGE if oracle baseline requested
    rouge = None
    if "ext_oracle" in args.baselines:
        rouge = load_metric("rouge")

    # Open output files
    handles = {}
    for bl in args.baselines:
        path = os.path.join(RESULT_DIR, f"{bl}{suffix}.txt")
        handles[bl] = open(path, "w", encoding="utf-8")
    print(f"Writing baselines: {args.baselines}")

    try:
        for abstract, annotation in tqdm.tqdm(
            zip(abstracts, annotations), total=len(abstracts), unit="row"
        ):
            sents = split_sentences(abstract)
            if not sents:
                sents = [abstract]

            results = {}
            if "lead1"      in args.baselines:
                results["lead1"]      = sents[0]
            if "last1"      in args.baselines:
                results["last1"]      = sents[-1]
            if "textrank"   in args.baselines:
                results["textrank"]   = textrank_best(sents)
            if "ext_oracle" in args.baselines:
                results["ext_oracle"] = oracle_best(sents, annotation, rouge)

            for bl, text in results.items():
                handles[bl].write(" ".join(text.split()) + "\n")
    finally:
        for f in handles.values():
            f.close()

    print("Done.")


if __name__ == "__main__":
    main()
