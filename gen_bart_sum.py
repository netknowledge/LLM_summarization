import os
import argparse
import torch
import torch.multiprocessing as mp
import pandas as pd
import tqdm
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

CHECKPOINT_DIR   = "data/BART/checkpoints/best_model"
MAX_INPUT        = 1024
MAX_NEW_TOKENS   = 128
DEFAULT_BATCH    = 64   # ~14 GB KV cache on L40 (46 GB), well within budget


def _worker(rank: int, gpu_id: int, abstracts: list, ckpt: str,
            batch_size: int, tmp_path: str):
    """Runs generation on a single GPU and writes results to tmp_path."""
    device = f"cuda:{gpu_id}"
    tokenizer = AutoTokenizer.from_pretrained(ckpt, local_files_only=True)
    model = AutoModelForSeq2SeqLM.from_pretrained(
        ckpt, local_files_only=True, dtype=torch.bfloat16
    ).to(device).eval()
    model.generation_config.min_length = 0
    model.generation_config.no_repeat_ngram_size = 0

    results = []
    for batch_start in tqdm.tqdm(
        range(0, len(abstracts), batch_size),
        desc=f"GPU {gpu_id}", position=rank, leave=True, unit="batch",
    ):
        batch = abstracts[batch_start : batch_start + batch_size]
        inputs = tokenizer(
            batch, return_tensors="pt",
            max_length=MAX_INPUT, truncation=True, padding=True,
        ).to(device)
        with torch.no_grad():
            outputs = model.generate(**inputs, max_new_tokens=MAX_NEW_TOKENS, num_beams=4)
        decoded = tokenizer.batch_decode(outputs, skip_special_tokens=True)
        results.extend(" ".join(t.split()) for t in decoded)

    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write("\n".join(results) + ("\n" if results else ""))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint", default=CHECKPOINT_DIR,
                        help="Path to fine-tuned BART checkpoint directory")
    parser.add_argument("--model_name", default="bart",
                        help="Label used in the output filename (default: bart)")
    parser.add_argument("--start_index", type=int, default=0,
                        help="Start processing test data row number (starting from 0)")
    parser.add_argument("--abstract_type", type=str, default="full",
                        choices=["full", "sent_shuffle", "tail"],
                        help="Type of abstract to use (default: full)")
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH,
                        help=f"Per-GPU batch size (default: {DEFAULT_BATCH}; "
                             "safe up to ~128 on L40 46 GB with num_beams=4)")
    parser.add_argument("--gpus", type=int, nargs="+", default=[0],
                        help="GPU indices to use, e.g. --gpus 0 1 2 3")
    args = parser.parse_args()

    # ── Output path (mirrors gen_llm_sum.py naming convention) ────────────────
    result_dir = "data/paper_html_10.1038/abs_annotation/generated_annotations"
    suffix = f"_{args.abstract_type}" if args.abstract_type != "full" else ""
    output_path = os.path.join(result_dir, f"{args.model_name}{suffix}.txt")
    os.makedirs(result_dir, exist_ok=True)

    # ── Test set ───────────────────────────────────────────────────────────────
    if args.abstract_type == "full":
        test_path = "data/paper_html_10.1038/abs_annotation/test.tsv"
    else:
        test_path = f"data/paper_html_10.1038/abs_annotation/test_{args.abstract_type}.tsv"
    test_df = pd.read_csv(test_path, sep="\t")
    abstracts = test_df["abstract"].tolist()

    ckpt          = os.path.abspath(args.checkpoint)
    gpu_ids       = args.gpus
    n_gpus        = len(gpu_ids)
    abstracts_todo = abstracts[args.start_index:]

    print(f"Checkpoint  : {ckpt}")
    print(f"GPUs        : {gpu_ids}  |  batch_size per GPU: {args.batch_size}")
    print(f"Samples     : {len(abstracts_todo)}  |  Output: {output_path}")

    if n_gpus == 1:
        # ── Single-GPU path: append mode supports --start_index resume ────────
        gpu_id = gpu_ids[0]
        device = f"cuda:{gpu_id}" if torch.cuda.is_available() else "cpu"
        tokenizer = AutoTokenizer.from_pretrained(ckpt, local_files_only=True)
        model = AutoModelForSeq2SeqLM.from_pretrained(
            ckpt, local_files_only=True, dtype=torch.bfloat16
        ).to(device).eval()
        model.generation_config.min_length = 0
        model.generation_config.no_repeat_ngram_size = 0

        with open(output_path, "a", encoding="utf-8") as f_out:
            for batch_start in tqdm.tqdm(
                range(0, len(abstracts_todo), args.batch_size),
                desc=f"GPU {gpu_id}", unit="batch",
            ):
                batch = abstracts_todo[batch_start : batch_start + args.batch_size]
                inputs = tokenizer(
                    batch, return_tensors="pt",
                    max_length=MAX_INPUT, truncation=True, padding=True,
                ).to(device)
                with torch.no_grad():
                    outputs = model.generate(
                        **inputs, max_new_tokens=MAX_NEW_TOKENS, num_beams=4,
                    )
                decoded = tokenizer.batch_decode(outputs, skip_special_tokens=True)
                for text in decoded:
                    f_out.write(" ".join(text.split()) + "\n")
                f_out.flush()

    else:
        # ── Multi-GPU path: split work across GPUs, merge afterwards ─────────
        chunk_size = (len(abstracts_todo) + n_gpus - 1) // n_gpus
        chunks     = [abstracts_todo[i * chunk_size : (i + 1) * chunk_size]
                      for i in range(n_gpus)]
        tmp_paths  = [f"{output_path}.part{i}" for i in range(n_gpus)]

        processes = []
        for rank, (gpu_id, chunk, tmp_path) in enumerate(
            zip(gpu_ids, chunks, tmp_paths)
        ):
            p = mp.Process(
                target=_worker,
                args=(rank, gpu_id, chunk, ckpt, args.batch_size, tmp_path),
            )
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
            if p.exitcode != 0:
                raise RuntimeError(f"Worker process exited with code {p.exitcode}")

        # Merge parts in order (append after any existing lines from start_index)
        with open(output_path, "a", encoding="utf-8") as f_out:
            for tmp_path in tmp_paths:
                with open(tmp_path, "r", encoding="utf-8") as f_in:
                    f_out.write(f_in.read())
                os.remove(tmp_path)

    print("Done.")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
