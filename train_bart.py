#!/usr/bin/env python3
"""
Fine-tune BART on the BiomedTLDR annotation dataset.

Launch (4 GPUs):
    torchrun --nproc_per_node=4 train_bart.py [options]

Single GPU (for quick sanity check):
    python3 train_bart.py [options]
"""

import csv
import os
import argparse

# Suppress tokenizer parallelism warning when DataLoader forks workers
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

import numpy as np
import evaluate
from datasets import Dataset
import transformers
from transformers import (
    AutoTokenizer,
    AutoModelForSeq2SeqLM,
    DataCollatorForSeq2Seq,
    Seq2SeqTrainer,
    Seq2SeqTrainingArguments,
    EarlyStoppingCallback,
)

# ── Defaults ───────────────────────────────────────────────────────────────────
TRAIN_FILE  = "data/paper_html_10.1038/abs_annotation/train.tsv"
OUTPUT_DIR  = "data/BART/checkpoints"
LOG_DIR     = "data/BART/logs"
MODEL_NAME  = "facebook/bart-large-cnn"   # CNN/DM checkpoint: good init for summarization
MAX_INPUT   = 1024   # BART maximum supported length
MAX_TARGET  = 128    # annotations are short (~20-60 words)


# ── Data loading ───────────────────────────────────────────────────────────────
def load_tsv(filepath: str) -> list[dict]:
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return [{"abstract": row["abstract"], "annotation": row["annotation"]}
                for row in reader]


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fine-tune BART for bibliography annotation")
    parser.add_argument("--model_name",            default=MODEL_NAME)
    parser.add_argument("--train_file",            default=TRAIN_FILE)
    parser.add_argument("--output_dir",            default=OUTPUT_DIR)
    parser.add_argument("--log_dir",               default=LOG_DIR)
    parser.add_argument("--num_epochs",            type=int,   default=10)
    parser.add_argument("--per_device_batch_size", type=int,   default=8)
    parser.add_argument("--grad_accum",            type=int,   default=2)
    parser.add_argument("--lr",                    type=float, default=1e-5)
    parser.add_argument("--warmup_ratio",          type=float, default=0.1)
    parser.add_argument("--val_split",             type=float, default=0.05,
                        help="Fraction of train.tsv held out for validation")
    parser.add_argument("--early_stopping_patience", type=int, default=0,
                        help="0 = disabled; N>0 = stop after N epochs without improvement")
    parser.add_argument("--seed",                  type=int,   default=42)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.log_dir,    exist_ok=True)

    # ── Load & split data ──────────────────────────────────────────────────────
    records = load_tsv(args.train_file)
    dataset = Dataset.from_list(records).shuffle(seed=args.seed)
    splits   = dataset.train_test_split(test_size=args.val_split, seed=args.seed)
    train_ds, val_ds = splits["train"], splits["test"]

    print(f"Train size: {len(train_ds)}, Val size: {len(val_ds)}")

    # ── Tokenizer & model ──────────────────────────────────────────────────────
    tokenizer = AutoTokenizer.from_pretrained(args.model_name)
    model     = AutoModelForSeq2SeqLM.from_pretrained(args.model_name)

    # bart-large-cnn's GenerationConfig was tuned for long CNN/DM summaries.
    # Our annotations are short (~20-60 tokens), so remove the min_length=56
    # constraint and no_repeat_ngram_size=3 to avoid artificially inflated
    # generation that corrupts ROUGE scores and misleads best-checkpoint selection.
    model.generation_config.min_length = 0
    model.generation_config.no_repeat_ngram_size = 0

    def preprocess(batch):
        model_inputs = tokenizer(
            batch["abstract"],
            max_length=MAX_INPUT,
            truncation=True,
            padding=False,
        )
        labels = tokenizer(
            text_target=batch["annotation"],
            max_length=MAX_TARGET,
            truncation=True,
            padding=False,
        )
        model_inputs["labels"] = labels["input_ids"]
        return model_inputs

    train_ds = train_ds.map(
        preprocess, batched=True, remove_columns=["abstract", "annotation"],
        desc="Tokenising train"
    )
    val_ds = val_ds.map(
        preprocess, batched=True, remove_columns=["abstract", "annotation"],
        desc="Tokenising val"
    )

    data_collator = DataCollatorForSeq2Seq(
        tokenizer, model=model, label_pad_token_id=-100, pad_to_multiple_of=8
    )

    # ── Evaluation metric (ROUGE) ──────────────────────────────────────────────
    rouge = evaluate.load("rouge")

    def compute_metrics(eval_preds):
        preds, labels = eval_preds
        # preds are token ids when predict_with_generate=True
        preds  = np.where(preds  != -100, preds,  tokenizer.pad_token_id)
        labels = np.where(labels != -100, labels, tokenizer.pad_token_id)
        decoded_preds  = tokenizer.batch_decode(preds,  skip_special_tokens=True)
        decoded_labels = tokenizer.batch_decode(labels, skip_special_tokens=True)
        result = rouge.compute(
            predictions=decoded_preds,
            references=decoded_labels,
            use_stemmer=True,
        )
        return {k: round(v * 100, 4) for k, v in result.items()}

    # ── Training arguments ─────────────────────────────────────────────────────
    training_args = Seq2SeqTrainingArguments(
        output_dir=args.output_dir,

        # ---- optimisation ----
        num_train_epochs=args.num_epochs,
        per_device_train_batch_size=args.per_device_batch_size,
        per_device_eval_batch_size=args.per_device_batch_size * 2,
        gradient_accumulation_steps=args.grad_accum,
        learning_rate=args.lr,
        warmup_ratio=args.warmup_ratio,
        lr_scheduler_type="cosine",
        weight_decay=0.01,
        label_smoothing_factor=0.1,  # regularise against overconfidence on small dataset

        # ---- precision ----
        bf16=True,           # L40 has native BF16 support (Ada Lovelace)

        # ---- generation (for eval) ----
        predict_with_generate=True,
        generation_max_length=MAX_TARGET,

        # ---- checkpointing & logging ----
        eval_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="steps",
        logging_steps=10,
        save_total_limit=3,
        load_best_model_at_end=True,
        metric_for_best_model="eval_loss",
        greater_is_better=False,

        # ---- misc ----
        seed=args.seed,
        log_level="info",
        report_to="tensorboard",
        logging_dir=args.log_dir,
        ddp_find_unused_parameters=False,
        dataloader_num_workers=4,
    )

    # ── Trainer ────────────────────────────────────────────────────────────────
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        processing_class=tokenizer,
        data_collator=data_collator,
        compute_metrics=compute_metrics,
        callbacks=(
            [EarlyStoppingCallback(early_stopping_patience=args.early_stopping_patience)]
            if args.early_stopping_patience > 0 else []
        ),
    )

    print("=" * 60)
    print(f"Model:           {args.model_name}")
    print(f"Effective batch: {args.per_device_batch_size * args.grad_accum} × num_gpus")
    print(f"Epochs:          {args.num_epochs}  (early stopping patience={args.early_stopping_patience})")
    print(f"Checkpoints:     {args.output_dir}")
    print("=" * 60)

    trainer.train()

    # Save best model
    best_dir = os.path.join(args.output_dir, "best_model")
    trainer.save_model(best_dir)
    tokenizer.save_pretrained(best_dir)
    print(f"\nTraining complete. Best model saved to: {best_dir}")


if __name__ == "__main__":
    main()
