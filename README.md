# Abstract 
High-quality scientific extreme summary (TLDR) facilitates effective science communication. How do large language models (LLMs) perform in generating them? How are LLM-generated summaries different from those written by human experts? However, the lack of a comprehensive, high-quality scientific TLDR dataset hinders both the development and evaluation of LLMs' summarization ability. To address these, we propose a novel dataset, BiomedTLDR, containing a large sample of researcher-authored summaries from scientific papers, which leverages the common practice of including authors' comments alongside bibliography items. We then test popular open-weight LLMs for generating TLDRs based on abstracts. Our analysis reveals that, although some of them successfully produce humanoid summaries, LLMs generally exhibit a greater affinity for the original text's lexical choices and rhetorical structures, hence tend to be more extractive rather than abstractive in general, compared to humans.

# File location

## Data
1. **LLM-generated summaries**: `data/paper_html_10.1038/abs_annotation/generated_annotations`

2. **Human-authored summaries**: `data/BiomedTLDR_dataset.tsv`

## Experiment code

1. **LLMs are more extractive summarizer**: `evaluation/ref_based`
2. **LLMs outperform human consistently**: `evaluation/predict_task`
3. **LLMs mirror human’s citation bias**: `bias`
