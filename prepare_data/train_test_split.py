import csv
import random
import argparse
from collections import defaultdict

parser = argparse.ArgumentParser(description='Split data into BART train, ICL, and test sets, grouping by abstract DOI.')
parser.add_argument('--num_train', type=int, required=True, help='Target number of training pairs (BART + ICL)')
parser.add_argument('--num_icl', type=int, default=5, help='Number of ICL examples to select (default: 5)')
parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility (default: 42)')
args = parser.parse_args()

input_file = 'data/paper_html_10.1038/abs_annotation/abs_annotation.tsv'
train_file = 'data/paper_html_10.1038/abs_annotation/train.tsv'
icl_file = 'data/paper_html_10.1038/abs_annotation/icl.tsv'
test_file = 'data/paper_html_10.1038/abs_annotation/test.tsv'

random.seed(args.seed)

with open(input_file, 'r', encoding='utf-8') as f:
    reader = list(csv.reader(f, delimiter='\t'))
    header = reader[0]
    rows = reader[1:]

if args.num_train > len(rows):
    raise ValueError("the length of the dataset is smaller than the specified number of training samples")
if args.num_icl >= args.num_train:
    raise ValueError("num_icl must be smaller than num_train")

# Group rows by abs_doi (column 0), preserving original order within each group
groups = defaultdict(list)
for row in rows:
    groups[row[0]].append(row)

group_keys = list(groups.keys())
random.shuffle(group_keys)

# Greedily select abstract groups for the training pool until reaching num_train pairs
train_keys = []
train_count = 0
for key in group_keys:
    if train_count >= args.num_train:
        break
    train_keys.append(key)
    train_count += len(groups[key])

train_keys_set = set(train_keys)

# Select ICL abstract groups from the training pool.
# Prefer single-annotation abstracts for cleaner ICL examples.
single_keys = [k for k in train_keys if len(groups[k]) == 1]
multi_keys = [k for k in train_keys if len(groups[k]) > 1]

if len(single_keys) >= args.num_icl:
    icl_keys = random.sample(single_keys, args.num_icl)
elif len(train_keys) >= args.num_icl:
    icl_keys = single_keys + random.sample(multi_keys, args.num_icl - len(single_keys))
else:
    raise ValueError("Not enough abstract groups in the training pool to select ICL examples")

# ICL rows: one representative row (first annotation) per selected ICL abstract group
icl_rows = [groups[k][0] for k in icl_keys]

# Train rows: all pairs in the training pool
train_rows = [row for key in train_keys for row in groups[key]]

# Test rows: all pairs whose abstract is not in the training pool
test_rows = [row for key in group_keys if key not in train_keys_set for row in groups[key]]


def write_tsv(filepath, header, rows):
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(header)
        writer.writerows(rows)


write_tsv(train_file, header, train_rows)
write_tsv(icl_file, header, icl_rows)
write_tsv(test_file, header, test_rows)

print(f"Training pairs : {len(train_rows):>6}  (across {len(train_keys)} unique abstracts)")
print(f"ICL examples   : {len(icl_rows):>6}  (across {len(icl_keys)} unique abstracts)")
print(f"Test pairs     : {len(test_rows):>6}  (across {len(group_keys) - len(train_keys)} unique abstracts)")
print(f"Total          : {len(train_rows) + len(test_rows):>6}  pairs")