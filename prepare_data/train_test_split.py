import csv
import random
import argparse

parser = argparse.ArgumentParser(description='Split data into train and test sets.')
parser.add_argument('--num_train', type=int, required=True, help='Number of training samples to select')
args = parser.parse_args()

input_file = 'data/paper_html_10.1038/abs_annotation/abs_annotation.tsv'
train_file = 'data/paper_html_10.1038/abs_annotation/train.tsv'
test_file = 'data/paper_html_10.1038/abs_annotation/test.tsv'
num_train = args.num_train

with open(input_file, 'r', encoding='utf-8') as f:
    reader = list(csv.reader(f, delimiter='\t'))
    header = reader[0]
    rows = reader[1:]

if num_train > len(rows):
    raise ValueError("训练集行数大于总数据行数")

train_rows = random.sample(rows, num_train)
test_rows = [row for row in rows if row not in train_rows]

with open(train_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(header)
    writer.writerows(train_rows)

with open(test_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(header)
    writer.writerows(test_rows)