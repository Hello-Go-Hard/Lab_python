import csv
from csv_file_importer import scan_file
import time
from ImmutableDict import ImmutableDict


def separate_data(data, count_of_buckets):
    buckets = tuple()
    len_of_one_file = len(data) // count_of_buckets
    remainder = len(data) % count_of_buckets  # остаток от деления
    for iteration in range(count_of_buckets):
        if remainder != 0:
            adding_index = 1
            remainder -= 1
        else:
            adding_index = 0
        part_file = data[int(iteration * len_of_one_file):int((iteration + 1) * len_of_one_file + adding_index)]
        buckets = tuple(buckets + (part_file,))
    return buckets


def write_to_csv(data, file_name):
    with open(file_name, 'w', newline='') as file_output:
        writer = csv.DictWriter(file_output, fieldnames=tuple(data[0].keys()), delimiter='|')
        writer.writeheader()
        for row in data:
            writer.writerow(row)
