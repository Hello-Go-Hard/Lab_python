import csv
from csv_file_importer import scan_file
import time
from ImmutableDict import ImmutableDict


def separate_file(data, count_of_files):
    files = tuple()
    len_of_one_file = len(data) // count_of_files
    remainder = len(data) % count_of_files  # остаток от деления
    for iteration in range(count_of_files):
        if remainder != 0:
            adding_index = 1
            remainder -= 1
        else:
            adding_index = 0
        files = tuple(files + (data[int(iteration * len_of_one_file):
                          int((iteration + 1) * len_of_one_file + adding_index)]
                     ,))
    return files


def write_to_csv(data, file_name):
    print('i am '+file_name)
    file_output = open(file_name, 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=tuple(data[0].keys()), delimiter='|')
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    print('write '+file_name)
    file_output.close()
    return True


if __name__ == '__main__':
    start = time.time()
    data_dict = scan_file('cstmc-CSV-en.csv')
    files = separate_file(data_dict, 4)
    write_to_csv(files[0], 'first_csv_output.csv')
    print('The end')
    print(time.time() - start)