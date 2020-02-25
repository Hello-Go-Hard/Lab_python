import csv
import types
import time
from ImmutableDict import ImmutableDict


def scan_file(file_name):
    with open(file_name, 'r') as source:
        reader = csv.DictReader(source, delimiter='|')
        data_dict = tuple(ImmutableDict(row) for row in reader)
    for row in range(len(data_dict)):
        if len(list(data_dict[row].keys())) > 36:
            keys = tuple(data_dict[row].keys())
            one_dict = ImmutableDict({key: data_dict[row].get(key) for key in keys[:36]})
            data_dict = tuple(data_dict[:row] + (one_dict,) + data_dict[row + 1:])
    print('The end ' + file_name)
    return data_dict


def write_into_csv(filename, fieldnames, output_data):
    file_output = open(filename, 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=fieldnames)
    writer.writeheader()
    for index in range(len(output_data[0])):
        writer.writerow({fieldname: value for (fieldname, value) in zip(fieldnames,
                                                                        [column[index] for column in output_data])})
    file_output.close()


if __name__ == '__main__':
    start = time.time()
    output = scan_file('cstmc-CSV-en.csv')
    print(time.time() - start)
