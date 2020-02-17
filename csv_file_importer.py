import csv


def scan_file(file_name):
    data_dict = []
    with open(file_name, 'r') as source:
        reader = csv.DictReader(source, delimiter='|')
        for row in reader:
            if len(row.keys()) != 36:
                del row[None]
            data_dict.append(row)
    return data_dict
































