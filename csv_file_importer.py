import csv


def scan_file(file_name):
    file_csv = open(file_name, 'r')
    data_dict = []

    reader = csv.DictReader(file_csv, delimiter='|')
    for row in reader:
        if len(row.keys()) != 36:
            del row[None]
        data_dict.append(row)
    file_csv.close()
    return data_dict
