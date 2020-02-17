import csv
from csv_file_importer import scan_file
from concurrent.futures import ProcessPoolExecutor
import timeit
import time


def third(data):
    materials = [record['material'] for record in data]
    dict_materials = {}
    begin_date_list = [record['BeginDate'] for record in data]
    set_of_date = set(begin_date_list)
    set_of_date.remove('')
    date_to_index_dict = {}
    mistakes = []

    for date in set_of_date:
        if date.isdigit():
            date_to_index_dict[date] = [index_date for index_date in range(len(begin_date_list)) if
                                        begin_date_list[index_date] == date]
        else:
            mistakes.append(date)

    for date in mistakes:
        set_of_date.remove(date)

    date_to_materials_dict = {}
    for date in set_of_date:
        materials_dict = {}
        for index in date_to_index_dict[date]:
            for one_material in materials[index].split(';'):
                try:
                    materials_dict[one_material] = materials_dict[one_material] + 1
                except KeyError:
                    materials_dict[one_material] = 1
        date_to_materials_dict[date] = materials_dict
    return date_to_materials_dict


def main():
    data_dict = scan_file('cstmc-CSV-en.csv')
    files = []
    prev_iter = 0

    for iterator in range(30000, len(data_dict), 30000):
        files.append(data_dict[prev_iter:iterator])
        prev_iter = iterator
    files.append(data_dict[prev_iter:])

    executor = ProcessPoolExecutor(max_workers=len(files))
    result = list(executor.map(third, files))

    all_date = []

    for one_date in [list(date_dict.keys()) for date_dict in result]:
        all_date += one_date

    all_date = list(set(all_date))
    all_date.sort()
    output_array = [[], [], []]

    for date in all_date:
        materials = {}
        for one_resulted_dict in result:
            if date in list(one_resulted_dict.keys()):
                for key_of_subdict in list(one_resulted_dict[date].keys()):
                    if key_of_subdict not in list(materials.keys()):
                        materials[key_of_subdict] = one_resulted_dict[date][key_of_subdict]
                    else:
                        materials[key_of_subdict] += one_resulted_dict[date][key_of_subdict]
        output_array[0] += [date] * len(materials.keys())
        output_array[1] += list(materials.keys())
        output_array[2] += list(materials.values())

    file_output = open('material-stats.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['Date', 'Material', 'Count'])
    writer.writeheader()
    for index in range(len(output_array[0])):
        writer.writerow({'Date': output_array[0][index], 'Material': output_array[1][index],
                         'Count': output_array[2][index]})
    file_output.close()


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
