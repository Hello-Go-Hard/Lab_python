import csv
from csv_file_importer import scan_file,write_into_csv
from concurrent.futures import ProcessPoolExecutor
from tail_recursion import tail_recursive, recurse
import time
import os
from file_cutter import separate_data, write_to_csv
from ImmutableDict import ImmutableDict, set_dict_value


@tail_recursive
def dict_categories(dict_data, array_categories, set_categories, iterator=0):
    if iterator != len(set_categories):
        dict_data[set_categories[iterator]] = array_categories.count(set_categories[iterator])
        recurse(dict_data, array_categories, set_categories, iterator+1)
    else:
        return dict_data


def category_count(data):
    categories_1 = [record.get('category1') for record in data]
    categories_2 = [record.get('category2') for record in data]
    categories_3 = [record.get('category3') for record in data]
    categories = categories_1 + categories_2 + categories_3
    set_of_categories = list(set(categories))
    dict_of_categories = {}
    dict_of_categories = dict_categories(dict_of_categories, categories, set_of_categories)
    return dict_of_categories


def data_init(file_name, executor):
    start = time.time()
    data_dict = scan_file('cstmc-CSV-en.csv')

    files_data = separate_data(data_dict, os.cpu_count())
    file_names = tuple(str(index) + '_input.csv' for index in range(os.cpu_count()))

    result = executor.map(write_to_csv, list(files_data), file_names)
    results = executor.map(scan_file, file_names)

    output = list(results)
    print(time.time() - start)
    return output


def main():
    executor = ProcessPoolExecutor(max_workers=os.cpu_count())
    files = data_init('cstmc-CSV-en.csv', executor)

    results = executor.map(category_count, files)
    dicts_of_category = tuple(one_dict for one_dict in results)
    keys_of_dicts = tuple(list(one_dict.keys()) for one_dict in dicts_of_category)
    all_keys = tuple()
    for one_tuple in keys_of_dicts:
        all_keys = tuple(all_keys + tuple(one_tuple))
    all_keys = set(all_keys)
    output_dict = ImmutableDict({key: 0 for key in all_keys})
    for one_dict in dicts_of_category:
        for one_key in list(one_dict.keys()):
            output_dict = set_dict_value(output_dict, one_key, one_dict.get(one_key) + output_dict.get(one_key))

    write_into_csv('object-stats.csv', ['Category', 'Count'], [list(output_dict.keys()), list(output_dict.values())])


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
