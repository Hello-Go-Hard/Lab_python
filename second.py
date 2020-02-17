import csv
from csv_file_importer import scan_file
from concurrent.futures import ProcessPoolExecutor
from tail_recursion import tail_recursive, recurse
import time


@tail_recursive
def init_categories(array_data, field_name, accumulator, iterator=0):
    if iterator != len(array_data):
        accumulator.append(array_data[iterator][field_name])
        recurse(array_data, field_name, accumulator, (iterator + 1))
    else:
        return accumulator


@tail_recursive
def dict_categories(dict_data, array_categories, set_categories, iterator=0):
    if iterator != len(set_categories):
        dict_data[set_categories[iterator]] = array_categories.count(set_categories[iterator])
        recurse(dict_data, array_categories, set_categories, iterator+1)
    else:
        return dict_data


def category_count(data):
    categories_1 = init_categories(data, 'category1', accumulator=[])
    categories_2 = init_categories(data, 'category2', accumulator=[])
    categories_3 = init_categories(data, 'category3', accumulator=[])
    categories = categories_1 + categories_2 + categories_3
    set_of_categories = list(set(categories))
    dict_of_categories = {}
    dict_of_categories = dict_categories(dict_of_categories, categories, set_of_categories)
    return dict_of_categories


def main():
    data_dict = scan_file('cstmc-CSV-en.csv')
    files = []
    prev_iter = 0
    for iterator in range(30000, len(data_dict), 30000):
        files.append(data_dict[prev_iter:iterator])
        prev_iter = iterator
    files.append(data_dict[prev_iter:])
    executor = ProcessPoolExecutor(max_workers=len(files))
    results = executor.map(category_count, files)
    dicts_of_category = [one_dict for one_dict in results]
    keys_of_dicts = [list(one_dict.keys()) for one_dict in dicts_of_category]
    all_keys = []
    for one_list in keys_of_dicts:
        all_keys += one_list
    all_keys = set(all_keys)
    output_dict = {key: 0 for key in all_keys}
    for one_dict in dicts_of_category:
        for one_key in list(one_dict.keys()):
            output_dict[one_key] += one_dict[one_key]

    file_output = open('object-stats.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['Category', 'Count'])
    writer.writeheader()

    for (category, count_of_category) in zip(list(output_dict.keys()), list(output_dict.values())):
        writer.writerow({'Category': category, 'Count': count_of_category})
    file_output.close()
    print('Чай с индийскими специями')


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
