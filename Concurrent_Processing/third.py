import csv
from csv_file_importer import scan_file, write_into_csv
from concurrent.futures import ProcessPoolExecutor
import time
import os
from file_cutter import separate_file, write_to_csv
from ImmutableDict import ImmutableDict, set_dict_value


def third(data):
    materials = tuple(record.get('material') for record in data)
    dict_materials = {}
    begin_date_tuple = tuple(record.get('BeginDate') for record in data)
    set_of_date = set(begin_date_tuple)
    set_of_date.remove('')
    date_to_index_dict = ImmutableDict()
    mistakes = ()

    for date in set_of_date:
        if date.isdigit():
            date_to_index_dict = set_dict_value(date_to_index_dict, date,
                                                tuple(index_date for index_date in range(len(begin_date_tuple)) if
                                                      begin_date_tuple[index_date] == date))
        else:
            mistakes = tuple(mistakes + (date,))

    for date in mistakes:
        set_of_date.remove(date)

    date_to_materials_dict = ImmutableDict()
    for date in set_of_date:
        materials_dict = ImmutableDict()
        for index in date_to_index_dict.get(date):
            for one_material in materials[index].split(';'):
                try:
                    materials_dict = set_dict_value(materials_dict, one_material, materials_dict.get(one_material) + 1)
                except TypeError:
                    materials_dict = set_dict_value(materials_dict, one_material, 1)
        date_to_materials_dict = set_dict_value(date_to_materials_dict, date, materials_dict)
    return date_to_materials_dict


def data_init(file_name, executor):
    data_dict = scan_file(file_name)
    files_data = separate_file(data_dict, os.cpu_count())
    file_names = ()
    for file_name in range(os.cpu_count()):
        file_names = (file_names + ((str(file_name) + '_input.csv'),))

    executor.map(write_to_csv, files_data, file_names)

    results = executor.map(scan_file, file_names)
    tuple_of_data = tuple(row for row in results)
    return tuple_of_data


def main():
    executor = ProcessPoolExecutor(max_workers=os.cpu_count())
    files = data_init('cstmc-CSV-en.csv', executor)

    result = tuple(executor.map(third, files))

    all_date = ()

    for one_date in [tuple(date_dict.keys()) for date_dict in result]:
        all_date = all_date + one_date

    all_date = list(set(all_date))
    all_date.sort()
    all_date = tuple(all_date)
    output_keys = ()
    output_values = ()
    output_date = ()

    for date in all_date:
        materials = ImmutableDict()
        for one_resulted_dict in result:
            if date in tuple(one_resulted_dict.keys()):
                for key_of_subdict in list(one_resulted_dict.get(date).keys()):
                    if key_of_subdict not in tuple(materials.keys()):
                        materials = set_dict_value(materials, key_of_subdict,
                                                   one_resulted_dict.get(date).get(key_of_subdict))
                    else:
                        materials = set_dict_value(materials, key_of_subdict,
                                                   one_resulted_dict.get(date).get(key_of_subdict) +
                                                   materials.get(key_of_subdict))

        output_date = output_date + (date,) * len(materials.keys())
        output_keys = tuple(output_keys + tuple(materials.keys()))
        output_values = tuple(output_values + tuple(materials.values()))

    write_into_csv('material-stats.csv', ['Date', 'Material', 'Count'], [output_date, output_keys, output_values])


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
