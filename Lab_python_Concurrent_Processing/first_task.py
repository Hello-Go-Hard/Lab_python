import csv
from csv_file_importer import scan_file, write_into_csv
from concurrent.futures import ProcessPoolExecutor
import time
import os
from file_cutter import separate_file, write_to_csv
from ImmutableDict import ImmutableDict, set_dict_value
from functools import reduce


def max_number_of_count(data):
    count_of_components = list(map(lambda record: record.get('NumberOfComponents'), data))
    unknown_components = len(list(filter(lambda x: x == '', count_of_components)))
    count_of_components = map(lambda x: x if x.isdigit() else 1, count_of_components)
    count_of_components = enumerate(count_of_components)
    max_components = max(count_of_components, key=lambda x: int(x[1]))
    max_components = (max_components[1], tuple(map(lambda x: x.get('ObjectName'), data))[max_components[0]])
    unknown_components = (unknown_components, "Unknown")
    return unknown_components, max_components


def min_weight_artifact(data):
    start_func = time.time()
    weight_tuple = tuple(map(lambda x: x.get('Weight'), data))
    type_of_weight = ('lbs', 'kg', 'Metric tons', 'gm')
    convert_weight = ImmutableDict({'lbs': 0.453592, 'kg': 1, 'Metric tons': 1000, 'gm': 0.001, 'tons': 1000})

    weight_tuple = tuple(map(lambda weight: '' if weight is None else weight, weight_tuple))
    weight_tuple = tuple(map(lambda weight: weight[(weight.find(',') + 1):]
                                            if '.' in weight and ',' in weight
                                            else weight, weight_tuple))

    weight_tuple = tuple(map(lambda weight: weight.replace(',', '.'), weight_tuple))

    unknown_weight = len(list(filter(lambda weight_str: max(list(map(lambda x: 1 if x in weight_str else 0,
                                                                     type_of_weight))) == 0, weight_tuple)))

    weight_tuple = tuple(map(lambda weight_str: '1000 kg'
                             if max(list(map(lambda x: 1 if x in weight_str else 0,
                                             type_of_weight))) == 0 else weight_str, weight_tuple))
    weight_tuple = tuple(map(lambda x: x[:x.find('(')] + x[x.find(')') + 1:] if '(' in x else x, weight_tuple))
    weight_pairs = tuple(map(lambda x: (list(x.split(' '))[0],) + (list(x.split(' '))[-1],), weight_tuple))
    weight_pairs = tuple(map(lambda x: ('1000','kg') if x[0] == '' else x, weight_pairs))

    weight_kg = tuple(map(lambda x: float(x[0])*convert_weight.get(x[1]), weight_pairs))

    weight_kg = enumerate(weight_kg)
    min_value = min(weight_kg, key=lambda x: x[1])

    return (unknown_weight, 'UnKnown'), (min_value[1],
                                         tuple(map(lambda x: x.get('ObjectName'), data))
                                         [min_value[0]])


def country_with_max_artifacts(data):
    countries = tuple(map(lambda x: x.get('ManuCountry'), data))
    set_of_countries = set(countries)
    unknown_country = ('Unknown', countries.count('') + countries.count('Unknown'))
    set_of_countries.remove('')
    set_of_countries.remove('Unknown')
    max_country = ('country', 1)

    pair_count = map(lambda x: (countries.count(x), x), set_of_countries)
    max_country = max(list(pair_count))

    return (unknown_country[1], 'Unknown'), (max_country[1], max_country[0])


def max_begin_end_date(data):
    begin_date_list = tuple(map(lambda x: x.get('BeginDate'), data))
    end_date_list = tuple(map(lambda x: x.get('EndDate'), data))
    countries = tuple(map(lambda x: x.get('ObjectName'), data))

    new_column = list(map(lambda begin, end: 0 if not begin.isdigit() or not end.isdigit()
    else int(end) - int(begin), begin_date_list, end_date_list))

    unknown_date = (new_column.count(0), 'Unknown')
    max_time = max(new_column)
    max_time = (max_time, countries[new_column.index(max_time)])
    return unknown_date, max_time


def structuring_of_output(func_object, files, executor):
    values = ()
    columns = ()
    result = tuple(executor.map(func_object, files))
    unknown = tuple(row[0] for row in result)
    named = tuple(row[1] for row in result)
    values = values + (sum(tuple(int(row[0]) for row in unknown)),)
    columns = columns + ('Unknown',)
    if 'max' in func_object.__name__:
        named_count = max(tuple(row[0] for row in named))
    else:
        named_count = min((row[0] for row in named))
    values = values + (named_count,)
    columns = columns + (named[[row[0] for row in named].index(named_count)][1],)
    return values, columns


def data_request(files, executor):
    columns = ()
    values = ()

    func_list = (max_number_of_count, max_begin_end_date, min_weight_artifact, country_with_max_artifacts)
    for func in func_list:
        some_values, some_columns = structuring_of_output(func, files, executor)
        values = values + (some_values,)
        columns = columns + (some_columns,)
    return columns, values


def data_init(file_name, executor):
    data_dict = scan_file('cstmc-CSV-en.csv')
    files = ()
    prev_iter = 0
    files_data = separate_file(data_dict, os.cpu_count())
    file_names = ()
    for file_name in range(os.cpu_count()):
        file_names = file_names + ((str(file_name) + '_input.csv'),)

    executor.map(write_to_csv, files_data, file_names)

    results = executor.map(scan_file, file_names)
    list_of_data = tuple(row for row in results)
    return list_of_data


def main():
    executor = ProcessPoolExecutor(max_workers=os.cpu_count())
    files = data_init('cstmc-CSV-en.csv', executor)
    output_data_column = ()
    output_data_value = ()
    prev_iter = 0
    output_data_column, output_data_value = data_request(files, executor)

    output_data_c = []
    output_data_v = []
    for (column_row, value_row) in zip(output_data_column, output_data_value):
        output_data_c += column_row
        output_data_v += value_row

    write_into_csv('general-stats1.csv', ['Name', 'Value'], [output_data_c, output_data_v])


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
