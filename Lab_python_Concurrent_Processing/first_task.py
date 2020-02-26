import csv
from csv_file_importer import scan_file,write_into_csv
from concurrent.futures import ProcessPoolExecutor
import time
import os
from file_cutter import separate_data, write_to_csv
from ImmutableDict import ImmutableDict, set_dict_value


def max_number_of_count(data):
    count_of_components = tuple(record.get('NumberOfComponents') for record in data)
    set_of = set(count_of_components)
    unknown_components = (count_of_components.count(""), 'Unknown')
    unknown_count = 0
    max_components = (0, 0)
    error_id = False
    for value in range(len(count_of_components)):
        if not str(count_of_components[value]).isdigit():
            count_of_components = count_of_components[:value] + (1,) + count_of_components[value + 1:]
            unknown_count += 1
            error_id = True
        if error_id:
            error_id = False
            continue

    for index in range(len(count_of_components)):
        if int(count_of_components[index]) > int(max_components[1]):
            max_components = (index, count_of_components[index])
    max_components = (max_components[1], tuple(record.get('ObjectName') for record in data)[max_components[0]])
    return unknown_components, max_components


def min_weight_artifact(data):
    start_func = time.time()
    weight_tuple = tuple(record.get('Weight') for record in data)
    type_of_weight = ('lbs', 'kg', 'Metric tons', 'gm')
    convert_weight = ImmutableDict({'lbs': 0.453592, 'kg': 1, 'Metric tons': 1000, 'gm': 0.001})
    unknown_weight = 0

    for index in range(len(weight_tuple)):
        weight = weight_tuple[index]
        if weight is None:
            weight = ''
        elif '.' in weight and ',' in weight:
            weight = weight[(weight.find(',') + 1):]
        weight_tuple = weight_tuple[:index] + (weight.replace(',', '.'),) + weight_tuple[index + 1:]

    start = time.time()
    for index in range(len(weight_tuple)):
        count_digit = 0
        for second_index in range(len(weight_tuple[index])):
            if weight_tuple[index][second_index].isdigit() or weight_tuple[index][second_index] == '.':
                count_digit += 1
            else:
                if weight_tuple[index][second_index + 1].isdigit():
                    continue
                else:
                    break
        if count_digit == 0:
            unknown_weight += 1
            weight_tuple = weight_tuple[:index] + (1000,) + weight_tuple[index + 1:]
        else:
            for id_convert in type_of_weight:
                if id_convert in str(weight_tuple[index]):
                    convert_value = convert_weight.get(id_convert)
                    digit_weight = float(weight_tuple[index][:count_digit])
                    weight_tuple = weight_tuple[:index] + (convert_value * digit_weight,) + weight_tuple[index + 1:]
                if 'cm' in str(weight_tuple[index]):
                    weight_tuple = weight_tuple[:index] + (1000,) + weight_tuple[index + 1:]
    min_weight = min(weight_tuple)
    print('3 ' + str(time.time() - start))
    print('func '+str(time.time()-start_func))

    return (unknown_weight, 'UnKnown'), (min_weight,
                                         tuple(record.get('ObjectName') for record in data)
                                         [weight_tuple.index(min(weight_tuple))])


def country_with_max_artifacts(data):
    countries = tuple(record.get('ManuCountry') for record in data)
    set_of_countries = set(countries)
    unknown_country = ('Unknown', countries.count('') + countries.count('Unknown'))
    set_of_countries.remove('')
    set_of_countries.remove('Unknown')
    max_country = ('country', 1)
    for country in set_of_countries:
        if countries.count(country) > max_country[1]:
            max_country = (country, countries.count(country))
    return (unknown_country[1], 'Unknown'), (max_country[1], max_country[0])


def max_begin_end_date(data):
    begin_date_list = tuple(record.get('BeginDate') for record in data)
    end_date_list = tuple(record.get('EndDate') for record in data)
    countries = tuple(record.get('ManuCountry') for record in data)

    new_column = ()
    unknown_date = 0
    for (date_of_begin, date_of_end) in zip(begin_date_list, end_date_list):
        if not date_of_begin.isdigit() or not date_of_end.isdigit():
            unknown_date += 1
            new_column = new_column + (0,)
        else:
            new_column = new_column + (int(date_of_end) - int(date_of_begin),)
    unknown_date = (unknown_date, 'Unknown')
    max_time = max(new_column)
    max_time = (max_time, countries[new_column.index(max_time)])
    return unknown_date, max_time


def structuring_of_output(func_object, files, executor):
    values = ()
    columns = ()
    result = tuple(executor.map(func_object, files))
    unknown = tuple(row[0] for row in result)
    named = tuple(row[1] for row in result)
    values = values + (sum(tuple(row[0] for row in unknown)),)
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
    files_data = separate_data(data_dict, os.cpu_count())
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

    write_into_csv('general-stats.csv', ['Name', 'Value'], [output_data_c, output_data_v])


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
