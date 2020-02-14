import csv
from csv_file_importer import scan_file
from concurrent.futures import ProcessPoolExecutor
import time


def max_number_of_count(data):
    count_of_components = [record['NumberOfComponents'] for record in data]
    set_of = set(count_of_components)
    unknown_components = (count_of_components.count(""), 'Unknown')
    unknown_count = 0
    max_components = (0, 0)
    error_id = False
    for value in range(len(count_of_components)):
        if not str(count_of_components[value]).isdigit():
            count_of_components[value] = 1
            unknown_count += 1
            error_id = True
        if error_id:
            error_id = False
            continue

    for index in range(len(count_of_components)):
        if int(count_of_components[index]) > int(max_components[1]):
            max_components = (index, count_of_components[index])
    max_components = (max_components[1], [record['ObjectName'] for record in data][max_components[0]])
    return unknown_components, max_components


def min_weight_artifact(data):
    weight_list = [record['Weight'] for record in data]
    type_of_weight = ['lbs', 'kg', 'Metric tons', 'gm']
    convert_weight = {'lbs': 0.453592, 'kg': 1, 'Metric tons': 1000, 'gm': 0.001}
    unknown_weight = 0
    for index in range(len(weight_list)):
        weight = weight_list[index]
        if weight is None:
            weight = ''
        elif '.' in weight and ',' in weight:
            weight = weight[(weight.find(',') + 1):]
        weight_list[index] = weight.replace(',', '.')

    for index in range(len(weight_list)):
        count_digit = 0
        for second_index in range(len(weight_list[index])):
            if weight_list[index][second_index].isdigit() or weight_list[index][second_index] == '.':
                count_digit += 1
            else:
                if weight_list[index][second_index + 1].isdigit():
                    continue
                else:
                    break
        if count_digit == 0:
            unknown_weight += 1
            weight_list[index] = 1000
        else:
            for id_convert in type_of_weight:
                if id_convert in str(weight_list[index]):
                    convert_value = convert_weight[id_convert]
                    digit_weight = float(weight_list[index][:count_digit])
                    weight_list[index] = convert_value * digit_weight
                if 'cm' in str(weight_list[index]):
                    weight_list[index] = 1000

    min_weight = min(weight_list)
    return (unknown_weight, 'UnKnown'), (min_weight,
                                         [record['ObjectName'] for record in data]
                                         [weight_list.index(min(weight_list))])


def country_with_max_artifacts(data):
    countries = [record['ManuCountry'] for record in data]
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
    begin_date_list = [record['BeginDate'] for record in data]
    end_date_list = [record['EndDate'] for record in data]
    countries = [record['ManuCountry'] for record in data]

    new_column = []
    unknown_date = 0
    for (date_of_begin, date_of_end) in zip(begin_date_list, end_date_list):
        if not date_of_begin.isdigit() or not date_of_end.isdigit():
            unknown_date += 1
            new_column.append(0)
        else:
            new_column.append(int(date_of_end) - int(date_of_begin))
    unknown_date = (unknown_date, 'Unknown')
    max_time = max(new_column)
    max_time = (max_time, countries[new_column.index(max_time)])
    return unknown_date, max_time


def data_request(files):
    executor = ProcessPoolExecutor(max_workers=len(files))
    columns = []
    values = []

    result = list(executor.map(max_number_of_count, files))
    unknown = [row[0] for row in result]
    named = [row[1] for row in result]
    values.append(sum([row[0] for row in unknown]))
    columns.append('Unknown')
    max_count = max([row[0] for row in named])
    values.append(max_count)
    columns.append(named[[row[0] for row in named].index(max_count)][1])

    result = list(executor.map(max_begin_end_date, files))
    unknown = [row[0] for row in result]
    values.append(sum([row[0] for row in unknown]))
    named = [row[1] for row in result]
    columns.append('Unknown')
    max_count = max([row[0] for row in named])
    values.append(max_count)
    columns.append(named[[row[0] for row in named].index(max_count)][1])

    result = list(executor.map(min_weight_artifact, files))
    unknown = [row[0] for row in result]
    values.append(sum([row[0] for row in unknown]))
    named = [row[1] for row in result]
    columns.append('Unknown')
    min_count = min([row[0] for row in named])
    values.append(min_count)
    columns.append(named[[row[0] for row in named].index(min_count)][1])

    result = list(executor.map(country_with_max_artifacts, files))
    unknown = [row[0] for row in result]
    values.append(sum([row[0] for row in unknown]))
    named = [row[1] for row in result]
    columns.append('Unknown')
    max_count = max([row[0] for row in named])
    values.append(max_count)
    columns.append(named[[row[0] for row in named].index(max_count)][1])
    return columns, values


def main():
    data_dict = scan_file('cstmc-CSV-en.csv')
    files = []
    output_data_column = []
    output_data_value = []
    prev_iter = 0
    for iterator in range(30000, len(data_dict), 30000):
        files.append(data_dict[prev_iter:iterator])
        prev_iter = iterator
    files.append(data_dict[prev_iter:])

    output_data_column, output_data_value = data_request(files)

    file_output = open('general-stats.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['Name', 'Value'])
    writer.writeheader()
    for (column, value) in zip(output_data_column, output_data_value):
        writer.writerow({'Name': column, 'Value': value})
    file_output.close()


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
