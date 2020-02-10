import csv
from csv_file_importer import scan_file


data_dict = scan_file('cstmc-CSV-en.csv')

output_data_column = []
output_data_value = []
countries = [record['ManuCountry'] for record in data_dict]
set_of_countries = set(countries)
unknown_country = ('Unknown', countries.count('') + countries.count('Unknown'))
set_of_countries.remove('')
set_of_countries.remove('Unknown')
max_country = ('country', 1)
for country in set_of_countries:
    if countries.count(country) > max_country[1]:
        max_country = (country, countries.count(country))
output_data_column.append(max_country[0])
output_data_value.append(max_country[1])
output_data_column.append(unknown_country[0])
output_data_value.append(unknown_country[1])

# max(begin_date - end_date)
begin_date_list = [record['BeginDate'] for record in data_dict]
end_date_list = [record['EndDate'] for record in data_dict]

new_column = []
unknown_date = 0
for (date_of_begin, date_of_end) in zip(begin_date_list, end_date_list):
    if not date_of_begin.isdigit() or not date_of_end.isdigit():
        unknown_date += 1
        new_column.append(0)
    else:
        new_column.append(int(date_of_end) - int(date_of_begin))
unknown_date = ('Unknown', unknown_date)
max_time = max(new_column)
max_time = (countries[new_column.index(max_time)], max_time)

output_data_column.append(max_time[0])
output_data_value.append(max_time[1])
output_data_column.append(unknown_date[0])
output_data_value.append(unknown_date[1])

# min(Weight)
Weight_list = [record['Weight'] for record in data_dict]
type_of_weight = ['lbs', 'kg', 'Metric tons', 'gm']
convert_weight = {'lbs': 0.453592, 'kg': 1, 'Metric tons': 1000, 'gm': 0.001}
unknown_weight = 0
for index in range(len(Weight_list)):
    weight = Weight_list[index]
    if weight is None:
        weight = ''
    elif '.' in weight and ',' in weight:
        weight = weight[(weight.find(',') + 1):]
    Weight_list[index] = weight.replace(',', '.')

for index in range(len(Weight_list)):
    count_digit = 0
    for second_index in range(len(Weight_list[index])):
        if Weight_list[index][second_index].isdigit() or Weight_list[index][second_index] == '.':
            count_digit += 1
        else:
            if Weight_list[index][second_index + 1].isdigit():
                continue
            else:
                break
    if count_digit == 0:
        unknown_weight += 1
        Weight_list[index] = 1000
    else:
        for id_convert in type_of_weight:
            if id_convert in str(Weight_list[index]):
                convert_value = convert_weight[id_convert]
                digit_weight = float(Weight_list[index][:count_digit])
                Weight_list[index] = convert_value * digit_weight
            if 'cm' in str(Weight_list[index]):
                Weight_list[index] = 1000

min_weight = min(Weight_list)
min_weight = ([record['ObjectName'] for record in data_dict][Weight_list.index(min(Weight_list))], min_weight)
unknown_weight = ('Unknown', unknown_weight)

output_data_column.append(min_weight[0])
output_data_value.append(min_weight[1])
output_data_column.append(unknown_weight[0])
output_data_value.append(unknown_weight[1])

# max(NumberOfComponents)
count_of_components = [record['NumberOfComponents'] for record in data_dict]
set_of = set(count_of_components)
unknown_components = ('Unknown', count_of_components.count(""))
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

unknown_components = ('Unknown', unknown_components[1] + unknown_count)
max_components = ([record['ObjectName'] for record in data_dict][max_components[0]], max_components[1])

output_data_column.append(max_components[0])
output_data_value.append(max_components[1])
output_data_column.append(unknown_components[0])
output_data_value.append(unknown_components[1])

file_output = open('general-stats.csv', 'w', newline='')
writer = csv.DictWriter(file_output, fieldnames=['Name', 'Value'])
writer.writeheader()
for (column, value) in zip(output_data_column, output_data_value):
    writer.writerow({'Name': column, 'Value': value})
file_output.close()
