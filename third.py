import csv
from csv_file_importer import scan_file


data_dict = scan_file('cstmc-CSV-en.csv')

materials = [record['material'] for record in data_dict]
dict_materials = {}
begin_date_list = [record['BeginDate'] for record in data_dict]
set_of_date = set(begin_date_list)
set_of_date.remove('')
date_to_index_dict = {}
mistakes = []
for date in set_of_date:
    if date.isdigit():
        date_to_index_dict[date] = [index_date for index_date in range(len(begin_date_list))
                                    if begin_date_list[index_date] == date]
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
keys_to_sort = list(date_to_materials_dict.keys())
keys_to_sort.sort()
date = []
material = []
count_of_material = []

date_keys = list(date_to_materials_dict.keys())
date_keys.sort()

for one_date in date_keys:
    material += list(date_to_materials_dict[one_date].keys())
    count_of_material += list((date_to_materials_dict[one_date].values()))
    for j in range(len(date_to_materials_dict[one_date].keys())):
        date.append(one_date)

file_output = open('material-stats.csv', 'w', newline='')
writer = csv.DictWriter(file_output, fieldnames=['Date', 'Material', 'Count'])
writer.writeheader()
for (one_date, one_material, one_count) in zip(date, material, count_of_material):
    writer.writerow({'Date': one_date, 'Material': one_material, 'Count': one_count})
file_output.close()
