import csv
from csv_file_importer import scan_file


data_dict = scan_file('cstmc-CSV-en.csv')

categories_1 = [record['category1'] for record in data_dict]
categories_2 = [record['category2'] for record in data_dict]
categories_3 = [record['category3'] for record in data_dict]
categories = categories_1 + categories_2 + categories_3
set_of_categories = set(categories)
dict_categories = {}

for category in set_of_categories:
    dict_categories[category] = categories.count(category)
categories, count_of_categories = list(dict_categories.keys()), list(dict_categories.values())

file_output = open('object-stats.csv', 'w', newline='')
writer = csv.DictWriter(file_output, fieldnames=['Category', 'Count'])
writer.writeheader()

for (category, count_of_category) in zip(categories, count_of_categories):
    writer.writerow({'Category': category, 'Count': count_of_category})
file_output.close()
