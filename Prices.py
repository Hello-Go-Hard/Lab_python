import csv
file = open('prices.csv', 'r')
reader = csv.DictReader(file)
data = []
for row in reader:
    data.append(row)

unique_item = len(set([record['ITEM_ID'] for record in data]))
unique_store = len(set([record['STORE_ID'] for record in data]))

users = [record['APPROVED_BY']for record in data]
users_set = set([record['APPROVED_BY']for record in data])
max_user_count = max([(users.count(record), record)for record in users_set])

store_set = set([record['STORE_ID'] for record in data])
store_dict = {store: [] for store in store_set}
for record in data:
    store_dict[record['STORE_ID']] = record['ITEM_ID']
store_set = [(store, store_dict[store]) for store in store_set]

items_dict = {item: [] for item in set([record['ITEM_ID'] for record in data])}
for record in data:
    items_dict[record['ITEM_ID']].append(record['PRICE'])

for item in list(items_dict.keys()):
    items_dict[item] = sum([float(value) for value in list(items_dict[item])])/len(list(items_dict[item]))

items_price = [(record['PRICE'], record['ITEM_ID']) for record in data]
max_min = (max(items_price), min(items_price))
output = []
max_price_item = []
min_price_item = []
for record in data:
    if max_min[0][1] in record.values():
        max_price_item.append([record['STORE_ID'], record['ITEM_ID'], record['PRICE']])
    elif max_min[1][1] in record.values():
        min_price_item.append([record['STORE_ID'], record['ITEM_ID'], record['PRICE']])
print('Чай с индийскими специями')
