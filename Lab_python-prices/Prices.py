import csv


data = None
with open('prices.csv', 'r') as source:
    data = list(csv.DictReader(source))


def unique_products():
    file_output = open('unique.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['Column', 'Count unique'])
    writer.writeheader()
    writer.writerow({'Column': 'Item', 'Count unique': len(set([record['ITEM_ID'] for record in data]))})
    writer.writerow({'Column': 'store', 'Count unique': len(set([record['STORE_ID'] for record in data]))})
    file_output.close()


def max_price_approved():
    users = [record['APPROVED_BY'] for record in data]
    users_set = set([record['APPROVED_BY'] for record in data])
    max_user_count = max([(users.count(record), record) for record in users_set])
    file_output = open('user-max-approved.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['User', 'Count of items'])
    writer.writeheader()
    writer.writerow({'User': max_user_count[1], 'Count of items':  max_user_count[0]})
    file_output.close()


def count_of_products_in_store():
    store_set = set([record['STORE_ID'] for record in data])
    store_dict = {store: [] for store in store_set}
    for record in data:
        store_dict[record['STORE_ID']] = record['ITEM_ID']
    store_set = [(store, store_dict[store]) for store in store_set]
    file_output = open('item-in-store.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['Store', 'Count of items'])
    writer.writeheader()
    for key in list(store_dict.keys()):
        writer.writerow({'Store': key, 'Count of items': store_dict[key]})
    file_output.close()


def avg_of_prices():
    items_dict = {item: [] for item in set([record['ITEM_ID'] for record in data])}
    for record in data:
        items_dict[record['ITEM_ID']].append(record['PRICE'])
    for item in list(items_dict.keys()):
        items_dict[item] = sum([float(value) for value in list(items_dict[item])]) / len(list(items_dict[item]))
    file_output = open('avg prices.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['Item', 'Price'])
    writer.writeheader()
    arrays = items_dict.items()
    for pair_item_price in items_dict.items():
        writer.writerow({'Item': pair_item_price[0], 'Price': pair_item_price[1]})
    file_output.close()


def store_with_max_min_price():
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
    file_output = open('store-max-min.csv', 'w', newline='')
    writer = csv.DictWriter(file_output, fieldnames=['Store', 'Item', 'Price'])
    writer.writeheader()

    for (array, designator) in zip([max_price_item, min_price_item], ['Max', 'Min']):
        writer.writerow({'Store': designator, 'Item': '', 'Price': ''})
        for sub_array in array:
            writer.writerow({'Store': sub_array[0], 'Item': sub_array[1], 'Price': sub_array[2]})
    file_output.close()


unique_products()
max_price_approved()
count_of_products_in_store()
avg_of_prices()
store_with_max_min_price()
print('Чай с индийскими специями')
