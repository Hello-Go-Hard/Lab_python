class ImmutableDict:
    class_dict = {}

    def __init__(self, dict_as_param={}):
        self.class_dict = dict_as_param

    def get(self, key, value=None):
        return self.class_dict.get(key, value)

    def keys(self):
        return self.class_dict.keys()

    def values(self):
        return self.class_dict.values()

    def items(self):
        return self.class_dict.items()


def set_dict_value(some_dict, key, value):
    keys_tuple, values_tuple = tuple(some_dict.keys()), tuple(some_dict.values())
    if key not in keys_tuple:
        keys_tuple = tuple(keys_tuple + (key,))
    values_tuple = values_tuple[:keys_tuple.index(key)] + (value,) + values_tuple[keys_tuple.index(key)+1:]
    some_dict = ImmutableDict({key: value for (key, value) in zip(keys_tuple, values_tuple)})
    return some_dict


def concat_dicts(first, second):
    new_dict = ImmutableDict({})
    unique_keys = list(first.keys()) + list(second.keys())
    for key in unique_keys:
        new_dict = set_dict_value(new_dict, key, int(first.get(key) or 0) + int(second.get(key) or 0))
    return new_dict
