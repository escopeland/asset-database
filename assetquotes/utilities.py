import json
from operator import itemgetter, attrgetter
from collections import namedtuple
from bisect import bisect_left


class KeyList(object):
    ''' KeyList is a wrapper for a list of objects that does not on its own support a 
        binary search via bisect (or similar binary search).

        To find the index of the dictionary with 'key' equal to key_value in a list of 
        dictionaries sorted by key 'key':
            index = bisect.bisect_left(KeyList(obj_list, key=lambda x: x['key']), key_value)

        To find the index of the namedtuple with field 'name' equal to key_value in a
        list of namedtuples sorted by field 'name':
            where: T = namedtuple('T', ['name', ['other', ..., 'fields'])
            index = bisect.bisect_left(KeyList(obj_list, key=lambda x: x.name), key_value)
    '''

    def __init__(self, obj_list, key):
        self.obj_list = obj_list
        self.key = key

    def __len__(self):
        return len(self.obj_list)

    def __getitem__(self, index):
        return self.key(self.obj_list[index])


def search_objlist(obj_list, key, key_value, fmt):
    ''' Return an object from within a sorted list of objects. 
    '''
    if isinstance(obj_list[0], dict):
        return obj_list[bisect_left(KeyList(obj_list, key=lambda x: x[key]), key_value)]

    elif isinstance(obj_list[0], tuple):
        return obj_list[
            bisect_left(KeyList(obj_list, key=lambda x: getattr(x, key)), key_value)
        ]


def sort_objlist(obj_list, key, fmt):
    if isinstance(obj_list[0], dict):
        return obj_list.sort(key=itemgetter(key))

    elif isinstance(obj_list[0], tuple):
        return obj_list.sort(key=attrgetter(key))


def dict_to_object(item, object_name):
    ''' Converts a python dict to a namedtuple
    '''
    fields = item.keys()
    values = item.values()

    return json.loads(
        json.dumps(item),
        object_hook=lambda d: namedtuple(object_name, fields)(*values),
    )

