import os
import collections

def recursive_update_ignore_none(any_dict, update_dict):
    """
    Similar to dict.update(), but updates recursively nested dictionaries and never
    updates a key's value to None.
    """
    for key, value in update_dict.items():
        if value is None:
            continue
        elif isinstance(value, collections.Mapping):
            any_dict[key] = recursive_update_ignore_none(any_dict.get(key, {}), value)
        else:
            any_dict[key] = value
    return any_dict


def ensure_file_exists(file_path):
    # Recursively create all directories if they don't exist
    file_dirs = os.path.dirname(file_path)
    if not os.path.exists(file_dirs):
        os.makedirs(file_dirs)
    # Create file if it doesn't exist, but don't alter it if it does
    with open(file_path, 'a'):
        pass
