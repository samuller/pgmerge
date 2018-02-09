import os
import logging
import collections


class NoExceptionFormatter(logging.Formatter):
    """
    Formatter to specifically remove any exception traceback from logging output.
    See: https://stackoverflow.com/questions/6177520/python-logging-exc-info-only-for-file-handler
    """
    def format(self, record):
        # Clear cached exception message
        record.exc_text = ''
        return super(NoExceptionFormatter, self).format(record)

    def formatException(self, record):
        return ''


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
