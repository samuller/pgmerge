import os
import logging
import collections

_log = logging.getLogger(__name__)


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


def recursive_update_ignore_none(any_dict, update_dict):  # pragma: no cover
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


def ensure_file_exists(file_path):  # pragma: no cover
    # Recursively create all directories if they don't exist
    file_dirs = os.path.dirname(file_path)
    if not os.path.exists(file_dirs):
        os.makedirs(file_dirs)
    # Create file if it doesn't exist, but don't alter it if it does
    with open(file_path, 'a'):
        pass


def decorate(decorators):
    """
    A decorator function to apply a list of decorators to a function. Useful when sharing a common
    group of decorators among functions.

    The original use case is with click decorators (see: https://github.com/pallets/click/issues/108)
    """
    def func_with_shared_decorators(func):
        for option in reversed(decorators):
            func = option(func)
        return func
    return func_with_shared_decorators


def only_file_stem(file_path):
    """
    Get name of file without directory path and extension.
    """
    file_name_only = os.path.basename(file_path)
    file_name_only = os.path.splitext(file_name_only)[0]
    return file_name_only


def is_windows():
    return os.name == 'nt'
