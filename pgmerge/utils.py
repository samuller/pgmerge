"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import logging
from typing import Any, Dict, List, Callable

_log = logging.getLogger(__name__)


class NoExceptionFormatter(logging.Formatter):
    """
    Formatter to specifically remove any exception traceback from logging output.

    See: https://stackoverflow.com/questions/6177520/python-logging-exc-info-only-for-file-handler
    """

    def format(self, record: logging.LogRecord) -> str:
        """Remove cached exception traceback message."""
        # Clear cached exception message
        record.exc_text = ''
        return super(NoExceptionFormatter, self).format(record)

    def formatException(self, exc: Any) -> str:
        """Remove exception details."""
        return ''


def replace_indexes(listy: List[Any], idxs_to_replace: List[int], new_values: List[Any]) -> None:
    """Remove given indexes and insert a new set of values into the given list."""
    # Delete values to be replaced (remove highest indices first so that indices don't change)
    for idx in reversed(sorted(idxs_to_replace)):
        del listy[idx]
    # We have to add all new values at the first index to be replaced since thats the only index which is now unchanged
    idx_to_add = min(idxs_to_replace)

    # Add multiple values in reverse so that we can keep the insertion index the same
    # and their final order will end up correct
    for value in reversed(new_values):
        listy.insert(idx_to_add, value)


def recursive_update_ignore_none(any_dict: Dict[Any, Any], update_dict: Dict[Any, Any]
                                 ) -> Dict[Any, Any]:  # pragma: no cover
    """Similar to dict.update(), but updates recursively nested dictionaries and never updates a key's value to None."""
    for key, value in update_dict.items():
        if value is None:
            continue
        elif isinstance(value, Dict):
            any_dict[key] = recursive_update_ignore_none(any_dict.get(key, {}), value)
        else:
            any_dict[key] = value
    return any_dict


def ensure_file_exists(file_path: str) -> None:  # pragma: no cover
    """Create file and complete path of sub-directories, if needed."""
    # Recursively create all directories if they don't exist
    file_dirs = os.path.dirname(file_path)
    if not os.path.exists(file_dirs):
        os.makedirs(file_dirs)
    # Create file if it doesn't exist, but don't alter it if it does
    with open(file_path, 'a'):
        pass


def decorate(decorators: List[Callable[..., Any]]) -> Callable[..., Any]:
    """Use this decorator function to apply a list of decorators to a function.

    Useful when sharing a common group of decorators among functions.

    The original use case is with click decorators (see: https://github.com/pallets/click/issues/108)
    """
    def func_with_shared_decorators(func: Callable[..., Any]) -> Callable[..., Any]:
        for option in reversed(decorators):
            func = option(func)
        return func
    return func_with_shared_decorators


def only_file_stem(file_path: str) -> str:
    """Get name of file without directory path and extension."""
    file_name_only = os.path.basename(file_path)
    file_name_only = os.path.splitext(file_name_only)[0]
    return file_name_only


def is_windows() -> bool:
    """Check if running on Windows OS."""
    return os.name == 'nt'
