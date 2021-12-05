import os
from contextlib import contextmanager


@contextmanager
def write_file(path):
    """
    Context manager for creating a file during a test. Will clean-up and delete the file afterwards.

    Example:
        with write_file(file_path) as file_handle:
            # write to file_handle
            # read from file
        # file is now deleted
    """
    file = open(path, 'w')
    try:
        yield file
    finally:
        file.close()
        os.remove(path)
