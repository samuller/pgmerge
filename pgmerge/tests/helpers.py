import os
import csv
from typing import List
from itertools import islice
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
    file = open(path, "w")
    try:
        yield file
    finally:
        file.close()
        os.remove(path)


@contextmanager
def del_files(paths: List[str]):
    """
    Context manager for deleting files if created during a test.

    Example:
        with del_files([file_path]):
            # perform tests commands that might generate file
        # file is now deleted
    """
    try:
        yield
    finally:
        for path in paths:
            if os.path.exists(path):
                os.remove(path)


def write_csv(path, rows):
    with open(path, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            csvwriter.writerow(row)


def slice_lines(multi_line_string: str, start=None, stop=None, step=None):
    return "\n".join(islice(multi_line_string.splitlines(), start, stop, step))


def compare_table_output(self, actual_output, table_result_output, total_output):
    """
    Helper function to test CLI output. We ignore whitespace, empty lines, and only
    check specific lines since the output should be free to change in creative ways
    without breaking all the tests.
    """
    actual_output_lines = [line.strip().split() for line in actual_output.splitlines()]
    # Check per-table output that consists of multiple lines of table name and result summary
    for idx in range(len(table_result_output)):
        self.assertEqual(actual_output_lines[idx], table_result_output[idx])
    # Check total count
    self.assertEqual(actual_output_lines[-1], total_output.strip().split())


def check_header(self, file_path, expected_header_list):
    """
    Check that the first line of the CSV header matches expectation.
    """
    with open(file_path, "r") as ifh:
        header_columns = ifh.readlines()[0].strip().split(",")
        self.assertEqual(header_columns, expected_header_list)


def count_lines(file_path):
    """
    Count the number of lines in a file.
    """
    with open(file_path, "r") as ifh:
        line_count = len(ifh.readlines())
        return line_count
