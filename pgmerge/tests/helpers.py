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


def compare_table_output(self, actual_output, table_result_output, total_output):
    """
    Helper function to test CLI output. We ignore whitespace, empty lines, and only
    check specific lines since the output should be free to change in creative ways
    without breaking all the tests.
    """
    actual_output_lines = actual_output.splitlines()
    # Check per-table output that consists of table name and result summary
    for idx in range(len(table_result_output) // 2):
        # Should be table name
        self.assertEqual(actual_output_lines[idx].strip().split(),
                         table_result_output[idx])
        # Check table result
        self.assertEqual(actual_output_lines[idx + 1].strip().split(),
                         table_result_output[idx + 1])
    # Check total count
    self.assertEqual(actual_output_lines[-1], total_output)


def check_header(self, file_path, expected_header_list):
    """
    Check that the first line of the CSV header matches expectation.
    """
    with open(file_path) as ifh:
        header_columns = ifh.readlines()[0].strip().split(',')
        self.assertEqual(header_columns, expected_header_list)
