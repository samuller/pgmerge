"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import logging
from contextlib import contextmanager

import yaml
from click.testing import CliRunner
# from typer.testing import CliRunner
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy import MetaData, Table, Column, ForeignKey, String, Integer, select, text


from pgmerge import pgmerge
from .test_db import TestDB, create_table

LOG = logging.getLogger()
LOG.level = logging.WARN


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


class TestCLI(TestDB):
    """
    Functional tests that test the application by using it's command-line interface (CLI).
    """

    output_dir = '_tmp_test'

    @classmethod
    def setUpClass(cls):
        super(TestCLI, cls).setUpClass()
        os.makedirs(cls.output_dir, exist_ok=True)
        cls.runner = CliRunner()

    @classmethod
    def tearDownClass(cls):
        super(TestCLI, cls).tearDownClass()
        os.rmdir(cls.output_dir)

    def run_query(self, sql_stmt):
        result = self.connection.execute(sql_stmt)
        all = result.fetchall()
        result.close()
        return all

    def get_first(self, sql_stmt):
        result = self.run_query(sql_stmt)
        return [value for value, in result][0]

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

    def test_basics(self):
        """
        Test the basic command-line and database connection by exporting an empty database.
        """
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', '--uri', self.url, self.output_dir])
        self.assertEqual(result.output, "Exported 0 tables to 0 files\n")

    def test_dir_invalid(self):
        """
        Test providing invalid output directory as a command-line parameter.
        """
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', '--uri', self.url, 'dir'])
        self.assertEqual(result.exit_code, 2)
        # If directory given is actually a file
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', '--uri', self.url, 'NOTICE'])
        self.assertEqual(result.exit_code, 2)

    def test_export_table(self):
        """
        Test exporting a single empty table.
        """
        table_name = 'country'
        table = Table(table_name, MetaData(),
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False))
        with create_table(self.engine, table):
            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")

            file_path = os.path.join(self.output_dir, "{}.csv".format(table_name))
            self.check_header(file_path, ['code', 'name'])
            # Clean up file that was created (also tests that it existed as FileNotFoundError would be thrown)
            os.remove(file_path)

    def test_export_and_import_with_utf8_values(self):
        """
        Test exporting some data (containing UTF-8 characters) and immediately importing it.
        """
        table_name = 'country'
        table = Table(table_name, MetaData(),
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False))
        with create_table(self.engine, table):
            stmt = table.insert(None).values([
                ('CI', 'Côte d’Ivoire'),
                ('RE', 'Réunion'),
                ('ST', 'São Tomé and Príncipe')
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url, self.output_dir, table_name])
            # Since data hasn't changed, the import should change nothing. All lines should be skipped.
            self.compare_table_output(result.output, [
                ["country:"],
                ["skip:", "3", "insert:", "0", "update:", "0"],
            ], "1 tables imported successfully")

            os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_merge(self):
        """
        Test insert and update (merge) by exporting data, clearing table and then importing into a table with
        slightly different data.
        """
        table_name = 'country'
        table = Table(table_name, MetaData(),
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False))
        # Create table with data to export
        with create_table(self.engine, table):
            stmt = table.insert(None).values([
                ('CI', 'Côte d’Ivoire'),
                ('EG', 'Egypt'),
                ('RE', 'Réunion'),
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")
        # Import the exported data into a table with different data
        with create_table(self.engine, table):
            stmt = table.insert(None).values([
                ('EG', 'Egypt'),
                ('RE', 'Re-union'),
                ('ST', 'São Tomé and Príncipe'),
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url, self.output_dir, table_name])
            self.compare_table_output(result.output, [
                ["country:"],
                ["skip:", "1", "insert:", "1", "update:", "1"],
            ], "1 tables imported successfully")

            stmt = select(table).order_by('code')
            with self.connection.begin():
                result = self.connection.execute(stmt)
            self.assertEqual(result.fetchall(), [
                ('CI', 'Côte d’Ivoire'), ('EG', 'Egypt'), ('RE', 'Réunion'), ('ST', 'São Tomé and Príncipe')])
            result.close()
            # Select requires us to close the connection before dropping the table
            self.connection.close()

        os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_config_references(self):
        """
        Test import and export that uses config file to select an alternate key.
        """
        # Use a new metadata for each test since the database schema should be empty
        metadata = MetaData()
        the_table = Table('the_table', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('code', String(2), nullable=False),
                          Column('name', String),
                          Column('ref_other_table', Integer, ForeignKey("other_table.id")))
        other_table = Table('other_table', metadata,
                            Column('id', Integer, primary_key=True),
                            Column('code', String(2), nullable=False),
                            Column('name', String))

        config_data = {
            'other_table': {'alternate_key': ['code']}
        }  # 'other_table': {'columns'}
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file, \
                create_table(self.engine, other_table), \
                create_table(self.engine, the_table):
            with self.connection.begin():
                self.connection.execute(other_table.insert(None), [
                    {'code': 'IS', 'name': 'Iceland'},
                ])
                self.connection.execute(other_table.insert(None), [
                    {'code': 'IN'},
                ])
            yaml.dump(config_data, config_file, default_flow_style=False)

            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 2 tables to 2 files\n")

            result = self.runner.invoke(pgmerge.upsert, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.compare_table_output(result.output, [
                ["other_table:"],
                ["skip:", "2", "insert:", "0", "update:", "0"],
                ["the_table:"],
                ["skip:", "0", "insert:", "0", "update:", "0"],
            ], "2 tables imported successfully")

            the_table_path = os.path.join(self.output_dir, "the_table.csv")
            self.check_header(the_table_path, ['id', 'code',
                                               'name', 'join_the_table_ref_other_table_fkey_code'])

            other_table_path = os.path.join(self.output_dir, "other_table.csv")
            self.check_header(other_table_path, ['id', 'code', 'name'])

            os.remove(the_table_path)
            os.remove(other_table_path)

    def test_config_self_reference(self):
        """
        Test import when table has self-reference that is part of alternate key.
        """
        metadata = MetaData()
        the_table = Table('the_table', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('code', String(10), nullable=False),
                          Column('name', String),
                          Column('parent_id', Integer, ForeignKey("the_table.id")))

        config_data = {
            'the_table': {'columns': ['code', 'name', 'parent_id'],
                          'alternate_key': ['code', 'parent_id']}
        }
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file, \
                create_table(self.engine, the_table), \
                self.connection:  # 'Select' requires us to close the connection before dropping the table
            with self.connection.begin():
                self.connection.execute(the_table.insert(None), [
                    {'code': 'LCY', 'name': 'London', 'parent_id': None},
                    {'code': 'NYC', 'name': 'New York City', 'parent_id': None},
                    {'code': 'MAIN', 'name': 'Main street', 'parent_id': 1},
                    {'code': 'MAIN', 'name': 'Main street', 'parent_id': 2},
                ])
            yaml.dump(config_data, config_file, default_flow_style=False)

            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            result_lines = result.output.splitlines()
            self.assertEqual(result_lines[0].strip(),
                             "Self-referencing tables found that could prevent import: the_table")
            self.assertEqual(result_lines[3].strip(), "Exported 1 tables to 1 files")
            with self.connection.begin():
                # Clear table to see if import worked
                self.connection.execute(the_table.delete())
                # We reset sequence so that id numbers match the initial import
                self.connection.execute(text("ALTER SEQUENCE the_table_id_seq RESTART WITH 1"))

            # self.runner.invoke(pgmerge.upsert, ['--config', config_file_path, '--disable-foreign-keys',
            #                                     '--dbname', self.db_name, '--uri', self.url, self.output_dir])

            self.runner.invoke(pgmerge.upsert, ['--config', config_file_path, '--disable-foreign-keys',
                                                '--dbname', self.db_name, '--uri', self.url, self.output_dir])

            result = self.run_query(select(the_table).order_by('id'))
            self.assertEqual(result, [
                (1, 'LCY', 'London', None), (2, 'NYC', 'New York City', None),
                (3, 'MAIN', 'Main street', None), (4, 'MAIN', 'Main street', None)])

            the_table_path = os.path.join(self.output_dir, "the_table.csv")
            os.remove(the_table_path)

    def test_logging_init(self):
        """
        Test initialisation of logging.

        TODO: Consider tests that validate the behaviour tested manually, e.g.
        printing errors to both stdout and log files, using specific formatting,
        reusing logging setup across modules, controlling logging level etc.
        """
        # pgmerge.setup_logging(False)
        pass

    def test_inspect_tables(self):
        """
        Test some inspect commands.
        """
        metadata = MetaData()
        the_table = Table('the_table', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('code', String(2), nullable=False),
                          Column('name', String),
                          Column('ref_other_table', Integer, ForeignKey("other_table.id")))
        other_table = Table('other_table', metadata,
                            Column('id', Integer, primary_key=True),
                            Column('code', String(2), nullable=False),
                            Column('name', String))

        with create_table(self.engine, other_table), \
             create_table(self.engine, the_table):
            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--list-tables'])
            self.assertEqual(result.output.splitlines(), ['other_table', 'the_table'])

            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--table-details'])
            result_output = result.output.splitlines()
            self.assertEqual(result_output[1], "table: other_table")
            self.assertEqual(result_output[2].strip().split()[0], "columns:")
            self.assertEqual(result_output[4], "table: the_table")
            self.assertEqual(result_output[5].strip().split()[0], "columns:")
            self.assertEqual(result_output[6].strip().split()[0], "fks:")

            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--insert-order'])
            self.assertEqual(result.output.splitlines(), [
                "Found 2 tables in schema 'public'", "",
                'Insertion order:', str(['other_table', 'the_table'])])
