"""
Tests of command-line interface (CLI) and main parts of app.
"""
import os
import io
import logging
from unittest import mock
from contextlib import redirect_stdout

import click
from click.testing import CliRunner
# from typer.testing import CliRunner
from sqlalchemy.dialects.postgresql import JSONB
from pgmerge.pgmerge import EXIT_CODE_ARGS, EXIT_CODE_INVALID_DATA, version_callback
from sqlalchemy import MetaData, Table, Column, ForeignKey, String, Integer, select

from pgmerge import pgmerge
from .test_db import TestDB, create_table
from .helpers import compare_table_output, check_header, slice_lines, write_csv, write_file

LOG = logging.getLogger()
LOG.level = logging.WARN


class TestCLI(TestDB):
    """
    Functional tests that test the application by using its command-line interface (CLI).
    """

    output_dir = '_tmp_test'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.makedirs(cls.output_dir, exist_ok=True)
        cls.runner = CliRunner()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        os.rmdir(cls.output_dir)

    def get_first(self, sql_stmt):
        result = self.run_query(sql_stmt)
        return [value for value, in result][0]

    def test_basics(self):
        """
        Test the basic command-line and database connection by exporting an empty database.
        """
        result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
        self.assertEqual(result.output, "Exported 0 tables to 0 files\n")
        self.assertEqual(result.exit_code, 0)

    def test_dir_invalid(self):
        """
        Test providing invalid output directory as a command-line parameter.
        """
        result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, 'dir'])
        self.assertEqual(result.exit_code, EXIT_CODE_ARGS)
        # If directory given is actually a file
        result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, 'NOTICE'])
        self.assertEqual(result.exit_code, EXIT_CODE_ARGS)

    def test_export_table(self):
        """
        Test exporting a single empty table.
        """
        table_name = 'country'
        table = Table(table_name, MetaData(),
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False))
        with create_table(self.engine, table):
            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")
            self.assertEqual(result.exit_code, 0)

            file_path = os.path.join(self.output_dir, "{}.csv".format(table_name))
            check_header(self, file_path, ['code', 'name'])
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
            stmt = table.insert().values([
                ('CI', 'Côte d’Ivoire'),
                ('RE', 'Réunion'),
                ('ST', 'São Tomé and Príncipe')
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")
            self.assertEqual(result.exit_code, 0)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir, table_name])
            # Since data hasn't changed, the import should change nothing. All lines should be skipped.
            compare_table_output(self, result.output, [
                ["country:"],
                ["skip:", "3", "insert:", "0", "update:", "0"],
            ], "1 files imported successfully into 1 tables")

            os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_export_and_import_with_jsonb_field(self):
        """
        Test exporting and importing some data to a column of type JSONB.
        """
        table_name = 'country'
        table = Table(table_name, MetaData(),
                      Column('code', String(2), primary_key=True),
                      Column('name', JSONB, nullable=False))
        with create_table(self.engine, table):
            stmt = table.insert().values([
                ('CI', 'Côte d’Ivoire'),
                ('RE', 'Réunion'),
                ('ST', 'São Tomé and Príncipe')
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")
            self.assertEqual(result.exit_code, 0)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir, table_name])
            # Since data hasn't changed, the import should change nothing. All lines should be skipped.
            compare_table_output(self, result.output, [
                ["country:"],
                ["skip:", "3", "insert:", "0", "update:", "0"],
            ], "1 files imported successfully into 1 tables")
            self.assertEqual(result.exit_code, 0)

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
            stmt = table.insert().values([
                ('CI', 'Côte d’Ivoire'),
                ('EG', 'Egypt'),
                ('RE', 'Réunion'),
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")
            self.assertEqual(result.exit_code, 0)
        # Import the exported data into a table with different data
        with create_table(self.engine, table):
            stmt = table.insert().values([
                ('EG', 'Egypt'),
                ('RE', 'Re-union'),
                ('ST', 'São Tomé and Príncipe'),
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir, table_name])
            compare_table_output(self, result.output, [
                ["country:"],
                ["skip:", "1", "insert:", "1", "update:", "1"],
            ], "1 files imported successfully into 1 tables")
            self.assertEqual(result.exit_code, 0)

            stmt = select(table).order_by('code')
            with self.connection.begin():
                result = self.connection.execute(stmt)
            self.assertEqual(result.fetchall(), [
                ('CI', 'Côte d’Ivoire'), ('EG', 'Egypt'), ('RE', 'Réunion'), ('ST', 'São Tomé and Príncipe')])
            result.close()
            # Select requires us to close the connection before dropping the table
            self.connection.close()

        os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_export_and_import_with_dependent_tables(self):
        """
        Test exporting and importing data from tables with dependencies among them.
        """
        metadata = MetaData()
        table_name = 'country'
        table = Table(table_name, metadata,
                      Column('code', String(3), primary_key=True),
                      Column('name', String, nullable=False))
        dep_table_name = 'places_to_go'
        dep_table = Table(dep_table_name, metadata,
                          Column('id', Integer, primary_key=True),
                          Column('place_code', String(3), ForeignKey('country.code'))
                          )
        with create_table(self.engine, table), \
             create_table(self.engine, dep_table):
            stmt = table.insert().values([
                ('BWA', 'Botswana'),
                ('ZAF', 'South Africa'),
                ('ZWE', 'Zimbabwe')
            ])
            with self.connection.begin():
                self.connection.execute(stmt)
            stmt = dep_table.insert().values([
                {'place_code': 'ZAF'},
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url,
                                                         '--include-dependent-tables', self.output_dir,
                                                         dep_table_name])
            self.assertEqual(result.output.splitlines()[-1], "Exported 2 tables to 2 files")
            self.assertEqual(result.exit_code, 0)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         '--include-dependent-tables', self.output_dir, dep_table_name])

            self.assertEqual(result.output.splitlines()[4], 'Final tables exported: country places_to_go')
            # Since data hasn't changed, the import should change nothing. All lines should be skipped.
            compare_table_output(self, slice_lines(result.output, 6), [
                ["country:"],
                ["skip:", "3", "insert:", "0", "update:", "0"],
                ["places_to_go:"],
                ["skip:", "1", "insert:", "0", "update:", "0"],
            ], "2 files imported successfully into 2 tables")
            self.assertEqual(result.exit_code, 0)

            for export_file in ['country.csv', 'places_to_go.csv']:
                export_path = os.path.join(self.output_dir, export_file)
                os.remove(export_path)

    def test_logging_init(self):
        """
        Test initialisation of logging.

        TODO: Consider tests that validate the behaviour tested manually, e.g.
        printing errors to both stdout and log files, using specific formatting,
        reusing logging setup across modules, controlling logging level etc.
        """
        # pgmerge.setup_logging(False)
        result = self.runner.invoke(pgmerge.cli_app, [])
        self.assertEqual(result.exit_code, EXIT_CODE_ARGS)

    def test_missing_table(self):
        """
        Test import to tables not found in schema.
        """
        result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                     self.output_dir, 'missing'])
        self.assertEqual(result.exit_code, EXIT_CODE_ARGS)

    def test_inspect_tables(self):
        """
        Test some inspect commands.
        """
        metadata = MetaData()
        the_table = Table('the_table', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('code', String(2), nullable=False),
                          Column('name', String),
                          Column('ref_other_table', Integer, ForeignKey('other_table.id')))
        other_table = Table('other_table', metadata,
                            Column('id', Integer, primary_key=True),
                            Column('code', String(2), nullable=False),
                            Column('name', String))

        with create_table(self.engine, other_table), \
             create_table(self.engine, the_table):
            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--list-tables'])
            self.assertEqual(result.output.splitlines(), ['other_table', 'the_table'])
            self.assertEqual(result.exit_code, 0)

            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--table-details'])
            result_output = result.output.splitlines()
            self.assertEqual(result_output[1], "table: other_table")
            self.assertEqual(result_output[2].strip().split()[0], "columns:")
            self.assertEqual(result_output[4], "table: the_table")
            self.assertEqual(result_output[5].strip().split()[0], "columns:")
            self.assertEqual(result_output[6].strip().split()[0], "fks:")
            self.assertEqual(result.exit_code, 0)

            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--insert-order'])
            self.assertEqual(result.output.splitlines(), [
                "Found 2 tables in schema 'public'", "",
                'Insertion order:', str(['other_table', 'the_table'])])
            self.assertEqual(result.exit_code, 0)

            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--warnings', '--cycles', '--partition'])
            self.assertEqual(result.exit_code, 0)

    def test_version(self):
        """
        Test version print out.
        """
        fh = io.StringIO()
        with self.assertRaises(click.exceptions.Exit), redirect_stdout(fh):
            version_callback(True)
        self.assertTrue(fh.getvalue().startswith("pgmerge, version "))

    def test_setting_encoding(self):
        """
        Test that we set standard encoding when needed.
        """
        with mock.patch.dict(os.environ, {'PGCLIENTENCODING': 'LATIN1'}):
            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir])
        self.assertEqual(
            result.output.splitlines()[0],
            "WARNING: Setting database connection encoding to UTF8 instead of 'LATIN1'"
        )
        self.assertEqual(result.exit_code, 0)

        with mock.patch.dict(os.environ, {'PGCLIENTENCODING': 'LATIN1'}):
            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
        self.assertEqual(
            result.output.splitlines()[0],
            "WARNING: Setting database connection encoding to UTF8 instead of 'LATIN1'"
        )
        self.assertEqual(result.exit_code, 0)

    def test_missing_table_file(self):
        """
        Test import when specified table has no matching file.
        """
        metadata = MetaData()
        the_table = Table('the_table', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('value', String))

        with create_table(self.engine, the_table):
            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir,  "the_table"])
        self.assertEqual(result.output.splitlines(), [
            "No files found for the following tables:",
            "\t the_table.csv"
        ])
        self.assertEqual(result.exit_code, EXIT_CODE_INVALID_DATA)

    def test_single_table_has_table_args(self):
        """
        Test single-table import requires one table argument.
        """
        result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                     '--single-table', self.output_dir])
        self.assertEqual(
            result.output.splitlines()[0],
            'One table has to be specified when using the --single-table option'
        )
        self.assertEqual(result.exit_code, EXIT_CODE_ARGS)

        metadata = MetaData()
        table_one = Table('table_one', metadata, Column('id', Integer, primary_key=True))
        table_two = Table('table_two', metadata, Column('id', Integer, primary_key=True),)

        with create_table(self.engine, table_one), \
             create_table(self.engine, table_two):
            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         '--single-table', self.output_dir,
                                                         "table_one", "table_two"])
        self.assertEqual(
            result.output.splitlines()[0],
            'Only one table can be specified when using the --single-table option'
        )
        self.assertEqual(result.exit_code, EXIT_CODE_ARGS)

    def test_unknown_schema(self):
        """
        Test checks when invalid schema is specified.
        """
        result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                     '--schema', 'invalid_schema', self.output_dir])
        self.assertEqual(result.output.splitlines()[0], "Schema not found: 'invalid_schema'")
        self.assertEqual(result.exit_code, EXIT_CODE_ARGS)

    def test_invalid_table(self):
        """
        Test checks for tables with unsupported schemas.
        """
        metadata = MetaData()
        the_table = Table('the_table', metadata, Column('value', String))

        the_table_csv_path = os.path.join(self.output_dir, "the_table.csv")
        with create_table(self.engine, the_table), write_file(the_table_csv_path):
            write_csv(the_table_csv_path, [['value']])
            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir, 'the_table'])
        self.assertEqual(
            result.output.splitlines()[0:2], [
                'the_table:',
                '\tSkipping table with unsupported schema: Table has no primary key or unique columns!'
            ])
        self.assertEqual(
            result.output.splitlines()[9:11], [
                '1 tables skipped due to errors:',
                '\tthe_table'
            ])
        compare_table_output(self, slice_lines(result.output, 3), [
            ], "0 files imported successfully into 0 tables")
        self.assertEqual(result.exit_code, 0)
