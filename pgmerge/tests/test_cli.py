"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import logging

from click.testing import CliRunner
# from typer.testing import CliRunner
from sqlalchemy import MetaData, Table, Column, ForeignKey, String, Integer, select

from pgmerge import pgmerge
from .test_db import TestDB, create_table
from .helpers import compare_table_output, check_header, slice_lines

LOG = logging.getLogger()
LOG.level = logging.WARN


class TestCLI(TestDB):
    """
    Functional tests that test the application by using it's command-line interface (CLI).
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
            stmt = table.insert(None).values([
                ('CI', 'Côte d’Ivoire'),
                ('RE', 'Réunion'),
                ('ST', 'São Tomé and Príncipe')
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir, table_name])
            # Since data hasn't changed, the import should change nothing. All lines should be skipped.
            compare_table_output(self, result.output, [
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

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir, table_name])
            compare_table_output(self, result.output, [
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
            stmt = table.insert(None).values([
                ('BWA', 'Botswana'),
                ('ZAF', 'South Africa'),
                ('ZWE', 'Zimbabwe')
            ])
            with self.connection.begin():
                self.connection.execute(stmt)
            stmt = dep_table.insert(None).values([
                {'place_code': 'ZAF'},
            ])
            with self.connection.begin():
                self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url,
                                                         '--include-dependent-tables', self.output_dir,
                                                         dep_table_name])
            self.assertEqual(result.output.splitlines()[-1], "Exported 2 tables to 2 files")

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         '--include-dependent-tables', self.output_dir, dep_table_name])

            self.assertEqual(result.output.splitlines()[4], 'Final tables exported: country places_to_go')
            # Since data hasn't changed, the import should change nothing. All lines should be skipped.
            compare_table_output(self, slice_lines(result.output, 6), [
                ["country:"],
                ["skip:", "3", "insert:", "0", "update:", "0"],
                ["places_to_go:"],
                ["skip:", "1", "insert:", "0", "update:", "0"],
            ], "2 tables imported successfully")

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
        self.assertEqual(result.exit_code, 2)

    def test_missing_table(self):
        """
        Test import to tables not found in schema.
        """
        result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                     self.output_dir, 'missing'])
        self.assertEqual(result.exit_code, 2)

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

            result = self.runner.invoke(pgmerge.inspect, ['--dbname', self.db_name, '--uri', self.url,
                                                          '--warnings', '--cycles', '--partition'])
            self.assertEqual(result.exit_code, 0)
