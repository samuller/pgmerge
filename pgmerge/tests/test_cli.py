"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018-2019 Simon Muller (samullers@gmail.com)
"""
import os
import sys
import yaml
import logging
from .test_db import *
from io import StringIO
from sqlalchemy import *
from pgmerge import pgmerge
from click.testing import CliRunner

logger = logging.getLogger()
logger.level = logging.WARN


@contextmanager
def write_file(path):
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
            with open(file_path) as fh:
                self.assertEqual(fh.readline(), "code,name\n")    
            # Clean up file that was created (also tests that it existed as FileNotFoundError would be thrown)
            os.remove(file_path)

    def test_export_and_import_with_utf8_values(self):
        """
        Test exporting some data and immediately importing it.
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
            self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url, self.output_dir, table_name])
            # Since data hasn't changed, the import should change nothing. All lines should be skipped.
            result_lines = result.output.splitlines()
            self.assertEqual(result_lines[0], "country:")
            self.assertEqual(result_lines[1].strip().split(), ["skip:", "3", "insert:", "0", "update:", "0"])
            self.assertEqual(result_lines[-1], "1 tables imported successfully")

            os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_merge(self):
        """
        Test insert and update (merge) by exporting data, changing and adding to it, and then importing it.
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
            self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 1 files\n")
        # Import the exported data into a table with different data
        with create_table(self.engine, table):
            stmt = table.insert().values([
                ('EG', 'Egypt'),
                ('RE', 'Re-union'),
                ('ST', 'São Tomé and Príncipe'),
            ])
            self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url, self.output_dir, table_name])
            result_lines = result.output.splitlines()
            self.assertEqual(result_lines[0], "country:")
            self.assertEqual(result_lines[1].strip().split(), ["skip:", "1", "insert:", "1", "update:", "1"])
            self.assertEqual(result_lines[-1], "1 tables imported successfully")

            stmt = select([table]).order_by('code')
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

        data = {'the_table': {'alternate_key': ['code']}}  # 'other_table': {'columns'}
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file, \
                create_table(self.engine, other_table), \
                create_table(self.engine, the_table):
            self.connection.execute(other_table.insert(), [
                {'code': 'IS', 'name': 'Iceland'},
            ])
            self.connection.execute(other_table.insert(), [
                {'code': 'IN'},
            ])
            yaml.dump(data, config_file, default_flow_style=False)

            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 2 tables to 2 files\n")

            result = self.runner.invoke(pgmerge.upsert, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            result_lines = result.output.splitlines()
            self.assertEqual(result_lines[0], "other_table:")
            self.assertEqual(result_lines[1].strip().split(), ["skip:", "2", "insert:", "0", "update:", "0"])
            self.assertEqual(result_lines[2], "the_table:")
            self.assertEqual(result_lines[3].strip().split(), ["skip:", "0", "insert:", "0", "update:", "0"])
            self.assertEqual(result_lines[-1], "2 tables imported successfully")

            with open(os.path.join(self.output_dir, "the_table.csv")) as cmd_output:
                header_columns = cmd_output.readlines()[0].strip().split(',')
                self.assertEqual(header_columns, ['id', 'code',
                                                   'name', 'ref_other_table'])

            with open(os.path.join(self.output_dir, "other_table.csv")) as cmd_output:
                header_columns = cmd_output.readlines()[0].strip().split(',')
                self.assertEqual(header_columns, ['id', 'code', 'name'])

            os.remove(os.path.join(self.output_dir, "the_table.csv"))
            os.remove(os.path.join(self.output_dir, "other_table.csv"))
