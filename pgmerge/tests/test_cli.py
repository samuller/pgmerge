"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
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
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
        self.assertEquals(result.output, "Exported 0 tables\n")

    def test_dir_invalid(self):
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'dir'])
        self.assertEqual(result.exit_code, 2)
        # If directory given is actually a file
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'NOTICE'])
        self.assertEqual(result.exit_code, 2)

    def test_export_table(self):
        table_name = 'country'
        table = Table(table_name, MetaData(),
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False))
        with create_table(self.engine, table):
            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
            self.assertEquals(result.output, "Exported 1 tables\n")
            os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_export_and_import_with_utf8_values(self):
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

            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
            self.assertEquals(result.output, "Exported 1 tables\n")

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', 'testdb', self.output_dir, table_name])
            result_lines = result.output.splitlines()
            self.assertEquals(result_lines[0], "country:")
            self.assertEquals(result_lines[1].strip().split(), ["skip:", "3", "insert:", "0", "update:", "0"])
            self.assertEquals(result_lines[-1], "1 tables imported successfully")

            os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_merge(self):
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

            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
            self.assertEquals(result.output, "Exported 1 tables\n")
        # Import the exported data into a table with different data
        with create_table(self.engine, table):
            stmt = table.insert().values([
                ('EG', 'Egypt'),
                ('RE', 'Re-union'),
                ('ST', 'São Tomé and Príncipe'),
            ])
            self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', 'testdb', self.output_dir, table_name])
            result_lines = result.output.splitlines()
            self.assertEquals(result_lines[0], "country:")
            self.assertEquals(result_lines[1].strip().split(), ["skip:", "1", "insert:", "1", "update:", "1"])
            self.assertEquals(result_lines[-1], "1 tables imported successfully")

            stmt = select([table]).order_by('code')
            result = self.connection.execute(stmt)
            self.assertEquals(result.fetchall(), [
                ('CI', 'Côte d’Ivoire'), ('EG', 'Egypt'), ('RE', 'Réunion'), ('ST', 'São Tomé and Príncipe')])
            result.close()
            # Select requires us to close the connection before dropping the table
            self.connection.close()

        os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))

    def test_config_references(self):
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
                                                         '--dbname', 'testdb', self.output_dir])
            self.assertEquals(result.output, "Exported 2 tables\n")

            with open(os.path.join(self.output_dir, "the_table.csv")) as cmd_output:
                header_columns = cmd_output.readlines()[0].strip().split(',')
                self.assertEquals(header_columns, ['the_table_id', 'the_table_code',
                                                   'the_table_name', 'the_table_ref_other_table'])

            with open(os.path.join(self.output_dir, "other_table.csv")) as cmd_output:
                header_columns = cmd_output.readlines()[0].strip().split(',')
                self.assertEquals(header_columns, ['other_table_id', 'other_table_code', 'other_table_name'])

            os.remove(os.path.join(self.output_dir, "the_table.csv"))
            os.remove(os.path.join(self.output_dir, "other_table.csv"))
