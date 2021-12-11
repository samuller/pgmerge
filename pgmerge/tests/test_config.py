"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os

import yaml
from click.testing import CliRunner
from sqlalchemy import MetaData, Table, Column, String, Integer, ForeignKey, select, text

from pgmerge import pgmerge
from .test_db import TestDB, create_table
from .helpers import write_file, compare_table_output, check_header


class TestConfig(TestDB):
    """
    Functional tests that use the CLI commands with table config files.
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
            compare_table_output(self, result.output, [
                ["other_table:"],
                ["skip:", "2", "insert:", "0", "update:", "0"],
                ["the_table:"],
                ["skip:", "0", "insert:", "0", "update:", "0"],
            ], "2 tables imported successfully")

            the_table_path = os.path.join(self.output_dir, "the_table.csv")
            check_header(self, the_table_path, ['id', 'code',
                                                'name', 'join_the_table_ref_other_table_fkey_code'])

            other_table_path = os.path.join(self.output_dir, "other_table.csv")
            check_header(self, other_table_path, ['id', 'code', 'name'])

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

    def test_parent_link(self):
        metadata = MetaData()
        area = Table('area', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('code', String(3)),
                     Column('name', String))
        party = Table('party', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('type', String(1), nullable=False),
                      Column('name', String))
        org = Table('organisation', metadata,
                    Column('id', Integer, primary_key=True),  # ForeignKey("party.id")
                    Column('code', String(3), nullable=False),
                    Column('name', String))
        party_area = Table('party_area', metadata,
                           Column('id', Integer, primary_key=True),
                           Column('party_id', Integer, nullable=False),
                           Column('area_id', Integer, nullable=False),
                           Column('type', String, nullable=False))

        config_data = {
            'area': {'columns': ['code', 'name'], 'alternate_key': ['code']},
            'organisation': {'columns': ['code', 'name'], 'alternate_key': ['code']},
            'party_area': {'columns': ['party_id', 'area_id', 'type'],
                           'alternate_key': ['party_id', 'area_id', 'type']},
        }
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file, \
             create_table(self.engine, area), \
             create_table(self.engine, party), \
             create_table(self.engine, org), \
             create_table(self.engine, party_area):
            yaml.dump(config_data, config_file, default_flow_style=False)
            with self.connection.begin():
                self.connection.execute(area.insert(None), [
                    {'id': 1, 'code': 'BWA', 'name': 'Botswana'},
                    {'id': 2, 'code': 'ZAF', 'name': 'South Africa'},
                    {'id': 3, 'code': 'ZWE', 'name': 'Zimbabwe'}
                ])
                self.connection.execute(party.insert(None), [
                    {'id': 1, 'type': 'O', 'name': 'First'},
                    {'id': 2, 'type': 'P', 'name': 'Second'},
                    {'id': 3, 'type': 'O', 'name': 'Third'}
                ])
                self.connection.execute(org.insert(None), [
                    {'id': 1, 'code': 'TBC', 'name': 'Table Mountain Co.'},
                    {'id': 3, 'code': 'ODR', 'name': 'Okavango Delta Resort'}
                ])
                self.connection.execute(party_area.insert(None), [
                    {'party_id': 1, 'area_id': 2, 'type': 'located_in'},
                    {'party_id': 3, 'area_id': 1, 'type': 'located_in'}
                ])

            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])

            result = self.runner.invoke(pgmerge.upsert, ['--config', config_file_path, '--disable-foreign-keys',
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            result_lines = result.output.splitlines()
            self.assertEqual(result_lines[-1], "4 tables imported successfully")

            # Delete exported files
            for export_file in ['area.csv', 'party.csv', 'organisation.csv', 'party_area.csv']:
                export_path = os.path.join(self.output_dir, export_file)
                os.remove(export_path)
