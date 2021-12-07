"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os

import yaml
from click.testing import CliRunner
from sqlalchemy import MetaData, Table, Column, String, Integer, inspect, select

from pgmerge import pgmerge
from .helpers import write_file
from .test_db import TestDB, create_table


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
            'party_area': {'columns': ['party_id', 'area_id', 'type'], 'alternate_key': ['party_id', 'area_id', 'type']},
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
