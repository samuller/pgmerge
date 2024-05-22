"""
Tests of the app's more advanced configurable capabilities.
"""
import os

import yaml
from click.testing import CliRunner
from sqlalchemy import MetaData, Table, Column, String, Integer, ForeignKey, select, text

from pgmerge import pgmerge
from .test_db import TestDB, create_table
from .helpers import count_lines, del_files, write_csv, write_file, compare_table_output, check_header


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

    def test_config_subsets(self):
        """
        Test import and export that uses config file to break table into subsets.
        """
        # Use a new metadata for each test since the database schema should be empty
        metadata = MetaData()
        the_table = Table('animals', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('type', String, nullable=False),
                          Column('name', String))
        config_data = {
            'animals': {
                'alternate_key': ['type', 'name'],
                'columns': ['type', 'name'],
                # Make parent export mutually exclusive with subsets if you don't want duplicated rows across CSVs
                'where': "type not in ('FISH', 'MAMMAL')",
                'subsets': [
                    {'name': 'fish', 'where': "type = 'FISH'"},
                    {'name': 'mammals', 'where': "type = 'MAMMAL'"},
                ]
            }
        }
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        animals_path = os.path.join(self.output_dir, "animals.csv")
        fish_path = os.path.join(self.output_dir, "fish.csv")
        mammals_path = os.path.join(self.output_dir, "mammals.csv")
        with write_file(config_file_path) as config_file, \
                create_table(self.engine, the_table), \
                del_files([animals_path, fish_path, mammals_path]):
            yaml.dump(config_data, config_file, default_flow_style=False)
            with self.connection.begin():
                self.connection.execute(the_table.insert(None), [
                    {'type': 'FISH', 'name': 'Salmon'},
                    {'type': 'FISH', 'name': 'Hake'},
                    {'type': 'MAMMAL', 'name': 'Elephant'},
                    {'type': 'MAMMAL', 'name': 'Whale'},
                    {'type': 'REPTILE', 'name': 'Lizard'},
                ])
            # Export
            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 1 tables to 3 files\n")
            self.assertEqual(result.exit_code, 0)
            # Check exported files
            check_header(self, animals_path, ['type', 'name'])
            self.assertEqual(count_lines(animals_path), 1+1)
            self.assertEqual(count_lines(fish_path), 1+2)
            self.assertEqual(count_lines(mammals_path), 1+2)
            # Import
            result = self.runner.invoke(pgmerge.upsert, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            compare_table_output(self, result.output, [
                ["animals:"],
                # TODO: skip count only looks at count of "parent" file?
                ["skip:", "1", "insert:", "0", "update:", "0"],
                # TODO: 1 table (3 files)
            ], "3 files imported successfully into 3 tables")
            self.assertEqual(result.exit_code, 0)

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
        the_table_path = os.path.join(self.output_dir, "the_table.csv")
        other_table_path = os.path.join(self.output_dir, "other_table.csv")
        with write_file(config_file_path) as config_file, \
                create_table(self.engine, other_table), \
                create_table(self.engine, the_table), \
                del_files([the_table_path, other_table_path]):
            with self.connection.begin():
                self.connection.execute(other_table.insert(None), [
                    {'code': 'IS', 'name': 'Iceland'},
                ])
                self.connection.execute(other_table.insert(None), [
                    {'code': 'IN'},
                ])
            yaml.dump(config_data, config_file, default_flow_style=False)
            # Export
            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output, "Exported 2 tables to 2 files\n")
            self.assertEqual(result.exit_code, 0)
            # Check exported files
            check_header(self, the_table_path, ['id', 'code',
                                                'name', 'join_the_table_ref_other_table_fkey_code'])
            self.assertEqual(count_lines(the_table_path), 1+0)
            check_header(self, other_table_path, ['id', 'code', 'name'])
            self.assertEqual(count_lines(other_table_path), 1+2)
            # Import
            result = self.runner.invoke(pgmerge.upsert, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            compare_table_output(self, result.output, [
                ["other_table:"],
                ["skip:", "2", "insert:", "0", "update:", "0"],
                ["the_table:"],
                ["skip:", "0", "insert:", "0", "update:", "0"],
            ], "2 files imported successfully into 2 tables")
            self.assertEqual(result.exit_code, 0)

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
        the_table_path = os.path.join(self.output_dir, "the_table.csv")
        with write_file(config_file_path) as config_file, \
                create_table(self.engine, the_table), del_files([the_table_path]), \
                self.connection:  # 'Select' requires us to close the connection before dropping the table
            with self.connection.begin():
                self.connection.execute(the_table.insert(None), [
                    {'code': 'LCY', 'name': 'London', 'parent_id': None},
                    {'code': 'NYC', 'name': 'New York City', 'parent_id': None},
                    {'code': 'MAIN', 'name': 'Main street', 'parent_id': 1},
                    {'code': 'MAIN', 'name': 'Main street', 'parent_id': 2},
                ])
            yaml.dump(config_data, config_file, default_flow_style=False)
            # Export
            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            result_lines = result.output.splitlines()
            self.assertEqual(result_lines[0].strip(),
                             "Self-referencing tables found that could prevent import: the_table")
            self.assertEqual(result_lines[3].strip(), "Exported 1 tables to 1 files")
            self.assertEqual(result.exit_code, 0)

            with self.connection.begin():
                # Clear table to see if import worked
                self.connection.execute(the_table.delete())
                # We reset sequence so that id numbers match the initial import
                self.connection.execute(text("ALTER SEQUENCE the_table_id_seq RESTART WITH 1"))

            # TODO: test idempotency with double import
            # self.runner.invoke(pgmerge.upsert, ['--config', config_file_path, '--disable-foreign-keys',
            #                                     '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            # Import
            self.runner.invoke(pgmerge.upsert, ['--config', config_file_path, '--disable-foreign-keys',
                                                '--dbname', self.db_name, '--uri', self.url, self.output_dir])

            result = self.run_query(select(the_table).order_by('id'))
            self.assertEqual(result, [
                (1, 'LCY', 'London', None), (2, 'NYC', 'New York City', None),
                (3, 'MAIN', 'Main street', None), (4, 'MAIN', 'Main street', None)])

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
             create_table(self.engine, party_area), \
             del_files([os.path.join(self.output_dir, ef) for ef in
                        ['area.csv', 'party.csv', 'organisation.csv', 'party_area.csv']]):
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
            # Export
            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.exit_code, 0)
            # Import
            result = self.runner.invoke(pgmerge.upsert, ['--config', config_file_path, '--disable-foreign-keys',
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            result_lines = result.output.splitlines()
            self.assertEqual(result_lines[-1], "4 files imported successfully into 4 tables")
            self.assertEqual(result.exit_code, 0)

    def test_single_table(self):
        """
        Test import with --single-table option that generates config file to import multiple files into a single table.
        """
        # Use a new metadata for each test since the database schema should be empty
        metadata = MetaData()
        creatures_table = Table('creatures', metadata,
                                Column('id', Integer, primary_key=True),
                                Column('type', String, nullable=False),
                                Column('name', String))
        animals_path = os.path.join(self.output_dir, "creatures.csv")
        fish_path = os.path.join(self.output_dir, "fish.csv")
        mammals_path = os.path.join(self.output_dir, "mammals.csv")
        with write_file(animals_path), write_file(mammals_path), write_file(fish_path):
            write_csv(animals_path, [
                [1, 'type', 'name'],
                [6, 'REPTILE', 'Lizard'],
            ])
            write_csv(fish_path, [
                [1, 'type', 'name'],
                [2, 'FISH', 'Salmon'],
                [3, 'FISH', 'Hake'],
            ])
            write_csv(mammals_path, [
                [1, 'type', 'name'],
                [4, 'MAMMAL', 'Elephant'],
                [5, 'MAMMAL', 'Whale'],
            ])
            # TODO: test with a config (generated config combines with it)
            # First test failed import into empty database
            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url, '--single-table',
                                                         self.output_dir, 'creatures'])
            self.assertEqual(result.output.splitlines(), [
                "Tables not found in database:",
                "\tcreatures"
            ])
            # Test working import once tables are created
            with create_table(self.engine, creatures_table):
                # TODO: test with a config (generated config combines with it)
                result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                             '--single-table', self.output_dir, 'creatures'])
                compare_table_output(self, result.output, [
                    ["creatures:"],
                    ["skip:", "0", "insert:", "1", "update:", "0"],
                    ["creatures", "[fish]:"],
                    ["skip:", "0", "insert:", "2", "update:", "0"],
                    ["creatures", "[mammals]:"],
                    ["skip:", "0", "insert:", "2", "update:", "0"],
                ], "3 files imported successfully into 3 tables")
                self.assertEqual(result.exit_code, 0)

    def test_invalid_tables(self):
        """
        Test import when tables are not found in database.
        """
        animals_path = os.path.join(self.output_dir, "creatures.csv")
        fish_path = os.path.join(self.output_dir, "fish.csv")
        mammals_path = os.path.join(self.output_dir, "mammals.csv")
        with write_file(animals_path), write_file(mammals_path), write_file(fish_path):
            # Test failed import into empty database
            result = self.runner.invoke(pgmerge.upsert, ['--dbname', self.db_name, '--uri', self.url,
                                                         self.output_dir])
            self.assertEqual(result.output.splitlines()[0], "Skipping files for unknown tables:")
            self.assertEqual([line.strip() for line in result.output.splitlines()[6:6+7]], [
                "Total results:", "skip: 0", "insert: 0", "update: 0", "total: 0", "",
                "3 tables skipped due to errors:",
            ])
            self.assertEqual(result.output.splitlines()[-1], "0 files imported successfully into 0 tables")
            self.assertEqual(result.exit_code, 0)

    def test_invalid_config_format(self):
        """
        Test invalid config checking.
        """
        config_data = {
            'animals': {
                'invalid_key': ['type', 'name'],
            }
        }
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file:
            yaml.dump(config_data, config_file, default_flow_style=False)
            # Export
            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output.splitlines()[0], "Configuration is invalid:")
            self.assertTrue(result.output.splitlines()[1].startswith(" incorrect format for"))
            self.assertEqual(result.exit_code, pgmerge.EXIT_CODE_EXC)

    def test_invalid_config_no_table(self):
        """
        Test config invalid as it references a non-existent table.
        """
        config_data = {
            'animals': {
                'columns': ['type', 'name'],
            }
        }
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file:
            yaml.dump(config_data, config_file, default_flow_style=False)
            # Export
            result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                         '--dbname', self.db_name, '--uri', self.url, self.output_dir])
            self.assertEqual(result.output.splitlines(), [
                "Configuration is invalid:",
                " table not found in database: ['animals']"
            ])
            self.assertEqual(result.exit_code, pgmerge.EXIT_CODE_EXC)

    def test_invalid_config_table_match(self):
        """
        Test config invalid as it doesn't match with table schema.
        """
        metadata = MetaData()
        animal_table = Table('animals', metadata,
                             Column('id', Integer, primary_key=True),
                             Column('type', String, nullable=False),
                             Column('name', String))
        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file, create_table(self.engine, animal_table):
            config_and_error = [
                {
                    'config_data': {
                        'animals': {'columns': ['type', 'name']}
                    },
                    'error': " 'columns' has to also contain primary/alternate keys, but doesn't contain ['id']"
                },
                {
                    'config_data': {
                        'animals': {'columns': ['type', 'name__invalid']}
                    },
                    'error': " 'columns' not found in table: ['name__invalid']"
                },
                {
                    'config_data': {
                        'animals': {'columns': ['name']}
                    },
                    'error': " 'columns' can't skip columns that aren't nullable or don't have defaults: ['type']"
                },
                {
                    'config_data': {
                        'animals': {'alternate_key': ['type', 'name__invalid']}
                    },
                    'error': " 'alternate_key' columns not found in table: ['name__invalid']"
                }
            ]
            for conferr in config_and_error:
                config_data = conferr['config_data']
                error_msg = conferr['error']
                yaml.dump(config_data, config_file, default_flow_style=False)
                # Export
                result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                             '--dbname', self.db_name, '--uri', self.url,
                                                             self.output_dir])
                self.assertEqual(result.output.splitlines(), [
                    "Configuration for table 'animals' is invalid:",
                    error_msg
                ])
                self.assertEqual(result.exit_code, pgmerge.EXIT_CODE_EXC)

    def test_invalid_config_subsets(self):
        """
        Test config invalid as it has invalid subsets.
        """
        metadata = MetaData()
        animal_table = Table('animals', metadata,
                             Column('id', Integer, primary_key=True),
                             Column('type', String, nullable=False),
                             Column('name', String))
        other_table = Table('other_table', metadata, Column('id', Integer, primary_key=True))

        config_file_path = os.path.join(self.output_dir, 'test.yml')
        with write_file(config_file_path) as config_file, \
             create_table(self.engine, animal_table), create_table(self.engine, other_table):
            config_and_error = [
                {
                    'config_data': {
                        'animals': {'subsets': [
                            {'name': 'fish', 'where': "type = 'FISH'"},
                            {'name': 'fish', 'where': "type = 'FISH'"}
                        ]}
                    },
                    'output': [
                        "Configuration for table 'animals' is invalid:",
                        " duplicate subset names: ['fish']"
                    ]
                },
                {
                    'config_data': {
                        'animals': {'subsets': [
                            {'name': 'fish', 'where': "type = 'FISH'"}
                        ]},
                        'other_table': {'subsets': [
                            {'name': 'fish', 'where': "type = 'FISH'"},
                        ]}
                    },
                    'output': [
                        "Configuration for table 'other_table' is invalid:",
                        " subset names already in use: ['fish']"
                    ]
                },
                {
                    'config_data': {
                        'animals': {'subsets': [
                            {'name': 'fish', 'where': "type = 'FISH'"},
                            {'name': 'other_table', 'where': "type = 'FISH'"},
                        ]}
                    },
                    'output': [
                        "Configuration for table 'animals' is invalid:",
                        " subset name can't be the same as that of a table in the schema: other_table"
                    ]
                }
            ]
            for conferr in config_and_error:
                config_data = conferr['config_data']
                output_msg = conferr['output']
                yaml.dump(config_data, config_file, default_flow_style=False)
                # Export
                result = self.runner.invoke(pgmerge.export, ['--config', config_file_path,
                                                             '--dbname', self.db_name, '--uri', self.url,
                                                             self.output_dir])
                self.assertEqual(result.output.splitlines(), output_msg)
                self.assertEqual(result.exit_code, pgmerge.EXIT_CODE_EXC)
