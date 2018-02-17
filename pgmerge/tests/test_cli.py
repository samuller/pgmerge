import os
from .test_db import *
from sqlalchemy import *
from pgmerge import pgmerge
from click.testing import CliRunner


class TestCLI(TestDB):

    @classmethod
    def setUpClass(cls):
        super(TestCLI, cls).setUpClass()
        cls.output_dir = '_tmp_test'
        os.makedirs(cls.output_dir)
        cls.runner = CliRunner()
        cls.metadata = MetaData()

    @classmethod
    def tearDownClass(cls):
        super(TestCLI, cls).tearDownClass()
        os.rmdir(cls.output_dir)

    def test_basics(self):
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'NOTICE'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output, "Directory not found: 'NOTICE'\n")

        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
        self.assertEquals(result.output, "Exported 0 tables\n")

    def test_export_table(self):
        table_name = 'country'
        table = Table(table_name, self.metadata,
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False),
                      extend_existing=True)
        with create_table(self.engine, table):
            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
            self.assertEquals(result.output, "Exported 1 tables\n")
            os.remove('{}/{}.csv'.format(self.output_dir, table_name))

    def test_export_and_import_with_utf8_values(self):
        table_name = 'country'
        table = Table(table_name, self.metadata,
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False),
                      extend_existing=True)
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
            self.assertEquals(result_lines[0], """country:""")
            self.assertEquals(result_lines[1].strip().split(), ["skip:", "3", "insert:", "0", "update:", "0"])
            self.assertEquals(result_lines[-1], "1 tables imported successfully")

            os.remove('{}/{}.csv'.format(self.output_dir, table_name))
