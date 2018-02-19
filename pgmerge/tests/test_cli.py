import os
from .test_db import *
from sqlalchemy import *
from pgmerge import pgmerge
from click.testing import CliRunner


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
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'NOTICE'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output, "Directory not found: 'NOTICE'\n")

        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
        self.assertEquals(result.output, "Exported 0 tables\n")

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
        with create_table(self.engine, table):
            stmt = table.insert().values([
                ('CI', 'Côte d’Ivoire'),
                ('EG', 'Egypt'),
                ('RE', 'Re-union'),
            ])
            self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
            self.assertEquals(result.output, "Exported 1 tables\n")

        with create_table(self.engine, table):
            stmt = table.insert().values([
                ('EG', 'Egypt'),
                ('RE', 'Réunion'),
                ('ST', 'São Tomé and Príncipe'),
            ])
            self.connection.execute(stmt)

            result = self.runner.invoke(pgmerge.upsert, ['--dbname', 'testdb', self.output_dir, table_name])
            result_lines = result.output.splitlines()
            self.assertEquals(result_lines[0], "country:")
            self.assertEquals(result_lines[1].strip().split(), ["skip:", "1", "insert:", "1", "update:", "1"])
            self.assertEquals(result_lines[-1], "1 tables imported successfully")

        os.remove(os.path.join(self.output_dir, "{}.csv".format(table_name)))
