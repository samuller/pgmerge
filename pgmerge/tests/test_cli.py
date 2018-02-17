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
        cls.runner = CliRunner()
        cls.metadata = MetaData()

    def test_basics(self):
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'NOTICE'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output, "Directory not found: 'NOTICE'\n")

        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
        self.assertEquals(result.output, "Exported 0 tables\n")

    def test_export_table(self):
                     )
        table_name = 'country'
        table = Table(table_name, self.metadata,
                      Column('code', String(2), primary_key=True),
                      Column('name', String, nullable=False),
        with create_table(self.engine, table):
            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', self.output_dir])
            self.assertEquals(result.output, "Exported 1 tables\n")
            os.remove('{}/{}.csv'.format(self.output_dir, table_name))

