import os
from .test_db import *
from sqlalchemy import *
from pgmerge import pgmerge
from click.testing import CliRunner


class TestCLI(TestDB):

    @classmethod
    def setUpClass(cls):
        super(TestCLI, cls).setUpClass()
        cls.runner = CliRunner()
        cls.metadata = MetaData()

    def test_basics(self):
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'NOTICE'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output, "Directory not found: 'NOTICE'\n")

        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'tmp'])
        self.assertEquals(result.output, "Exported 0 tables\n")

    def test_export_table(self):
        table_name = 'film'
        film = Table(table_name, self.metadata,
                     Column('code', Integer, primary_key=True),
                     Column('name', String(16), nullable=False)
                     )
        with create_table(self.engine, film):
            result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'tmp'])
            self.assertEquals(result.output, "Exported 1 tables\n")
            os.remove('tmp/{}.csv'.format(table_name))
