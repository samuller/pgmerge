import os
from pgmerge import pgmerge
from .test_db import TestDB
from click.testing import CliRunner


class TestCLI(TestDB):

    @classmethod
    def setUpClass(cls):
        super(TestCLI, cls).setUpClass()
        cls.runner = CliRunner()

    def test_basics(self):
        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'NOTICE'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output, "Directory not found: 'NOTICE'\n")

        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'tmp'])
        self.assertEquals(result.output, "Exported 0 tables\n")

    def test_export_table(self):
        self.connection.execute("DROP TABLE IF EXISTS films;")
        self.connection.execute("""
        CREATE TABLE films (
            code        char(5) CONSTRAINT firstkey PRIMARY KEY,
            title       varchar(40) NOT NULL,
            did         integer NOT NULL,
            date_prod   date,
            kind        varchar(10),
            len         interval hour to minute
        );
        """)

        result = self.runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'tmp'])
        self.assertEquals(result.output, "Exported 1 tables\n")
        os.remove('tmp/films.csv')

        self.connection.execute("DROP TABLE IF EXISTS films;")

