import click
import unittest
from pgmerge import pgmerge
from click.testing import CliRunner


class TestCLI(unittest.TestCase):

    def test_basics(self):
        runner = CliRunner()
        result = runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'NOTICE'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output, "Directory not found: 'NOTICE'\n")

        result = runner.invoke(pgmerge.export, ['--dbname', 'testdb', 'tmp'])
        self.assertEquals(result.output, "Exported 0 tables\n")

