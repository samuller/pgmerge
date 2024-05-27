"""
Tests for utility functions.
"""
import unittest

from pgmerge.db_config import generate_url


class TestUtils(unittest.TestCase):
    """
    Class for setting different utility functions
    """

    def test_pgpass_url(self):
        url = generate_url("user@localhost", "db", "", "", "", "", "")
        self.assertEqual(url, "user@localhost/db")

        url = generate_url("user@localhost/", "db", "", "", "", "", "")
        self.assertEqual(url, "user@localhost/db")

        url = generate_url(
            None,
            dbname="test_db",
            host="localhost",
            port="5432",
            username="user",
            password="password",
        )
        self.assertEqual(url, "postgresql://user:password@localhost:5432/test_db")

        url = generate_url(
            None,
            dbname="test_db",
            host="localhost",
            port="5432",
            username="user",
            password="pass@word",
        )
        self.assertEqual(url, "postgresql://user:pass%40word@localhost:5432/test_db")
