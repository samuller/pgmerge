"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import unittest
from contextlib import contextmanager

from sqlalchemy import create_engine, text
import psycopg2.extensions as psyext


@contextmanager
def create_table(engine, table):
    table.create(engine, checkfirst=True)
    try:
        yield
    finally:
        table.drop(engine, checkfirst=False)


def find_open_connections(connection):
    return connection.execute(text("SELECT * FROM pg_stat_activity")).fetchall()


class TestDB(unittest.TestCase):
    """
    Class for setting up a test database and handling connections to it.

    Requires environment variable with connection details/URL to database, e.g.:
        DB_TEST_URL=postgres://postgres:password@localhost:5432/
    User has to have create database permissions.
    """

    env_var = "DB_TEST_URL"
    url = ''
    db_name = 'testdb'
    initial_db = 'template1'

    @classmethod
    def setUpClass(cls):
        # Create class variables to be re-used between tests
        cls.create_db_engine = None
        cls.engine = None
        cls.url = os.getenv(cls.env_var)
        if not cls.url:
            assert False, "No database URL set in '{}'".format(cls.env_var)
        try:
            cls.create_db(cls.db_name)
        except Exception as err:
            cls.drop_db(cls.db_name)
            raise err

    @classmethod
    def tearDownClass(cls):
        cls.drop_db(cls.db_name)

    @classmethod
    def create_db(cls, db_name):
        # Open connection to template database (could build url with sqlalchemy.engine.url.URL)
        cls.create_db_engine = create_engine(cls.url + cls.initial_db)
        with cls.create_db_engine.begin() as conn:
            conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_AUTOCOMMIT)
            conn.execute(text("DROP DATABASE IF EXISTS " + db_name))
            conn.execute(text("CREATE DATABASE " + db_name))
            # conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_DEFAULT)
        # self.connection.close()
        cls.create_db_engine.dispose()
        cls.create_db_engine = None

        cls.engine = create_engine(cls.url + db_name)

    @classmethod
    def drop_db(cls, db_name):
        if cls.engine is not None:
            cls.engine.dispose()
            cls.engine = None

        cls.create_db_engine = create_engine(cls.url + cls.initial_db)
        with cls.create_db_engine.connect() as conn:
            conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_AUTOCOMMIT)
            # print(find_open_connections(conn))
            with conn.begin():
                conn.execute(text("DROP DATABASE IF EXISTS " + db_name))
            # self.connection.connection.set_isolation_level(psyext.ISOLATION_LEVEL_DEFAULT)
        # self.connection.close()
        cls.create_db_engine.dispose()

    def setUp(self):
        # Open connection before each test
        self.open_db_conn()

    def tearDown(self):
        # Close connection after each test
        self.close_db_conn()

    def open_db_conn(self):
        self.connection = self.engine.connect()

    def close_db_conn(self):
        self.connection.close()
        self.connection = None

    def run_query(self, sql_stmt):
        """Helper function to execute raw SQL statements"""
        result = self.connection.execute(sql_stmt)
        all = result.fetchall()
        result.close()
        return all
