import os
import unittest
import sqlalchemy
import psycopg2.extensions as psyext
from contextlib import contextmanager


@contextmanager
def create_table(engine, table):
    table.create(engine, checkfirst=True)
    try:
        yield
    finally:
        table.drop(engine, checkfirst=False)


def find_open_connections(connection):
    return connection.execute("SELECT * FROM pg_stat_activity").fetchall()


class TestDB(unittest.TestCase):
    """
    Class for setting up a test database and handling connections to it.
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
        # Environment variable for test database, e.g.:
        #  DB_TEST_URL=postgres://postgres:password@localhost:5432/
        cls.url = os.getenv(cls.env_var)
        if not cls.url:
            assert False, "No database URL set in '{}'".format(cls.env_var)
        # Open connection to template database (could build url with sqlalchemy.engine.url.URL)
        cls.create_db_engine = sqlalchemy.create_engine(cls.url + cls.initial_db)
        with cls.create_db_engine.connect() as conn:
            conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_AUTOCOMMIT)
            conn.execute("DROP DATABASE IF EXISTS " + db_name)
            conn.execute("CREATE DATABASE " + db_name)
            # conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_DEFAULT)
        # self.connection.close()
        cls.create_db_engine.dispose()
        cls.create_db_engine = None

        cls.engine = sqlalchemy.create_engine(cls.url + db_name)

    @classmethod
    def drop_db(cls, db_name):
        if cls.engine is not None:
            cls.engine.dispose()
            cls.engine = None

        cls.create_db_engine = sqlalchemy.create_engine(cls.url + cls.initial_db)
        with cls.create_db_engine.connect() as conn:
            conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_AUTOCOMMIT)
            # print(find_open_connections(conn))
            conn.execute("DROP DATABASE IF EXISTS " + db_name)
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
