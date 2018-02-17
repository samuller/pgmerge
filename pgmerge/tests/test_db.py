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


class TestDB(unittest.TestCase):
    """
    Class for setting up a test database and handling connections to it.
    """

    db_name = ''

    @classmethod
    def setUpClass(cls):
        # Create class variables to be re-used between tests
        cls.create_db_engine = None
        cls.initial_db = 'template1'
        cls.engine = None
        cls.db_name = 'testdb'

        try:
            TestDB.create_db(cls, cls.db_name)
        except Exception as err:
            TestDB.drop_db(cls, cls.db_name)
            raise err

    @classmethod
    def tearDownClass(cls):
        TestDB.drop_db(cls, cls.db_name)

    def create_db(self, db_name):
        # Environment variable for test database, e.g.:
        #  DB_TEST_URL=postgres://postgres:password@localhost:5432/
        self.url = os.getenv("DB_TEST_URL")
        if not self.url:
            self.skipTest("No database URL set")
        # Open connection to template database (could build url with sqlalchemy.engine.url.URL)
        self.create_db_engine = sqlalchemy.create_engine(self.url + self.initial_db)
        with self.create_db_engine.connect() as conn:
            conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_AUTOCOMMIT)
            conn.execute("DROP DATABASE IF EXISTS " + db_name)
            conn.execute("CREATE DATABASE " + db_name)
            # conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_DEFAULT)
        # self.connection.close()
        self.create_db_engine.dispose()
        self.create_db_engine = None

        self.engine = sqlalchemy.create_engine(self.url + db_name)

    def drop_db(self, db_name):
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None

        self.create_db_engine = sqlalchemy.create_engine(self.url + self.initial_db)
        with self.create_db_engine.connect() as conn:
            conn.connection.set_isolation_level(psyext.ISOLATION_LEVEL_AUTOCOMMIT)
            # print(find_open_connections(conn))
            conn.execute("DROP DATABASE IF EXISTS " + db_name)
            # self.connection.connection.set_isolation_level(psyext.ISOLATION_LEVEL_DEFAULT)
        # self.connection.close()
        self.create_db_engine.dispose()

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

    def find_open_connections(self, connection):
        return connection.execute("SELECT * FROM pg_stat_activity").fetchall()
