"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
"""
import os
import yaml
import logging
import getpass
from .utils import *
from rxjson import Rx
from .pg_pass import *
from appdirs import user_config_dir

_log = logging.getLogger(__name__)

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tables_config_schema.yml')
DB_CONFIG_FILE = "db_config.yml"
PGPASS_FILE = ".pgpass"


def load_config_for_tables(config_path):
    """
    Loads a config defining how tables should be imported and exported.
    """
    # Load YAML defining schema for validation of default config
    schema_path = SCHEMA_FILE
    schema = None
    if os.path.isfile(schema_path):
        with open(schema_path, 'r') as config_file:
            schema_config = yaml.safe_load(config_file)
            rx = Rx.Factory({"register_core_types": True})
            schema = rx.make_schema(schema_config)
    else:
        _log.warning('Config schema description is missing (re-install recommended): {}'.format(schema_path))

    # Load config
    with open(config_path, 'r') as config_file:
        yaml_config = yaml.safe_load(config_file)

    # Validate config if it's not empty
    if yaml_config is not None and schema is not None and not schema.check(yaml_config):
        _log.warning("Config is invalid: '%s'" % (config_path,))
        return None
    return yaml_config



def retrieve_password(appname, dbname, host, port, username, password, type="postgresql"):
    """
    If password isn't yet available, make sure to get it. Either by loading it from the appropriate config files
    or else by asking the user.
    """
    if password is not None:
        return password
    # With Postgresql we look for a pgpass file
    if type == "postgresql":
        pgpass_path = os.path.join(user_config_dir(appname, appauthor=False), PGPASS_FILE)
        if not os.path.isfile(pgpass_path):
            pgpass_path = None
        password = load_pgpass(host, port, dbname, username, pgpass_path=pgpass_path)

    if password is None:
        password = getpass.getpass("Password for {}: ".format(username))

    return password


def generate_url(dbname, host, port, username, password, type="postgresql"):
    config_db = {'type': type, 'host': host, 'port': port,
                 'username': username, 'password': password,
                 'dbname': dbname}
    url = "{type}://{username}:{password}@{host}:{port}/{dbname}".format(**config_db)
    return url
