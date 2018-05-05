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


def validate_table_configs_with_schema(inspector, schema, config_per_table):
    table_names = inspector.get_table_names(schema)
    unknown_tables = set(config_per_table.keys()) - set(table_names)
    if len(unknown_tables) > 0:
        raise ConfigInvalidException("table not found in database: {}".format(list(unknown_tables)))

    for table in config_per_table:
        db_columns = inspector.get_columns(table, schema)
        actual_columns = [col['name'] for col in db_columns]
        actual_skippable_columns = [col['name'] for col in db_columns if col['nullable'] or col['default'] is not None]
        actual_pk_columns = inspector.get_primary_keys(table, schema)

        table_config = config_per_table[table]

        alternate_key = table_config.get('alternate_key', None)
        if alternate_key is not None:
            unknown_pk_columns = set(alternate_key) - set(actual_columns)
            if len(unknown_pk_columns) > 0:
                raise ConfigInvalidException(
                    "'alternate_key' columns not found in table: {}".format(list(unknown_pk_columns)),
                    table)

        config_pk_columns = actual_pk_columns
        if alternate_key is not None:
            config_pk_columns = alternate_key

        config_columns = table_config.get('columns', None)
        if config_columns is not None:
            validate_config_columns(table, config_columns, actual_columns, skippable_columns, config_pk_columns)


def validate_config_columns(table, config_columns, actual_columns, skippable_columns, pk_columns):
    unknown_columns = set(config_columns) - set(actual_columns)
    if len(unknown_columns) > 0:
        raise ConfigInvalidException(
            "'columns' not found in table: {}".format(list(unknown_columns)),
            table)

    skipped_columns = set(actual_columns) - set(config_columns)
    unallowable_skipped_columns = set(skipped_columns) - set(skippable_columns)
    if len(unallowable_skipped_columns) > 0:
        raise ConfigInvalidException(
            "'columns' can't skip columns that aren't nullable or don't have defaults: {}"
                .format(list(unallowable_skipped_columns)), table)

    missing_pk_columns = set(pk_columns) - set(config_columns)
    if len(missing_pk_columns) > 0:
        raise ConfigInvalidException(
            "'columns' has to also contain primary/alternate keys, but doesn't contain {}"
                .format(list(missing_pk_columns)), table)



def retrieve_password(appname, dbname, host, port, username, password, type="postgresql", never_prompt=False):
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

    if password is None and not never_prompt:
        password = getpass.getpass("Password for {}: ".format(username))

    return password


def generate_url(dbname, host, port, username, password, type="postgresql"):
    config_db = {'type': type, 'host': host, 'port': port,
                 'username': username, 'password': password,
                 'dbname': dbname}
    url = "{type}://{username}:{password}@{host}:{port}/{dbname}".format(**config_db)
    return url


class ConfigInvalidException(Exception):
    """
    Exception raised for invalid config file.
    """

    def __init__(self, message, table=None):
        if table is not None:
            message = "Configuration for table '{}' is invalid:\n {}".format(table, message)
        else:
            message = "Configuration is invalid:\n {}".format(message)
        super().__init__(message)
