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

log = logging.getLogger(__name__)

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'default_config_schema.yml')
DB_CONFIG_FILE = "db_config.yml"
PGPASS_FILE = ".pgpass"


def load_config_for_db(appname, dbname, priority_config_for_db=None):
    """
    Loads any config for the specific database name from the default config file, but
    then merges those configs with the given configs which take higher priority.
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
        log.debug('Schema file missing (re-install recommended): {}'.format(schema_path))

    # Load default config
    config_path = os.path.join(user_config_dir(appname, appauthor=False), DB_CONFIG_FILE)
    ensure_file_exists(config_path)
    with open(config_path, 'r') as config_file:
        yaml_config = yaml.safe_load(config_file)

    # Validate config if it's not empty
    if yaml_config is not None and schema is not None and not schema.check(yaml_config):
        print("Default config is invalid: '%s'" % (config_path,))
        return None

    # Assign empty config
    final_config = {dbname: {'host': None, 'port': None, 'username': None, 'password': None}}
    # Override empty config with those from default config
    if yaml_config is not None and dbname in yaml_config:
        recursive_update_ignore_none(final_config, yaml_config)
    # Override default config with priority configs
    if priority_config_for_db is not None:
        recursive_update_ignore_none(final_config[dbname], priority_config_for_db)
    return final_config[dbname]


def combine_cli_and_db_configs_to_get_url(appname, dbname, host, port, username, password, type="postgresql"):
    """
    Combine command-line parameters with default config to get database connection URL.

    Command-line parameters take priority over defaults in config file. Will request password if not yet provided.
    """
    config_db_user = {'type': type, 'host': host, 'port': port, 'username': username, 'password': password}
    config_db = load_config_for_db(appname, dbname, config_db_user)
    if config_db is None:
        return
    if config_db['password'] is None:
        pgpass_path = os.path.join(user_config_dir(appname, appauthor=False), PGPASS_FILE)
        if not os.path.isfile(pgpass_path):
            pgpass_path = None
        pgpass = load_pgpass(config_db['host'], config_db['port'], dbname, config_db['username'],
                             pgpass_path=pgpass_path)
        if pgpass is None:
            config_db['password'] = getpass.getpass("Password for {}: ".format(config_db['username']))
        else:
            config_db['password'] = pgpass

    url = "{type}://{username}:{password}@{host}:{port}/{dbname}".format(dbname=dbname, **config_db)
    return url
