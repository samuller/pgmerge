"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import urllib
import logging
import getpass
from typing import Any, Dict, List, Set, Optional, cast

import yaml
from rxjson import Rx
from appdirs import user_config_dir

from .pg_pass import load_pgpass

_log = logging.getLogger(__name__)

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tables_config_schema.yml')
DB_CONFIG_FILE = "db_config.yml"
PGPASS_FILE = ".pgpass"


def load_config_for_tables(config_path: str) -> Dict[str, Any]:
    """Load a config defining how tables should be imported and exported."""
    # Load YAML defining schema for validation of default config
    schema_path = SCHEMA_FILE
    schema = None
    if os.path.isfile(schema_path):
        with open(schema_path, 'r') as config_file:
            schema_config = yaml.safe_load(config_file)
            rxf = Rx.Factory({"register_core_types": True})
            schema = rxf.make_schema(schema_config)
    else:
        _log.warning('Config schema description is missing (re-install recommended): {}', schema_path)

    # Load config
    with open(config_path, 'r') as config_file:
        yaml_config = yaml.safe_load(config_file)

    # Validate config if it's not empty
    if yaml_config is not None and schema is not None and not schema.check(yaml_config):
        # _log.warning("Config is invalid: '%s'" % (config_path,))
        raise ConfigInvalidException("incorrect format for '{}', should match description in '{}'"
                                     .format(config_path, schema_path))
        # return None
    return cast(Dict[str, Any], yaml_config)


def validate_table_configs_with_schema(inspector: Any, schema: str, config_per_table: Dict[str, Any]
                                       ) -> None:
    """Check that config matches the current schema and tables without any inconsistencies."""
    table_names = inspector.get_table_names(schema)
    unknown_tables = set(config_per_table.keys()) - set(table_names)
    if len(unknown_tables) > 0:
        raise ConfigInvalidException("table not found in database: {}".format(list(unknown_tables)))

    subset_names: Set[str] = set()
    for table in config_per_table:
        db_columns = inspector.get_columns(table, schema)
        actual_columns = [col['name'] for col in db_columns]
        skippable_columns = [col['name'] for col in db_columns if col['nullable'] or col['default'] is not None]
        actual_pk_columns = inspector.get_pk_constraint(table, schema)['constrained_columns']

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

        subsets = table_config.get('subsets', None)
        if subsets is not None:
            validate_config_subsets(table, subsets, table_names, subset_names)
            subset_names.update([subset['name'] for subset in subsets])


def validate_config_columns(table: str, config_columns: List[str], actual_columns: List[str],
                            skippable_columns: List[str], pk_columns: List[str]) -> None:
    """Check that columns specified in config match those in table."""
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


def validate_config_subsets(table: str, new_subsets: List[Dict[str, Any]], table_names: List[str],
                            known_subsets: Set[str]) -> None:
    """Check that subsets specified are valid tables and don't have duplicates."""
    for subset in new_subsets:
        name = subset['name']
        if name in table_names:
            raise ConfigInvalidException(
                "subset name can't be the same as that of a table in the schema: {}".format(name), table)

    table_subset_names = set()
    if new_subsets is not None:
        table_subset_names = {subset['name'] for subset in new_subsets}
    duplicate_names = known_subsets.intersection(table_subset_names)
    if len(duplicate_names) > 0:
        raise ConfigInvalidException(
            "duplicate subset names: {}".format(sorted(list(duplicate_names))), table)


def retrieve_password(appname: str, dbname: str, host: str, port: str, username: str, password: Optional[str],
                      type: str = "postgresql", never_prompt: bool = False) -> str:
    """
    If password isn't yet available, make sure to get it.

    Either by loading it from the appropriate config files or else by asking the user.
    """
    if password is not None:
        return password
    # With Postgresql we look for a pgpass file
    if type == "postgresql":
        pgpass_path: Optional[str] = os.path.join(user_config_dir(appname, appauthor=False), PGPASS_FILE)
        if pgpass_path and not os.path.isfile(pgpass_path):
            pgpass_path = None
        password = load_pgpass(host, port, dbname, username, pgpass_path=pgpass_path)

    if password is None and not never_prompt:
        password = getpass.getpass("Password for {}: ".format(username))

    return cast(str, password)


def generate_url(uri: Optional[str], dbname: str, host: str, port: str, username: str, password: str,
                 type: str = "postgresql") -> str:
    """Generate connection string URL from various connection details."""
    if uri:
        uri = uri if uri[-1] != '/' else uri[:-1]
        return "{}/{}".format(uri, dbname)

    config_db = {'type': type, 'port': port,
                 'host': urllib.parse.quote(host),
                 'username': urllib.parse.quote(username),
                 'password': urllib.parse.quote(password),
                 'dbname': urllib.parse.quote(dbname)}
    url = "{type}://{username}:{password}@{host}:{port}/{dbname}".format(**config_db)
    return url


class ConfigInvalidException(Exception):
    """Exception raised for invalid config file."""

    def __init__(self, message: str, table: Optional[str] = None) -> None:
        if table is not None:
            message = "Configuration for table '{}' is invalid:\n {}".format(table, message)
        else:
            message = "Configuration is invalid:\n {}".format(message)
        super().__init__(message)
