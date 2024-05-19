"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import copy
import logging
import getpass
import urllib.parse
from collections import Counter
from typing import Any, Dict, List, Set, Optional, Callable, cast

import yaml
import fastjsonschema
from appdirs import user_config_dir

from .pg_pass import load_pgpass

_log = logging.getLogger(__name__)

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tables_config_schema.yml')
DB_CONFIG_FILE = "db_config.yml"
PGPASS_FILE = ".pgpass"

# In Python 3.8+ we can use TypedDict and Literal (and remove some "type: ignore" comments)
# See: https://stackoverflow.com/questions/44225788/python-3-dictionary-with-known-keys-typing
# 3.8+: Literal['columns', 'alternate_key', 'where', 'subsets']
PerTableConfig = Dict[str, Any]
TablesConfig = Dict[str, PerTableConfig]
# 3.8+: Literal['name', 'where', 'columns']
SubsetConfig = Dict[str, Any]
# Combination of Subset and PerTable
# 3.8+: Literal['name', 'alternate_key', 'where', 'columns']
FileConfig = Dict[str, Any]


def load_config_for_tables(config_path: str) -> TablesConfig:
    """Load a config defining how tables should be imported and exported."""
    # Load YAML defining schema for validation of default config
    schema_path = SCHEMA_FILE
    if os.path.isfile(schema_path):
        with open(schema_path, 'r') as config_file:
            # We put JSON schema into YAML
            json_schema = yaml.safe_load(config_file)
    else:
        _log.warning('Config schema description is missing (re-install recommended): {}', schema_path)

    # Load config
    with open(config_path, 'r') as config_file:
        yaml_config = yaml.safe_load(config_file)

    try:
        # Validate config if it's not empty
        if yaml_config is not None and json_schema is not None:
            fastjsonschema.validate(json_schema, yaml_config)
    except fastjsonschema.JsonSchemaException as exc:
        raise ConfigInvalidException(
            f"incorrect format for '{config_path}', should match description in '{schema_path}'\n"
            + f" Details: {exc}")

    return cast(TablesConfig, yaml_config)


def convert_to_config_per_subset(config_per_table: TablesConfig) -> Dict[str, FileConfig]:
    """Subset configs include parent config and the configs of subset that override those of the parent."""
    subsets: Dict[str, List[str]] = {
        table: [subset['name'] for subset in config_per_table[table]['subsets']]
        for table in config_per_table if 'subsets' in config_per_table[table]
    }
    subsets_configs = {config['name']: config
                       for table in config_per_table if 'subsets' in config_per_table[table]
                       for config in cast(List[SubsetConfig], config_per_table[table]['subsets'])}
    subset_to_table = {name: table for table in subsets for name in subsets[table]}
    # Give copy parent configs to all subsets as a base
    cast_copy: Callable[[PerTableConfig], FileConfig] = lambda x: cast(FileConfig, copy.deepcopy(x))
    config_per_subset = {name: cast_copy(config_per_table[subset_to_table[name]]) for name in subset_to_table}
    for subset_name in subset_to_table:
        # Remove extra key to fully correct typing
        del cast(PerTableConfig, config_per_subset[subset_name])['subsets']
        # Overwrite keys that are defined on subset-level
        subset_config = subsets_configs[subset_name]
        for key in subset_config:
            config_per_subset[subset_name][key] = subset_config[key]

    # config_per_file = {(name + '.csv'): config_per_subset[name] for name in config_per_subset}
    return config_per_subset


def validate_table_configs_with_schema(inspector: Any, schema: str, config_per_table: TablesConfig
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

        subsets: Optional[List[SubsetConfig]] = table_config.get('subsets', None)
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


def validate_config_subsets(table: str, new_subsets: List[SubsetConfig], all_db_table_names: List[str],
                            known_subsets: Set[str]) -> None:
    """Check that subsets specified are valid tables and don't have duplicates."""
    table_subset_names = []
    if new_subsets is not None:
        table_subset_names = [subset['name'] for subset in new_subsets]

    self_duplicates = [k for k, v in Counter(table_subset_names).items() if v > 1]
    if len(self_duplicates) > 0:
        raise ConfigInvalidException("duplicate subset names: {}".format(self_duplicates), table)

    for subset in new_subsets:
        name = subset['name']
        if name in all_db_table_names:
            raise ConfigInvalidException(
                "subset name can't be the same as that of a table in the schema: {}".format(name), table)

    duplicate_names = known_subsets.intersection(set(table_subset_names))
    if len(duplicate_names) > 0:
        raise ConfigInvalidException(
            "subset names already in use: {}".format(sorted(list(duplicate_names))), table)


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
