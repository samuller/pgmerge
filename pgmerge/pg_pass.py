"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import errno
import logging
from typing import Optional, List
from .utils import is_windows

_log = logging.getLogger(__name__)

# A string that is hopefully unused in hostnames, ports, databases, usernames or passwords
COLON_REPLACE_STRING = "<|\t COLON \t|>"  # string can't contain a colon or escaped backslash


def load_pgpass(hostname: str, port: str, database: str, username: str, pgpass_path: Optional[str] = None
                ) -> Optional[str]:  # pragma: no cover
    """
    Return a password if a matching entry is found in PostgreSQL's pgpass file.

    See: https://www.postgresql.org/docs/9.3/static/libpq-pgpass.html
    """
    if pgpass_path is None:
        pgpass_path = get_default_pgpass_path()
    if pgpass_path is None or not os.path.isfile(pgpass_path):
        return None

    # "Field can be a literal value, or *, which matches anything."
    def field_matches(pg_field: str, our_value: str) -> bool:
        return pg_field == '*' or our_value is None or pg_field == our_value

    def line_matches(fields: List[str]) -> bool:
        if len(fields) != 5:
            return False
        # hostname:port:database:username:password
        for field, our_value in zip(fields, [hostname, port, database, username]):
            if not field_matches(field, our_value):
                return False
        return True

    try:
        with open(pgpass_path, 'r') as pgpass_file:
            lines = pgpass_file.readlines()
            # Filter out comments
            lines = [line for line in lines if not line.startswith('#')]
            for line in lines:
                # "If an entry needs to contain : or \, escape this character with \."
                line = line.replace("\\:", COLON_REPLACE_STRING)
                line = line.replace("\\\\", "\\")

                fields = line.split(":")
                fields = [field.replace(COLON_REPLACE_STRING, ":") for field in fields]
                # "The password field from the first line that matches the current connection parameters will be used."
                if not line_matches(fields):
                    continue
                # Return password after removing any trailing newlines
                return fields[4].splitlines()[0]
    except IOError as err:
        if err.errno == errno.EACCES:
            return None
        raise err

    return None


def get_default_pgpass_path() -> str:
    """
    Return path where pgpass file is expected to be.

    See documentation at: https://www.postgresql.org/docs/10/static/libpq-pgpass.html
    """
    pgpass_path = os.getenv('PGPASSFILE')
    if pgpass_path is not None:
        return pgpass_path

    app_data_dir = os.getenv('APPDATA')
    if is_windows() and app_data_dir is not None:
        return os.path.join(app_data_dir, 'postgresql', 'pgpass.conf')

    return os.path.join(os.path.expanduser("~"), '.pgpass')
