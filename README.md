# pgmerge - PostgreSQL data import and merge utility

    Usage: pgmerge.py [OPTIONS] [DIRECTORY] [TABLES]...

    Merges data in CSV files (from the given directory, default: 'tmp') into a Postgresql database.
    If one or more tables are specified then only they will be used and any data for other tables
    will be ignored.

    Options:
    -d, --dbname TEXT               database name to connect to  [required]
    -h, --host TEXT                 database server host or socket directory  [default: localhost]
    -p, --port TEXT                 database server port  [default: 5432]
    -U, --username TEXT             database user name
    -s, --schema TEXT               database schema to use  [default: public]
    -W, --password TEXT             database password (default is to prompt for password or read config)
    -i, --include-dependent-tables  when selecting specific tables, also include all tables that depend on those
                                    tables due to foreign key constraints
    -f, --disable-foreign-keys      disable foreign key constraint checking during import (necessary if you have
                                    cycles, but requires superuser rights)
    -e, --export                    instead of import/merge, export all tables to directory
    --version                       Show the version and exit.
    --help                          Show this message and exit.

## Installation

    pip install .
    pgmerge --version

You can uninstall at any time with:

    pip uninstall pgmerge








