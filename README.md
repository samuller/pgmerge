# pgmerge - PostgreSQL data import and merge utility

This utility will read CSV files, one per table, and *merge* its rows into a database. This means that it will:

* Import rows that don't yet exist.
* Update rows that are already found in the database.
* Ignore unchanged or missing rows.

This tool can also export data in the same format expected for import.

These features allow you to move data between active/in-use databases to keep them up to date and in sync, although it does not cover handling removals.

    Usage: pgmerge [OPTIONS] COMMAND [ARGS]...

    Merges data in CSV files into a Postgresql database.

    Options:
    --version  Show the version and exit.
    --help     Show this message and exit.

    Commands:
    export  Export each table to a CSV file.
    import  Import/merge each CSV file into a table.
    inspect  Inspect database schema in various ways.

Import:

    Usage: pgmerge import [OPTIONS] [DIRECTORY] [TABLES]...

    Import/merge each CSV file into a table.

    All CSV files need the same name as their matching table and have to be located in the given directory
    (default: 'tmp'). If one or more tables are specified then only they will be used, otherwise all tables found
    will be selected.

    Options:
    -d, --dbname TEXT               Database name to connect to.  [required]
    -h, --host TEXT                 Database server host or socket directory.  [default: localhost]
    -p, --port TEXT                 Database server port.  [default: 5432]
    -U, --username TEXT             Database user name.
    -s, --schema TEXT               Database schema to use.  [default: public]
    -W, --password TEXT             Database password (default is to prompt for password or read config).
    -f, --disable-foreign-keys      Disable foreign key constraint checking during import (necessary if you have
                                    cycles, but requires superuser rights).
    -i, --include-dependent-tables  When selecting specific tables, also include all tables that depend on those
                                    tables due to foreign key constraints.
    --help                          Show this message and exit.

## Installation

    pip install git+https://github.com/samuller/pgmerge
    pgmerge --help

You can uninstall at any time with:

    pip uninstall pgmerge

If you have trouble installing and you're running a Debian-based Linux that uses Python2 as it's system default, then you might need to run:

    sudo apt install python3-pip python3-setuptools
    sudo pip3 install git+https://github.com/samuller/pgmerge








