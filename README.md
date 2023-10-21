# pgmerge - a PostgreSQL data import and merge utility

[![Build Status](https://github.com/samuller/pgmerge/actions/workflows/tests.yml/badge.svg)](https://github.com/samuller/pgmerge/actions)
[![PyPI Version](https://badge.fury.io/py/pgmerge.svg)](https://badge.fury.io/py/pgmerge)
[![Code Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/samuller/3c84321138784d39b31a02d7fe93b31d/raw/badge-coverage.json)](https://github.com/samuller/pgmerge/actions)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

This utility's main purpose is to manage a set of CSV files that correspond with tables in a PostgreSQL database. Each of these CSV files can then be *merged* into their table, meaning that the following process will occur (also called an *upsert* operation):

* Rows whose primary key don't yet exist in the table will be imported.
* When the primary key already exists, row values will be updated.
* Rows that are missing or unchanged will be ignored.

pgmerge can then also export data in the *same format* expected for import.

These features allow you to move data between databases with the same schema to keep them up to date and in sync, although it does not cover handling deleted data.

    $ pgmerge --help
    Usage: pgmerge [OPTIONS] COMMAND [ARGS]...

    Merges data in CSV files into a Postgresql database.

    Options:
    --version  Show the version and exit.
    --help     Show this message and exit.

    Commands:
    export  Export each table to a CSV file.
    import  Import/merge each CSV file into a table.
    inspect  Inspect database schema in various ways.

### Import

    $ pgmerge import --help
    Usage: pgmerge import [OPTIONS] DIRECTORY [TABLES]...

    Import/merge each CSV file into a table.

    All CSV files need the same name as their matching table and have to be located
    in the given directory. If one or more tables are specified then only they will
    be used, otherwise all tables found will be selected.

    Options:
    -d, --dbname TEXT               Database name to connect to.  [required]
    -h, --host TEXT                 Database server host or socket directory.
                                    [default: localhost]
    -p, --port TEXT                 Database server port.  [default: 5432]
    -U, --username TEXT             Database user name.  [default: postgres]
    -s, --schema TEXT               Database schema to use.  [default: public]
    -w, --no-password               Never prompt for password (e.g. peer
                                    authentication).
    -W, --password TEXT             Database password (default is to prompt for
                                    password or read config).
    -L, --uri TEXT                  Connection URI can be used instead of specifying
                                    parameters separately (also sets --no-password).
    -f, --ignore-cycles             Don't stop import when cycles are detected in
                                    schema (will still fail if there are cycles in
                                    data)
    -F, --disable-foreign-keys      Disable foreign key constraint checking during
                                    import (necessary if you have cycles, but requires
                                    superuser rights).
    -c, --config PATH               Config file for customizing how tables are
                                    imported/exported.
    -i, --include-dependent-tables  When selecting specific tables, also include all
                                    tables on which they depend due to foreign key
                                    constraints.
    --help                          Show this message and exit.

## Installation

> WARNING: the reliability of this utility is not guaranteed and loss or corruption of data is always a possibility.

### Install from PyPI

With `Python 3` installed on your system, you can run:

    pip install pgmerge

(your `pip --version` has to be 9.0 or greater). To test that installation worked, run:

    pgmerge --help

and you can uninstall at any time with:

    pip uninstall pgmerge

### Install from Github

To install the newest code directly from Github:

    pip install git+https://github.com/samuller/pgmerge

### Issues

If you have trouble installing and you're running a Debian-based Linux that uses `Python 2` as its system default, then you might need to run:

    sudo apt install libpq-dev python3-pip python3-setuptools
    sudo -H pip3 install pgmerge
