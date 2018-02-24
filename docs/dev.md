# Development

Clone the repo and perform all these following steps from the root directory.

Setup a virtualenv (optional, but recommended):

    virtualenv -p python3 .env
    . .env/bin/activate

Then install required modules:

    pip install -r requirements.txt

Install in editable mode:

    pip install -e .

If it worked correctly, the following will be in your path and be able to run anywhere:

    pgmerge --help

To run the tests you'll need to set an environment variable with a database connection URL, e.g.:

    DB_TEST_URL=postgres://postgres:password@localhost:5432/

The user has to have rights to create a new database. Then you can run the tests with:

    nosetests

Build wheel with:

    python setup.py bdist_wheel

