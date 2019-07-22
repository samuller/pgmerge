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

The user has to have rights to create a new database. Then you can run the tests with `nosetests` or `pytest`, e.g.:

    nosetests --exe --nocapture --stop

To install `nosetests` so that it uses the virtualenv might require:

    pip install nose -I

and then reactivating the virtualenv with:

    deactivate
    . .env/bin/activate

Build wheel with:

    # https://packaging.python.org/guides/using-testpypi/
    python setup.py bdist_wheel

Upload wheel:

    twine upload --repository-url https://test.pypi.org/legacy/ dist/*

Test it with:

    sudo pip3 install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple pgmerge
