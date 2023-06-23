# Development

Clone the repo and perform all these following steps from the root directory.

## Setup virtualenv

[Install poetry](https://python-poetry.org/docs/#osx--linux--bashonwindows-install-instructions):

    curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.4.0 python -

Install packages:

    poetry install

Activate virtual environment (and app command - in editable mode):

    poetry shell

If it worked correctly, the following will be in your path and be able to run anywhere:

    pgmerge --help

## Code style

You can check PEP8 code style by using `flake8`:

    poetry run flake8 --exclude "tests" pgmerge/
    # To ignore doc strings issues:
    # poetry run flake8 --extend-ignore=D pgmerge/


    pip install pylint
    pylint --rcfile=setup.cfg pgmerge

Type checking can be done with `mypy`:

    # Install any type libraries found for current dependencies
    poetry run mypy --install-types
    # Do strict checking on non-test code
    poetry run mypy --ignore-missing-imports --strict pgmerge/*.py
    # Do lighter checks on test code
    poetry run mypy --ignore-missing-imports pgmerge/tests/

## Tests

To run the tests you'll need to set an environment variable with a database connection URL, e.g.:

    DB_TEST_URL=postgresql://postgres:password@localhost:5432/

The user has to have rights to create a new database. Then you can run the tests with `nosetests` or `pytest`, e.g.:

    poetry run pytest --capture=no --exitfirst

## Code coverage

To determine code coverage of the tests, include theses arguments:

    poetry run pytest --cov-report html --cov-report term --cov pgmerge

## Packaging

Build wheel with:

    # Make sure no local changes are distributed
    git stash
    # Build
    poetry build
    # Delete any cached build data
    # rm -rf pgmerge.egg-info/ build/

View package contents:

    # View files in source package
    tar -tf $(ls -1t dist/*.tar.gz | head -n1)
    # View files in wheel binary distribution
    jar -tf $(ls -1t dist/*.whl | head -n1)

Upload wheel:

    poetry config repositories.testpypi https://test.pypi.org/legacy/
    poetry publish -r testpypi --dry-run
    poetry publish -r testpypi

Test it with:

    pip install -i https://test.pypi.org/simple/ pgmerge

## Poetry helpers

`poetry` environment can be reset with:

    rm -rf `poetry env list --full-path`

Check for outdated dependencies with:

    poetry show -l

Update all dependencies to latest matching version specs in `pyproject.toml` (can thus change dependency version by manually editing the version spec first):

    poetry update

Add a new dependency or change dependencies with the following (which also includes/forces a `poetry update` to run):

    poetry add name_of_lib@latest

