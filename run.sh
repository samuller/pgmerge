#!/bin/bash
#
# Script for running commonly used commands quickly, e.g. "./run.sh lint". See: "./run.sh help".
#

# Fail on first error.
set -e

man_help="List all commands available."
help() {
    echo "You have to provide a command to run, e.g. '$0 lint'"
    # List declared functions that are not from exports (-fx).
    commands=$(echo "$KNOWN_COMMANDS" | cut -d' ' -f 3 | tr '\n' ' ')
    echo "All commands available are:"
    echo
    (
        for cmd in ${commands}; do
            doc_name=man_$(echo "$cmd" | tr - _)
            echo -e "  $cmd\t\t\t${!doc_name}"
        done
    ) | column -t -s$'\t'
    echo
    exit
}

man_lint="Run lint, formatting and type checks for Python code."
lint() {
    if [ "$#" -gt 0 ]; then
        echo "Too many args."
        exit
    fi

    poetry run flake8 --exclude "tests" pgmerge/
    # We do a separate pass with less stringent documentation requirements for tests
    poetry run flake8 --extend-ignore=D pgmerge/

    poetry run mypy --install-types --non-interactive --ignore-missing-imports --strict pgmerge/*.py
    # No strict type requirements for tests
    poetry run mypy --ignore-missing-imports pgmerge/tests
}

man_test="Run tests for Python code."
test() {
    if [ "$#" -gt 0 ]; then
        echo "Too many args."
        exit
    fi

    # We add "test" contexts to see how many tests cover each line of code. This helps to spot overlapping coverage
    # or a too-high "coverage density" which means that small changes to those parts of the code will require updating
    # and fixing many different tests.
    DB_TEST_URL=postgresql://postgres:postgres@localhost:5432/ SQLALCHEMY_WARN_20=1 poetry run pytest \
            --cov-report html --cov-report xml --cov pgmerge \
            --cov-branch --cov-context test --cov-fail-under=85 --verbose "${@:2}"
}

man_publish="Build and upload Python package."
publish() {
    # Make sure no local changes are distributed.
    git stash

    poetry build
    echo "Package contents"
    tar -tf $(ls -1t dist/*.tar.gz | head -n1)

    read -s -p "Password: " PASSWORD
    poetry publish -u __token__ -p $PASSWORD

    git stash pop
}

man_check_version="Check consistency of newest version in code and docs."
check-version() {
    PROJECT_VERSION=$(cat pyproject.toml | grep "^version = " | sed "s/^version =//" | tr -d '" ')
    CHANGE_VERSION=$(cat CHANGELOG.md | grep "## \[[[:digit:]]" | cut -d'-' -f1 | sed "s/## //" | tr -d "[] " | head -n1)
    CLI_VERSION=$(cat pgmerge/__init__.py | grep "__version__" | sed "s/__version__ = //" | tr -d "' ")
    # Run "test" from system command instead of local function
    $(which test) "$PROJECT_VERSION" = "$CHANGE_VERSION"
    $(which test) "$PROJECT_VERSION" = "$CLI_VERSION"
}


# Find all declared functions that are not from exports (-fx). This will only pick up functions before this point.
KNOWN_COMMANDS=$(declare -F | grep -v "\-fx")

# If column command is not available, create a no-op function to replace it and prevent errors.
# Alternatively, install it with: apt-get install -y bsdmainutils
if ! type column >/dev/null 2>&1
then function column { cat - ;}
fi

# Run function with same name of CLI argument (default to "help").
cmd=${1:-"help"}
if [ "$#" -gt 0 ]; then
    # Remove argument we've already used.
    shift
fi
$cmd "$@"
