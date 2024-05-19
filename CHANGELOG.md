# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Fix `--single-table` option to also work when no config is provided.
- Detect invalid config that has duplicate subset names within one table's configs.

### Changed

- Table config schema has been changed to use standard JSON schema.
  - Table config's format remains unchanged and the format of the schema file is still YAML.
  - Dependency `rxjson` has been replaced with `fastjsonschema` (version 2.19.1).
  - Details about validation failure are now included in reported error messages.

## [1.10.1] - 2024-05-17

### Fixed

- Fix reported version when running `--version`.

## [1.10.0] - 2024-05-16

### Changed

* Upsert operations more consistenly maintains ordering of newly inserted rows in PostgreSQL 16+.
* Move to Poetry for build and dependency management.

### Removed

* Support for Python 3.6 & Ubuntu 18.04.

### Added

* Changelog.
* Mypy type checking & CI improvements.

## [1.9.1] - 2021-12-11

### Changed

* Make changes to partially migrate and prepare for new APIs in SQLAlchemy 2.0.
* Return exit code 3 when exceptions cause execution to fail and exit code 4 when there are unexpected data issues.

### Added

* Add extra tests, code coverage and CI checks.

## [1.9.0] - 2021-12-05

### Added

* Add `--single-table` option for imports that assumes all files in a folder are of the same type and should be imported into the same table.
* Many extra linting & typing checks and improvements.

### Changed

* Update depdencies: `networkX` from 1.11 to 2.5, `PyYAML` from 5.1.1 to 6.0, `SQLAlchemy` from 1.3.5 to 1.4.27 & `appdirs` from 1.4.3 to 1.4.4.
* Convert command-line handling from `click` (6.7 to 7.1.2) to `typer` 0.4.0.
* Improve performance during and after imports with `ANALYZE` queries.
* Always import data in same order by sorting import files and tables order in dependency graph.
* Convert to Poetry build system.

## [1.8.0] - 2019-07-23

## [1.7.0] - 2018-04-24

## [1.6.2] - 2018-04-23

## [1.6.1] - 2018-04-23

## [1.6.0] - 2018-04-21

## [1.5.0] - 2018-04-18

## [1.1.0] - 2018-02-26

Initial release to PyPI.
