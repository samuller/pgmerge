# Config files

There are two types of configuration files used by pgmerge:

* Password configs: allow you to set database passwords so they don't need to be entered manually.
    * The only password configuration currently supported is PostgreSQL's [.pgpass file format][1]. If this file is found in the standard locations, it will automatically be loaded. It will also be loaded if it's found in pgmerge's own config directory (standard OS-dependent location for per-user app configs).
* Table import/export configuration: allow you to customize how tables are imported and exported. See [schema with comments](../pgmerge/tables_config_schema.yml).

[1]: https://www.postgresql.org/docs/9.3/static/libpq-pgpass.html
