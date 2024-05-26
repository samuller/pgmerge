# Questions & Answers

## Can I use pgmerge on an empty database to initialise it with data?

Only if the database's schema has already been set up. pgmerge expects that the database with all its tables already exists and matches the same format as when the files were exported.

## Should I use pgmerge to import data into a newly created database?

It depends. If you want the fastest possible way to import lots of data, then *no*. Consider using the built-in `pg_restore` utility or manually using the `COPY` command. These tools are more mature and should be very reliable. A good guide is available in the [PostgreSQL documentation][1].

If you want a simple method that'll work the same for importing your initial data and any future updates to the data, then pgmerge could work well.

[1]: https://www.postgresql.org/docs/current/static/populate.html

## What alternatives are there to pgmerge?

There are many tools for PostgreSQL that provide import/export functionality. The best tool depends on what your requirements are. Some alternatives are:

* **pg_dump** / **pg_restore**: built-in tools for directly importing/exporting full tables to an SQL or binary format
* Postgres' **COPY** command: built-in SQL command for importing/exporting any table or query data to various different text, CSV or binary formats
* **pg_bulkload**: external tool that provides similar import functionality as COPY, but with performance improvements, increased configurability and data validation and transformation capabilities
* **pgloader**: external tool that uses COPY to do bulk imports, but with better error handling
* **pgfutter** and **pgclimb**: two tools by the same author, one that imports CSV and line delimited JSON into a database using COPY and will create the tables for you, the other exports into various formats that are either standard (JSON, CSV, XML) or can be fully custom through the use of templates
* **pgmerge**: external tool that uses COPY to import/export full tables or table subsets in CSV format, but on import also does merging using primary key or other unique columns

## Can pgmerge export the schema for my database/tables?

No, to get your database schema you can use `pg_dump` which comes installed with PostgreSQL:

    pg_dump -s database_name > schema.sql

This will give you all the commands necessary to fully create your database schema.

If you only want to see the schema for individual tables you can use `pgAdmin3` which will shows the schema whenever a table is selected. There's also the describe command of `psql` which can be used:

    \d table_name

## Can pgmerge import data while the database is currently in-use?

No, if there are other active connections to the database the importing process will likely fail. The current implementation might require obtaining exclusive locks on the database tables.

## Can pgmerge import data with a custom format or that was created manually?

Not currently, no. While it is easy to create CSV files such that they'll import correctly, it is dangerous as any mistakes can cause your database to contain data in an invalid state. This is because pgmerge might temporarily disable database checks that enforce consistency during import (currently this occurs when using the `--disable-foreign-keys` option). It is currently an assumption that the data provided is completely valid.

## Can pgmerge corrupt data in my database?

Yes, especially when `--disable-foreign-keys` is used as the option disables various database consistency checks during the import process. The import process always assumes the input data is completely valid.

