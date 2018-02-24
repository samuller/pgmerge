# Questions & Answers

## Can I use `pgmerge` on an empty database to initialise it with data?

Only if the database's schema has already been set up. `pgmerge` expects that the database with all its tables already exists and matches the same format as when the files were exported.

## Should I use `pgmerge` to import data into a newly created database?

It depends. If you want the fastest possible way to import lots of data, then *no*. Consider using the built-in `pg_restore` utility or manually using the `COPY` command. These tools are more mature and should be very reliable. A good guide is available in the [PostgreSQL documentation][1].

If you want a simple method that'll be the same when importing your initial data and any future updates, then `pgmerge` could work well.

[1]: https://www.postgresql.org/docs/current/static/populate.html

## Can `pgmerge` export the schema for my database/tables?

No, to get your database schema you can use `pg_dump` which comes installed with Postgres:

    pg_dump -s database_name > schema.sql

This will give you all the commands necessary to fully create your database schema.

If you only want to see the schema for individual tables you can use `pgAdmin3` which will shows the schema whenever a table is selected. There's also the describe command of `psql` which can be used:

    \d table_name

## Can `pgmerge` import data while the database currently in-use?

No, if there are other active connections to the database importing will likely fail. The current implementation might require obtaining exclusive locks on the database tables.

