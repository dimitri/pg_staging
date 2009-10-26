pg_staging
==========

Setup and maintain your staging environments from your production backups!

Introduction
------------

`pg_staging` implements commands for playing with your `PostgreSQL_
<http://www.postgresql.org/docs/current/interactive/backup.html>` backups,
allowing you to expose in devel or prelive environments more than one copy
of a live database at the same time.

This document is formatted in a `github_
<http://github.com/dimitri/pg_staging>` supported format, unfortunately
`asciidoc` is not one of them. So all documents are in *asciidoc* format
except for this `README`.

backing up, restoring
~~~~~~~~~~~~~~~~~~~~~

`pg_staging` is all about leaveraging your backups procedures, and propose
commands allowing to restore those into environments developpers are using
to validate their code.

switching between multiple copies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Thanks to `pgbouncer` you can setup things so that `psql -h devel mydb` will
connect to whatever you restored last, which is called `mydb_YYYYMMDD`,
after the date the backup was taken. `pg_staging` supports switching

Usage
-----

Please refer to the manual for details.

You have 3 ways to use the pg_staging commands:

 - interactive console

   pg_staging.py
 
 - command line interface

   pg_staging.py command arg1 arg2
 
 - scripting

   (echo 'arg1'; echo 'arg2') | pg_staging

All those interfaces expose the exact same set of commands, use the one best
fitting your usage environment.

Please note that the scripting `.pgs` support is pretty limited currently,
it lets you `get` and `set` configuration and run the usual commands.

Setup
-----

The setup is to be handled in `.ini` files, where you describe your target
environment in terms of the production one. Basically you tell `pg_staging`
where to find your backups and restore them.
