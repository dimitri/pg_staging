#! /bin/bash
#
# client script for pg_staging.py, to be installed on target machines
# must run as root
#
PGBOUNCER_BASEDIR=/etc/pgbouncer

if [ $EUID -ne 0 ]; then
    echo "Must be run as root." >&2
    exit 1
fi

if [ ! -r $1 ]; then
    echo "$0: unable to read new pgbouncer.ini at $1" >&2
    exit 2
fi

ini=$1
port=$2
filename=`basename $1`

mv $1 $PGBOUNCER_BASEDIR/$filename
chmod a+r $PGBOUNCER_BASEDIR/$filename
(cd $PGBOUNCER_BASEDIR && ln -sf $filename pgbouncer.ini)

psql -c "reload;" -U postgres -p $2 pgbouncer
