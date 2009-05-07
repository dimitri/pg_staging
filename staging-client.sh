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

function pgbouncer() {
    if [ ! -f $1 ]; then
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
}

function init_londiste() {
    # $1 ini file
    # $2 .. $N list of schema-qualified table names to add to replication
    echo 'init londiste $1'
    echo 'provider add'
    echo 'subscriber add'
}

function run_londiste() {
    echo "londiste.py $1 replay -d"
}

function init_pgq() {
    echo 'init pgq $1'
}

function ticker() {
    echo "pgqadm.py $1 ticker -d"
}

case $1 in
    "pgbouncer")
	shift
	pgbouncer $*;;

    "init-londiste")
	shift
	init_londiste $*;;

    "run-londiste")
	shift
	run_londiste $*;;

    "init-pgq")
	shift
	init_pgq $*;;

    "ticker")
	shift
	ticker $*;;

    *)
	echo "unsupported command";;
esac
