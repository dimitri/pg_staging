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
    # $1 provider
    # $2 ini file
    # $3 .. $N list of schema-qualified table names to add to replication

    # replication setups go into /home/pgstaging/londiste
    mkdir -p ~/londiste/$1
    mv /tmp/$2 ~/londiste/$1/

    echo "londiste.py ~/londiste/$1/$2 provider install"
    echo "londiste.py ~/londiste/$1/$2 subscriber install"
    echo "londiste.py ~/londiste/$1/$2 provider add $3 ..."
    echo "londiste.py ~/londiste/$1/$2 subscriber add $3 ..."
}

function run_londiste() {
    echo "londiste.py ~/londiste/$1 replay -d"
}

function init_pgq() {
    # $1 ini file
    mkdir -p ~/londiste
    mv /tmp/$1 ~/londiste

    echo "pgqadm.py ~/londiste/$1 install"
}

function ticker() {
    echo "pgqadm.py ~/londiste/$1 ticker -d"
}

command=$1
shift

case $command in
    "pgbouncer")
	pgbouncer $*;;

    "init-londiste")
	init_londiste $*;;

    "run-londiste")
	run_londiste $*;;

    "init-pgq")
	init_pgq $*;;

    "ticker")
	ticker $*;;

    *)
	echo "unsupported command";;
esac
