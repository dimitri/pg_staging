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

function service() {
    action=$1
    service=$2

    case $service in
	"pgbouncer")
	    port=$3
	    case $action in
		"restart")
	            # arg is pgbouncer port number
		    psql -c "shutdown;" -U postgres -p $port pgbouncer 2>/dev/null
		    /etc/init.d/pgbouncer start
		    ;;

		"stop")
		    psql -c "shutdown;" -U postgres -p $port pgbouncer 2>/dev/null
		    ;;

		"start")
		    /etc/init.d/pgbouncer start
		    ;;

		"status")
		    psql -c "show pools;" -U postgres -p $port pgbouncer
		    ;;

		*)
		    echo "Don't know how to $action $service" >&2
		    exit 1
		    ;;
	    esac
	    ;;

	"londiste")
	    provider=$3
	    ini=$4
	    case $action in
		"start")
		    londiste.py ~/londiste/$provider/$ini replay -d

		"stop")
		    # arg is londiste configuration file
		    londiste.py ~/londiste/$provider/$ini -s
		    sleep 3
		    londiste.py ~/londiste/$provider/$ini -k
		    ;;

		"status")
		    londiste.py ~/londiste/$provider/$ini subscriber tables
		    ;;

		*)
		    echo "Don't know how to $action $service" >&2
		    exit 1
		    ;;
	    esac
	    ;;

	"ticker")
	    ini=$3
	    case $action in
		"start")
		    pgqadm.py ~/londiste/$ini ticker -d
		    ;;

		"stop")
		    pgqadm.py ~/londiste/$ini -s
		    sleep 3
		    pgqadm.py ~/londiste/$ini -k
		    ;;

		"status")
		    pgqadm.py ~/londiste/$ini status
		    ;;

		*)
		    echo "Don't know how to $action $service" >&2
		    exit 1
		    ;;
	    esac
	    ;;

	*)
	    echo "Unknown service to restart: $service" >&2
	    exit 1
	    ;;
    esac
}

function init_londiste() {
    # $1 provider
    # $2 ini file
    # $3 .. $N list of schema-qualified table names to add to replication

    provider=$1
    ini=$2
    shift; shift; # now $* is the list of tables to consider

    # replication setups go into /home/pgstaging/londiste
    mkdir -p ~/londiste/$provider
    mv /tmp/$ini ~/londiste/$provider/

    londiste.py ~/londiste/$provider/$ini provider install
    londiste.py ~/londiste/$provider/$ini subscriber install

    londiste.py ~/londiste/$provider/$ini provider add $*
    londiste.py ~/londiste/$provider/$ini subscriber add $*
}

function replay() {
    # $1 provider
    # $2 ini file
    londiste.py ~/londiste/$1/$2 replay -d
}

function init_pgq() {
    # $1 ini file
    mkdir -p ~/londiste
    mv /tmp/$1 ~/londiste

    pgqadm.py ~/londiste/$1 install
}

function ticker() {
    pgqadm.py ~/londiste/$1 ticker -d
}

command=$1
shift

case $command in
    "pgbouncer")
	pgbouncer $*;;

    "start")
	service 'start' $*;;

    "stop")
	service 'stop' $*;;

    "restart")
	service 'restart' $*
	if [ $? -eq 1 ]; then
	    service 'stop' $* && service start $*
	fi
	;;

    "status")
	service 'status' $*;;

    "init-londiste")
	init_londiste $*;;

    "replay")
	replay $*;;

    "init-pgq")
	init_pgq $*;;

    "ticker")
	ticker $*;;

    *)
	echo "unsupported command" >&2
	exit 1
	;;
esac
