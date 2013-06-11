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

    psql -c "reload;" -U postgres -p $2 pgbouncer > /dev/null 2>&1 
}

function dropdb() {
    dbname=$1
    pgbouncer_port=$2
    postgres_port=$3
    postgres_major=$4
    outcode=0

    # Stop PgBouncer 
    service stop pgbouncer $pgbouncer_port

    # Cancel Backend 
    if [ $(echo "$postgres_major >= 9.2" | bc) -eq 1 ]
    then
 	    echo "version >= 9.2"
	    psql -U postgres -p $postgres_port -c "select pg_cancel_backend( pid ) from pg_stat_activity where datname='${dbname}'" > /dev/null 2>&1
    else
            echo "version < 9.2"
            psql -U postgres -p $postgres_port -c "select pg_cancel_backend( procpid ) from pg_stat_activity where datname='${dbname}'" > /dev/null 2>&1
    fi

    if [ $? -ne 0 ]; then 
	outcode=1 
    fi

    nbsession=`psql -U postgres -p $postgres_port -At -c "select count( * ) from pg_stat_activity where datname='${dbname}'"`
    if [ "$outcode" -ne 0 ] || [ "$nbsession" -ne 0 ]; then
	service stop postgresql
	sleep 2
	service start postgresql
        sleep 5
    fi 

    # Drop Database 
    psql -U postgres -p $postgres_port -c "drop database $dbname ; "  > /dev/null 2>&1

    if [ $? -ne 0 ]; then 
	outcode=2
    fi

    # Start Pgbouncer 
    service start pgbouncer > /dev/null 2>&1

return $outcode 
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
		    $(exit 1)
		    ;;
	    esac
	    ;;

	"londiste")
	    provider=$3
	    ini=$4
	    case $action in
		"start")
		    londiste.py ~/londiste/$provider/$ini replay -d
		    ;;

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
		    $(exit 1)
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
		    $(exit 1)
		    ;;
	    esac
	    ;;

	"postgresql")
	    OIFS=$IFS
	    IFS="$(echo -e "\n\r")"
	    for cluster in `pg_lsclusters -h|awk '{print $1 " " $2}'` 
	    do
		IFS=" " read -r version name <<< "$cluster" 
		if [ "$action" = "stop" ]; then
		    switch="--force"
		else
		    switch=""
		fi
		/usr/bin/pg_ctlcluster $version $name $action $switch
	    done
	    IFS=$OIFS
	    ;;

	*)
	    echo "Unknown service to restart: $service" >&2
	    $(exit 3)
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

function pitr() {
    # args are:
    # $1 local cluster directory 
    # $2 base backup copy command
    # $3 wal archive copy command
    # $4 type of recovery target, optional (either time or xid)
    # $5 value of the recovery target, optional unless $4 is given

    if [ $# -ne 3 -a $# -ne 5 ]; then
	$(exit 1)
    fi

    # get an initial copy of the base backup
    if test -d "$1".orig; then
	echo "Initial copy has already been made, skipping"
    else
	mkdir -p "$1".orig || $(exit 2)
	cd "$1".orig       || $(exit 2)
	`$2`               || $(exit 2)
	cd -
    fi

    # copy the base backup to where we start the cluster from
    mkdir -p "$1"             || $(exit 3)
    rm -rf "$1"/*             || $(exit 3)
    rsync -a "$1".orig/ $1/   || $(exit 3)

    # get a copy of the archived WALs
    mkdir -p /var/tmp/wals    || $(exit 4)
    cd /var/tmp/wals          || $(exit 4)
    `$3`                      || $(exit 4)

    # create recovery.conf file in $1
    cat  > "$1"/recovery.conf <<EOF
# restore.conf generated by pg_staging client script
restore_command = 'cp /var/tmp/wals/%f "%p"'
log_restartpoints = true
EOF

    if [ $# -eq 5 ]; then
	echo "recovery_target_$4 $5" >> "$1"/recovery.conf
    fi

    # start the cluster, ATTENTION: port? existing clusters?
    # maybe we're missing some options there
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

    "dropdb")
	dropdb $*;;

    *)
	echo "unsupported command" >&2
	exit 1
	;;
esac
