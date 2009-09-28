##
## Commands available both in command line or in console
##
import os, os.path, ConfigParser

import utils
from staging import Staging
from options import VERBOSE, DRY_RUN

from utils import WrongNumberOfArgumentsException
from utils import UnknownSectionException
from utils import UnknownOptionException
from utils import StagingRuntimeException
from utils import UnknownCommandException

# cache
config = None

def get_option(config, section, option, optional=False):
    """ if [section] has no option, try in DEFAULT """
    if config.has_option(section, option):
        return config.get(section, option)

    if config.has_option('default', option):
        return config.get('default', option)

    if not optional:
        mesg = "Unable to read %s.%s configuration" % (section, option)
        raise UnknownOptionException, mesg
    return None

def parse_config(conffile, dbname, init_staging = True, force_reload = False):
    """ parse given INI file, and return it all if init_staging is False,
    return a staging object for dbname section otherwise."""

    if VERBOSE:
        print "Parsing configuration INI file '%s'" % conffile

    global config
    if force_reload or config is None:
        import sys
        config = ConfigParser.SafeConfigParser()

        try:
            config.read(conffile)
        except Exception, e:
            print >>sys.stderr, "Error: unable to read '%s'" % conffile
            print >>sys.stderr, e
            sys.exit(2)

    if not config.has_section(dbname):
        mesg = "Error: Please provide a [%s] section" % dbname
        raise UnknownSectionException, mesg

    if init_staging:
        # prepare the Staging shell object
        try:
            staging = Staging(dbname, # section
                              get_option(config, dbname, "backup_host"),
                              get_option(config, dbname, "backup_base_url"),
                              get_option(config, dbname, "dumpall_url", True),
                              get_option(config, dbname, "host"),
                              get_option(config, dbname, "dbname"),
                              get_option(config, dbname, "dbuser"),
                              get_option(config, dbname, "dbowner"),
                              get_option(config, dbname, "maintdb"),
                              get_option(config, dbname, "db_encoding"),
                              get_option(config, dbname, "postgres_port"),
                              get_option(config, dbname, "postgres_major"),
                              get_option(config, dbname, "pgbouncer_port"),
                              get_option(config, dbname, "pgbouncer_conf"),
                              get_option(config, dbname, "remove_dump"),
                              get_option(config, dbname, "keep_bases"),
                              get_option(config, dbname, "auto_switch"),
                              get_option(config, dbname, "use_sudo"),
                              get_option(config, dbname, "pg_restore"),
                              get_option(config, dbname, "pg_restore_st"),
                              get_option(config, dbname, "tmpdir", True))

            schemas = get_option(config, dbname, "schemas", True)
            if schemas:
                schemas = [s.strip() for s in schemas.split(',')]
            staging.schemas = schemas

            schemas_nodata = get_option(config, dbname, "schemas_nodata", True)
            if schemas_nodata:
                schemas_nodata = [s.strip() for s in schemas_nodata.split(',')]
            staging.schemas_nodata = schemas_nodata

            search_path = get_option(config, dbname, "search_path", True)
            if search_path:
                search_path = [s.strip() for s in search_path.split(',')]
            staging.search_path = search_path

            replication = get_option(config, dbname, "replication", True)
            if replication:
                try:
                    staging.replication = ConfigParser.SafeConfigParser()
                    staging.replication.read(replication)
                except Exception, e:
                    raise Exception, "Error: unable to read '%s'" % replication

            # Which tmpdir to use?
            # prefer -t command line option over .ini setup
            from options import TMPDIR, DEFAULT_TMPDIR
            if TMPDIR is not None:
                staging.tmpdir = TMPDIR

            if staging.tmpdir is None:
                staging.tmpdir = DEFAULT_TMPDIR

            # SQL PATH
            sql_path = None

            if config.has_option(dbname, "sql_path"):
                sql_path = config.get(dbname, "sql_path")
            
            if sql_path:
                # be nice, expand ~
                sql_path = os.path.join(os.path.expanduser(sql_path), dbname)

                if os.path.isdir(sql_path):
                    staging.sql_path = sql_path
            
        except Exception, e:
            print >>sys.stderr, "Configuration error: %s" % e
            raise
            sys.exit(4)

        return staging
    else:
        return config

def parse_args_for_dbname_and_date(args, usage):
    """ returns dbname, date or raise WrongNumberOfArgumentsException """
    if len(args) not in (1, 2):
        raise WrongNumberOfArgumentsException, usage

    dbname = args[0]
    date   = None

    if len(args) == 2:
        date = args[1]

    return dbname, date

def duration_pprint(duration):
    """ pretty print duration (human readable information) """
    if duration > 3600:
        h  = int(duration / 3600)
        m  = int((duration - 3600 * h) / 60)
        s  = duration - 3600 * h - 60 * m + 0.5
        return '%2dh%02dm%03.1f' % (h, m, s)
    
    elif duration > 60:
        m  = int(duration / 60)
        s  = duration - 60 * m
        return ' %02dm%06.3f' % (m, s)
        
    else:
        return '%6.3fs' % duration

##
## Facilities to run commands, including parsing when necessary
##
    
def run_command(conffile, command, args):
    """ run given pg_staging command """
    import sys
    from pgstaging.options import VERBOSE, DRY_RUN, DEBUG

    # and act accordinly
    if command not in exports:
        print >>sys.stderr, "Error: no command '%s'" % command
        raise UnknownCommandException
        
    try:
        exports[command](conffile, args)
    except Exception, e:
        print >>sys.stderr, e

        if DEBUG:
            raise
        raise StagingRuntimeException, e

def parse_input_line_and_run_command(conffile, line):
    """ parse input line """
    from options import DEBUG, COMMENT
    import shlex, sys
            
    try:
        if len(line) > 0 and line[0] != COMMENT:
            cli = shlex.split(line, COMMENT)
            run_command(conffile, cli[0], cli[1:])
            
    except Exception, e:
        raise

##
## From now on, pgstaging commands, shared by pg_staging.py and console.py
##

    
def init_cluster(conffile, args):
    """ <dbname> """
    usage = "init <dbname>"

    if len(args) != 1:
        raise WrongNumberOfArgumentsException, "init <dbname>"

    staging = parse_config(conffile, args[0])
    staging.init_cluster()

def dump(conffile, args, force = False):
    """ <dbname> dump a database """
    usage = "dump <dbname> [filename]"

    if len(args) not in [1, 2]:
        raise WrongNumberOfArgumentsException, usage

    dbname = args[0]
    
    if len(args) == 1:
        import datetime
        filename = '%s.%s.dump' % (dbname, datetime.date.today().isoformat())
    else:
        filename = args[1]

    # now load configuration and dump to filename
    staging = parse_config(conffile, dbname)
    pgdump_t = staging.dump(filename, force)

    if pgdump_t:
        print "   pg_dump:", duration_pprint(pgdump_t)

def redump(conffile, args):
    """ dump a database, overwriting the pre-existing dump file if it exists """
    dump(conffile, args, True)

def restore(conffile, args):
    """ <dbname> restore a database """
    usage = "restore <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    wget_t, pgrestore_t = staging.restore()

    print "  fetching:", duration_pprint(wget_t)
    print "pg_restore:", duration_pprint(pgrestore_t)

def restore_from_dump(conffile, args):
    """ <dbname> <dumpfile> """
    usage = "load <dbname> <dumpfile>"

    if len(args) != 2:
        raise WrongNumberOfArgumentsException, "load <dbname> <dumpfile>"
    
    staging = parse_config(conffile, args[0])
    secs = staging.load(args[1])

    print "load time:", duration_pprint(secs)

def fetch_dump(conffile, args):
    """ <dbname> [date] """
    usage = "fetch <dumpfile> <dbname>"

    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and fetch
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.get_dump()

    print "  timing", duration_pprint(staging.wget_timing)

def pre_source_extra_files(conffile, args):
    """ <dbname> [date] """
    usage = "presql <dbname> [date]"

    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and source extra sql files
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    for sql in staging.psql_source_files(utils.PRE_SQL):
        print "psql -f %s" % sql

def post_source_extra_files(conffile, args):
    """ <dbname> [date] """
    usage = "postsql <dbname> [date]"

    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and source extra sql files
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    for sql in staging.psql_source_files(utils.POST_SQL):
        print "psql -f %s" % sql

def list_databases(conffile, args):
    """ list configured databases """
    config = ConfigParser.SafeConfigParser()

    try:
        config.read(conffile)
    except Exception, e:
        print >>sys.stderr, "Error: unable to read '%s'" % conffile
        print >>sys.stderr, e
        sys.exit(2)

    for section in config.sections():
        print section

def list_backups(conffile, args):
    """ list available backups for a given database """
    if len(args) != 1:
        raise WrongNumberOfArgumentsException, "backups <dbname>"
    
    dbname = args[0]
    staging = parse_config(conffile, dbname)
    for backup, size in staging.list_backups():
        print "%6s %s " % (size, backup)

def psql_connect(conffile, args):
    """ launch a psql connection to the given configured section """
    usage = "psql <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    return staging.psql_connect()

def show_setting(conffile, args):
    """ show given database setting current value """
    usage = "show <dbname> [date] <setting>"

    if len(args) not in (2, 3):
        raise WrongNumberOfArgumentsException, usage

    if len(args) == 2:
        setting = args[1]
        dbname, backup_date = parse_args_for_dbname_and_date(args[0:1], usage)
        
    elif len(args) == 3:
        setting = args[2]
        dbname, backup_date = parse_args_for_dbname_and_date(args[0:2], usage)

    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    value = staging.show(setting)
    print "%25s: %s" % (setting, value)

def show_dbsize(conffile, args):
    """ show given database size """
    usage = "dbsize <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    name, size, pretty = staging.dbsize()
    print "%25s: %s" % (name, pretty)

def show_all_dbsizes(conffile, args):
    """ show dbsize for all databases of a dbname section """
    usage = "dbsizes <dbname> [<dbname> ...]"
    if len(args) < 1:
        raise WrongNumberOfArgumentsException, usage

    total_size = 0

    if args[0] in ('--all', '--match'):
        config = ConfigParser.SafeConfigParser()

        try:
            config.read(conffile)
        except Exception, e:
            print >>sys.stderr, "Error: unable to read '%s'" % conffile
            print >>sys.stderr, e
            sys.exit(2)

    if args[0] == '--all':
        args = config.sections()

    elif args[0] == '--match':
        if len(args) != 2:
            raise WrongNumberOfArgumentsException, usage

        import re
        regexp = re.compile(args[1])

        args = [x for x in config.sections() if regexp.search(x)]
    
    for db in args:
        # now load configuration and restore
        staging = parse_config(conffile, db)

        print db
        
        for d, s, p in staging.dbsizes():
            if s > 0:
                total_size += s
                
            print "%25s: %s" % (d, p)

    print "%-25s= %s" % ('Total', staging.pg_size_pretty(total_size))

def list_pgbouncer_databases(conffile, args):
    """ list configured pgbouncer databases """
    usage = "pgbouncer <dbname>"
    if len(args) != 1:
        raise WrongNumberOfArgumentsException, usage

    dbname = args[0]
    staging = parse_config(conffile, dbname)
    for name, database, host, port in staging.pgbouncer_databases():
        print "%25s %25s %s:%s" % (name, database, host, port)

def pause_pgbouncer_database(conffile, args):
    """ pause <dbname> [date] (when no date given, not expanded to today) """
    usage = "pause <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)

    if backup_date is None:
        dbname = staging.dbname
    else:
        staging.set_backup_date(backup_date)
        dbname = staging.dated_dbname

    staging.pgbouncer_pause(dbname)

def resume_pgbouncer_database(conffile, args):
    """ resume <dbname> [date] (when no date given, not expanded to today) """
    usage = "resume <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)

    if backup_date is None:
        dbname = staging.dbname
    else:
        staging.set_backup_date(backup_date)
        dbname = staging.dated_dbname
        
    staging.pgbouncer_resume(dbname)
    
def switch(conffile, args):
    """ <dbname> <bdate> switch default pgbouncer config to dbname_bdate """
    usage = "switch <dbname> [date]"    
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.switch()

def drop(conffile, args):
    """ <dbname> drop given database """
    usage = "drop <dbname> [date]"    
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.drop()

def catalog(conffile, args):
    """ <dbname> [dump] print catalog for dbname, edited for nodata tables """
    # experimental devel facility, not too much error handling
    usage = "catalog <dbname> [dump]"

    dbname   = args[0]
    filename = None
    staging = parse_config(conffile, dbname)

    if len(args) > 1:
        filename = args[1]

    if filename is None or not os.path.exists(filename):
        staging.set_backup_date(None)
        filename = staging.get_dump()
        print filename

    print staging.get_catalog(filename)

def triggers(conffile, args):
    """ <dbname> [dump] print triggers procedures for dbname """
    # experimental devel facility, not too much error handling
    usage = "triggers <dbname> [dump]"

    dbname   = args[0]
    filename = None
    staging = parse_config(conffile, dbname)

    if len(args) > 1:
        filename = args[1]

    if filename is None or not os.path.exists(filename):
        staging.set_backup_date(None)
        filename = staging.get_dump()
        print filename

    triggers = staging.get_triggers(filename)

    print "%15s %-25s %s" % ('SCHEMA', 'TRIGGER', 'FUNCTION')
    for s in triggers:
        for t in triggers[s]:
            for f in triggers[s][t]:
                print "%15s %-25s %s" % (s, t, f)

def set_database_search_path(conffile, args):
    """ alter database <dbname> set search_path """
    # experimental only
    usage = "search_path <dbname> [date]"    
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.set_database_search_path()

def list_nodata_tables(conffile, args):
    """ list tables to restore without their data """
    usage = "nodata <dbname> [date]"    
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    for t in staging.get_nodata_tables():
        print '%s' % t

def prepare_then_run_londiste(conffile, args):
    """ prepare londiste files for providers of given dbname section """
    usage = "londiste <dbname> [date]"    
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    for provider, filename in staging.prepare_then_run_londiste():
        print "%25s: %s" % (provider, os.path.basename(filename))

def control_service(conffile, service, action, usage, args):
    """ internal stuff """
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.control_service(service, action)

def service_restart(conffile, args):
    """ restart given service depending on its configuration and special args """
    usage = "restart <service> <dbname> [date]"

    if len(args) < 2:
        raise WrongNumberOfArgumentsException, usage

    control_service(conffile, args[0], 'restart', usage, args[1:])

def service_stop(conffile, args):
    """ stop given service depending on its configuration and special args """
    usage = "stop <service> <dbname> [date]"

    if len(args) < 2:
        raise WrongNumberOfArgumentsException, usage

    control_service(conffile, args[0], 'stop', usage, args[1:])

def service_start(conffile, args):
    """ start given service depending on its configuration and special args """
    usage = "start <service> <dbname> [date]"

    if len(args) < 2:
        raise WrongNumberOfArgumentsException, usage

    control_service(conffile, args[0], 'start', usage, args[1:])

def service_status(conffile, args):
    """ show status of given service ..."""
    usage = "status <service> <dbname> [date]"

    if len(args) < 2:
        raise WrongNumberOfArgumentsException, usage

    control_service(conffile, args[0], 'status', usage, args[1:])

def get_config_option(conffile, args):
    """ <dbname> <option> print the current value of [dbname] option """
    if len(args) != 2:
        raise WrongNumberOfArgumentsException, "get dbname option"

    dbname, option = args[0], args[1]
    config = parse_config(conffile, dbname, init_staging = False)

    print config.get(dbname, option)

def set_config_option(conffile, args):
    """ <dbname> <option> <value> for current session only """

    if len(args) < 3:
        raise WrongNumberOfArgumentsException, "set dbname option value"

    dbname, option, value = args[0], args[1], ' '.join(args[2:])
    config = parse_config(conffile, dbname, init_staging = False)

    if not config.has_option(dbname, option):
        print "Error: inexistant option '%s'" % option
        return

    config.set(dbname, option, value)
    print config.get(dbname, option)

def list_commands(conffile, args):
    """ provide a user friendly listing of commands """

    for fn in exports:
        print "%10s %s" % (fn, exports[fn].__doc__.strip())

##
## dynamic programming, let's save typing
##
exports = {
    # nice help
    "commands":  list_commands,

    # main operation
    "init":      init_cluster,
    "dump":      dump,
    "redump":    redump,
    "restore":   restore,
    "drop":      drop,
    "switch":    switch,
    "load":      restore_from_dump,
    "fetch":     fetch_dump,
    "presql":    pre_source_extra_files,
    "postsql":   post_source_extra_files,

    # listing
    "databases":   list_databases,
    "backups":     list_backups,
    "dbsize":      show_dbsize,
    "dbsizes":     show_all_dbsizes,
    "psql":        psql_connect,
    "show":        show_setting,
    "search_path": set_database_search_path,

    # pgbouncer
    "pgbouncer": list_pgbouncer_databases,
    "pause":     pause_pgbouncer_database,
    "resume":    resume_pgbouncer_database,

    # londiste
    "londiste": prepare_then_run_londiste,
    "restart":  service_restart,
    "stop":     service_stop,
    "start":    service_start,
    "status":   service_status,

    # configuration file
    "get":       get_config_option,
    "set":       set_config_option,

    # internal subcommands commands used to add features
    "nodata":      list_nodata_tables,
    "catalog":     catalog,
    "triggers":    triggers,
    }
