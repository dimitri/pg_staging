##
## Commands available both in command line or in console
##
import ConfigParser
from staging import Staging
from options import VERBOSE, DRY_RUN
from options import WrongNumberOfArgumentsException
from options import UnknownSectionException
from options import UnknownOptionException

# cache
config = None

def get_option(config, section, option, optional=False):
    """ if [section] has no option, try in DEFAULT """
    if config.has_option(section, option):
        return config.get(section, option)

    if config.has_option('default', option):
        return config.get(section, 'default')

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
                              get_option(config, dbname, "pg_restore_st"))

            schemas = get_option(config, dbname, "schemas", True)
            if schemas:
                schemas = [s.strip() for s in schemas.split(',')]
            staging.schemas = schemas

            schemas_nodata = get_option(config, dbname, "schemas_nodata", True)
            if schemas_nodata:
                schemas_nodata = [s.strip() for s in schemas_nodata.split(',')]
            staging.schemas_nodata = schemas_nodata

            replication = get_option(config, dbname, "replication", True)
            if replication:
                try:
                    staging.replication = ConfigParser.SafeConfigParser()
                    staging.replication.read(replication)
                except Exception, e:
                    raise Exception, "Error: unable to read '%s'" % replication
            
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

def init_cluster(conffile, args):
    """ <dbname> """
    usage = "init <dbname>"

    if len(args) != 1:
        raise WrongNumberOfArgumentsException, "init <dbname>"

    staging = parse_config(conffile, args[0])
    staging.init_cluster()

def restore(conffile, args):
    """ <dbname> restore a database """
    usage = "restore <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.restore()

def restore_from_dump(conffile, args):
    """ <dbname> <dumpfile> """
    usage = "load <dbname> <dumpfile>"

    if len(args) != 2:
        raise WrongNumberOfArgumentsException, "load <dbname> <dumpfile>"
    
    staging = parse_config(conffile, args[0])
    staging.load(args[1])

def fetch_dump(conffile, args):
    """ <dbname> [date] """
    usage = "fetch <dumpfile> <dbname>"

    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and fetch
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.get_dump()

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

def show_dbsize(conffile, args):
    """ show given database size """
    usage = "dbsize <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    print "%25s: %s" % staging.dbsize()

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

def list_nodata_tables(conffile, args):
    """ list tables to restore without their data """
    # experimental only
    usage = "nodata <dbname> [date]"    
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    for t in staging.get_nodata_tables():
        print t

def catalog(conffile, args):
    """ <dbname> [dump] print catalog for dbname, edited for nodata tables """
    # experimental devel facility, not too much error handling
    usage = "catalog <dbname> [dump]"

    dbname   = args[0]
    filename = None
    staging = parse_config(conffile, dbname)

    if len(args) > 1:
        filename = args[1]

    import os.path
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

    import os.path
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
    "restore":   restore,
    "drop":      drop,
    "switch":    switch,
    "load":      restore_from_dump,
    "fetch":     fetch_dump,

    # listing
    "databases": list_databases,
    "backups":   list_backups,
    "dbsize":    show_dbsize,

    # pgbouncer
    "pgbouncer": list_pgbouncer_databases,
    "pause":     pause_pgbouncer_database,
    "resume":    resume_pgbouncer_database,

    # experimental commands used to add features
    "nodata":    list_nodata_tables,
    "catalog":   catalog,
    "triggers":  triggers
    }
