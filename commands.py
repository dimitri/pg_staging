##
## Commands available both in command line or in console
##
import ConfigParser
from staging import Staging
from options import VERBOSE, DRY_RUN
from options import WrongNumberOfArgumentsException
from options import UnknownSectionException

# cache
config = None

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
                              config.get(dbname, "backup_host"),
                              config.get(dbname, "backup_base_url"),
                              config.get(dbname, "host"),
                              config.get(dbname, "dbname"),
                              config.get(dbname, "dbuser"),
                              config.get(dbname, "dbowner"),
                              config.get(dbname, "maintdb"),
                              config.get(dbname, "postgres_port"),
                              config.get(dbname, "pgbouncer_port"),
                              config.get(dbname, "pgbouncer_conf"),
                              config.get(dbname, "pgbouncer_rcmd"),
                              config.get(dbname, "remove_dump"),
                              config.get(dbname, "keep_bases"),
                              config.get(dbname, "auto_switch"),
                              config.get(dbname, "use_sudo"))
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

def restore(conffile, args):
    """ <dbname> restore a database """
    usage = "restore <dbname> [date]"
    dbname, backup_date = parse_args_for_dbname_and_date(args, usage)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.restore()

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
    for backup in staging.list_backups():
        # only print the date of the backup, leave out the database name and
        # the .dump extension
        try:
            n, d, e = backup.split('.')
            if n == dbname and e == 'dump':
                print d
            else:
                raise ValueError
        except Exception, e:
            print backup

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
        print "%15s %s" % (fn, exports[fn].__doc__.strip())

##
## dynamic programming, let's save typing
##
exports = {
    "commands":  list_commands,
    "restore":   restore,
    "drop":      drop,
    "switch":    switch,
    "databases": list_databases,
    "backups":   list_backups,
    "get":       get_config_option,
    "set":       set_config_option
    }
