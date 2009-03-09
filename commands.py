##
## Commands available both in command line or in console
##
import ConfigParser
from staging import Staging
from options import VERBOSE, DRY_RUN
from options import WrongNumberOfArgumentsException

def parse_config(conffile, dbname):
    """ parse given INI file, and return it all """

    if VERBOSE:
        print "Parsing configuration INI file '%s'" % conffile

    config = ConfigParser.SafeConfigParser()

    try:
        config.read(conffile)
    except Exception, e:
        print >>sys.stderr, "Error: unable to read '%s'" % conffile
        print >>sys.stderr, e
        sys.exit(2)

    if not config.has_section(dbname):
        print >>sys.stderr, "Error: Please provide a [%s] section" % dbname
        sys.exit(3)

    # prepare the Staging shell object
    try:
        staging = Staging(dbname,
                          config.get(dbname, "backup_host"),
                          config.get(dbname, "backup_base_url"),
                          config.get(dbname, "host"),
                          config.get(dbname, "dbuser"),
                          config.get(dbname, "dbowner"),
                          config.get(dbname, "postgres_port"),
                          config.get(dbname, "pgbouncer_port"),
                          config.get(dbname, "pgbouncer_conf"),
                          config.get(dbname, "pgbouncer_rcmd"),
                          config.get(dbname, "keep_bases"),
                          config.get(dbname, "auto_switch"),
                          config.get(dbname, "use_sudo"))
    except Exception, e:
        print >>sys.stderr, "Configuration error: %s" % e
        raise
        sys.exit(4)

    return staging

def parse_args_for_dbname_and_date(args):
    """ returns dbname, date or raise WrongNumberOfArgumentsException """
    if len(args) not in (1, 2):
        raise WrongNumberOfArgumentsException, \
              "restore <database> [date]"

    dbname = args[0]
    date   = None

    if len(args) == 2:
        date = args[1]

    return dbname, date

def restore(conffile, args):
    """ restore a database """
    dbname, backup_date = parse_args_for_dbname_and_date(args)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.restore()

def list_databases(conffile, args):
    """ List configured databases """
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

def switch(args):
    pass

def drop(args):
    """ drop given database """
    dbname, backup_date = parse_args_for_dbname_and_date(args)

    # now load configuration and restore
    staging = parse_config(conffile, dbname)
    staging.set_backup_date(backup_date)
    staging.drop()

def set_option(config, args):
    """ set an option in given section of the config file """
    pass
