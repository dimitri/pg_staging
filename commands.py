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

def restore(conffile, args):
    """ restore a database """

    if len(args) not in (1, 2):
        raise WrongNumberOfArgumentsException, \
              "restore <database> [date]"

    dbname = args[0]
    backup_date = None

    if len(args) == 2:
        backup_date = args[1]

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

def switch(args):
    pass

def drop(args):
    pass

def set_option(config, args):
    """ set an option in given section of the config file """
    pass
