#!/usr/bin/env python
#
# pg_staging allows to
#
import os, os.path, sys, ConfigParser
from optparse import OptionParser

import options

def parse_options():
    """ get the command to run from command line """

    usage  = "%prog [-c <config_filename>] command dbname [date]"
    parser = OptionParser(usage = usage)
    
    parser.add_option("-c", "--config", dest = "config",
                      default = options.DEFAULT_CONFIG_FILE,
                      help    = "configuration file, defauts to %s" \
                      % options.DEFAULT_CONFIG_FILE)

    parser.add_option("-n", "--dry-run", action = "store_true",
                      dest    = "dry_run",
                      default = False,
                      help    = "simulate operations, don't do them")

    parser.add_option("-v", "--verbose", action = "store_true",
                      dest    = "verbose",
                      default = False,
                      help    = "be verbose and about processing progress")

    (opts, args) = parser.parse_args()

    options.VERBOSE = opts.verbose
    options.DRY_RUN = opts.dry_run

    if options.VERBOSE:
        print "We'll be verbose!"

    # if there's not enough arguments we know we can stop here
    if len(args) < 2:
        print >>sys.stderr, "Error: not enough arguments"
        print >>sys.stderr, parser.format_help()
        sys.exit(-1)

    # check existence and read ability of config file
    if options.VERBOSE:
        print "Checking if config file '%s' exists" % opts.config

    if not os.path.exists(opts.config):
        print >>sys.stderr, \
              "Error: Configuration file %s does not exists" % opts.config
        print >>sys.stderr, parser.format_help()
        sys.exit(1)

    if options.VERBOSE:
        print "Checking if config file '%s' is readable" % opts.config

    if not os.access(opts.config, os.R_OK):
        print >>sys.stderr, \
              "Error: Can't read configuration file %s" % opts.config
        print >>sys.stderr, parser.format_help()
        sys.exit(1)

    command = args[0]
    dbname  = args[1]

    if len(args) >= 3:
        backup_date = args[2]
    else:
        import datetime
        backup_date = datetime.date.today().isoformat()

    if options.VERBOSE and len(args) > 3:
        print "Warning: garbage arguments will get ignored"
        print "         %s" % " ".join(args[3:])

    return opts.config, command, dbname, backup_date

def parse_config(conffile, dbname, date):
    """ parse given INI file, and return it all """

    if VERBOSE:
        print "Parsing configuration INI file '%s'" % conffile

    config = ConfigParser.ConfigParser()

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
                          date,
                          config.get(dbname, "host"),
                          config.get(dbname, "postgres_port"),
                          config.get(dbname, "pgbouncer_port"),
                          config.get(dbname, "pgbouncer_conf"),
                          config.get(dbname, "pgbouncer_rcmd"),
                          config.get(dbname, "keep_bases"),
                          config.get(dbname, "auto_switch"),
                          config.get(dbname, "use_sudo"))
    except Exception, e:
        print >>sys.stderr, "Configuration error: %s" % e
        sys.exit(4)

    return staging

if __name__ == '__main__':
    # first parse command line
    conffile, command, dbname, date = parse_options()

    # only load staging module after options are set
    from options import VERBOSE, DRY_RUN
    from staging import Staging
    from staging import NotYetImplementedException, CouldNotGetDumpException

    if VERBOSE:
        print "Parsed command='%s', dbname='%s', date='%s'" \
              % (command, dbname, date)

    # now load configuration
    staging = parse_config(conffile, dbname, date)

    # and act accordinly
    try:
        if command == "restore":
            staging.restore()

        elif command == "switch":
            staging.switch()

        elif command == "drop":
            staging.drop()

        else:
            print >>sys.stderr, "Error: unknown command '%s'" % command
            sys.exit(5)

    except NotYetImplementedException, e:
        print >>sys.stderr, "Error: %s is not yet implemented." % command

        if e:
            print >>sys.stderr, e

    except CouldNotGetDumpException, e:
        print >>sys.stderr, "Error: could not get dump '%s%s'" \
              % (staging.backup_host, staging.backup_filename)
        print >>sys.stderr, e
            
    except Exception, e:
        print >>sys.stderr, "Error: couldn't %s %s" % (command, dbname)
        raise
