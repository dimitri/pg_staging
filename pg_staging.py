#!/usr/bin/env python
#
# pg_staging allows to
#
import os, os.path, sys
from optparse import OptionParser

import options
from options import NotYetImplementedException
from options import CouldNotGetDumpException
from options import CouldNotConnectPostgreSQLException
from options import WrongNumberOfArgumentsException
from options import UnknownBackupDateException
from options import NoArgsCommandLineException

from staging import Staging

import commands

def parse_options():
    """ get the command to run from command line """

    usage  = "%prog [-c <config_filename>] command dbname <args>"
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

    # check existence and read ability of config file
    if options.VERBOSE:
        print "Checking if config file '%s' exists" % opts.config

    if not os.path.exists(opts.config):
        print >>sys.stderr, \
              "Error: Configuration file %s does not exists" % opts.config
        sys.exit(1)

    if options.VERBOSE:
        print "Checking if config file '%s' is readable" % opts.config

    if not os.access(opts.config, os.R_OK):
        print >>sys.stderr, \
              "Error: Can't read configuration file %s" % opts.config
        print >>sys.stderr, parser.format_help()
        sys.exit(1)

    if len(args) == 0:
        if options.VERBOSE:
            print "No arguments, want console?"
        raise NoArgsCommandLineException, opts.config        

    # we return configuration filename, command, command args
    return opts.config, args[0], args[1:]

if __name__ == '__main__':
    # first parse command line
    try:
        conffile, command, args = parse_options()
    except NoArgsCommandLineException, conffile:
        # no args given, console mode
        from console import Console
        c = Console()
        c.set_config(str(conffile)) # ok I need to read docs about exceptions
        c.cmdloop()

        # exiting the console also exit main program
        sys.exit(0)

    # only load staging module after options are set
    from options import VERBOSE, DRY_RUN

    # now that VERBOSE and DRY_RUN are correctly set, parse config
    #staging = commands.parse_config(conffile, dbname)

    # and act accordinly
    if command not in commands.exports:
        print >>sys.stderr, "Error: no command '%s'" % command
        sys.exit(1)
        
    try:
        commands.exports[command](conffile, args)
    except Exception, e:
        print >>sys.stderr, e
        sys.exit(1)

    sys.exit(0)
