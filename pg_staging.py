#!/usr/bin/env python
#
# pg_staging allows to
#
import os, os.path, sys
from optparse import OptionParser

import pgstaging.options
from pgstaging.options import NotYetImplementedException
from pgstaging.options import CouldNotGetDumpException
from pgstaging.options import CouldNotConnectPostgreSQLException
from pgstaging.options import WrongNumberOfArgumentsException
from pgstaging.options import UnknownBackupDateException
from pgstaging.options import NoArgsCommandLineException
from pgstaging.options import StagingRuntimeException

from pgstaging.staging import Staging

import pgstaging.commands as commands

def parse_options():
    """ get the command to run from command line """

    usage  = "%prog [-c <config_filename>] command dbname <args>"
    parser = OptionParser(usage = usage)
    
    parser.add_option("--version", action = "store_true",
                      dest    = "version",
                      default = False,
                      help    = "show pg_staging version")

    parser.add_option("-c", "--config", dest = "config",
                      default = pgstaging.options.DEFAULT_CONFIG_FILE,
                      help    = "configuration file, defauts to %s" \
                      % pgstaging.options.DEFAULT_CONFIG_FILE)

    parser.add_option("-n", "--dry-run", action = "store_true",
                      dest    = "dry_run",
                      default = False,
                      help    = "simulate operations, don't do them")

    parser.add_option("-v", "--verbose", action = "store_true",
                      dest    = "verbose",
                      default = False,
                      help    = "be verbose and about processing progress")

    parser.add_option("-q", "--quiet", action = "store_true",
                      dest    = "terse",
                      default = False,
                      help    = "be terse, almost silent")

    parser.add_option("-d", "--debug", action = "store_true",
                      dest    = "debug",
                      default = False,
                      help    = "provide python stacktrace when error")

    parser.add_option("-t", "--tmpdir", dest = "tmpdir",
                      default = pgstaging.options.TMPDIR,
                      help    = "temp dir where to fetch dumps, %s" \
                      % pgstaging.options.TMPDIR)

    (opts, args) = parser.parse_args()

    if opts.version:
        from pgstaging.options import VERSION
        print "pg_staging %s" % VERSION
        sys.exit(0)

    pgstaging.options.DEBUG   = opts.debug
    pgstaging.options.TERSE   = opts.terse
    pgstaging.options.VERBOSE = opts.verbose
    pgstaging.options.DRY_RUN = opts.dry_run
    pgstaging.options.TMPDIR  = opts.tmpdir

    if pgstaging.options.TERSE and pgstaging.options.VERBOSE:
        print >>sys.stderr, "Error: can't be verbose and terse"
        sys.exit(1)

    if pgstaging.options.DRY_RUN:
        print >>sys.stderr, "Error: dry run is not yet implemented"
        sys.exit(1)

    if pgstaging.options.VERBOSE:
        print "We'll be verbose!"

    # check existence and read ability of config file
    if pgstaging.options.VERBOSE:
        print "Checking if config file '%s' exists" % opts.config

    if not os.path.exists(opts.config):
        print >>sys.stderr, \
              "Error: Configuration file %s does not exists" % opts.config
        sys.exit(1)

    if pgstaging.options.VERBOSE:
        print "Checking if config file '%s' is readable" % opts.config

    if not os.access(opts.config, os.R_OK):
        print >>sys.stderr, \
              "Error: Can't read configuration file %s" % opts.config
        print >>sys.stderr, parser.format_help()
        sys.exit(1)

    if len(args) == 0:
        if pgstaging.options.VERBOSE:
            print "No arguments, want console?"
        raise NoArgsCommandLineException, opts.config        

    # we return configuration filename, command, command args
    return opts.config, args[0], args[1:]


if __name__ == '__main__':

    # first parse command line
    try:
        conffile, command, args = parse_options()

    except NoArgsCommandLineException, conffile:
        # ok I need to read docs about exceptions, conffile here ain't
        # exactly pretty

        # no args given, console mode
        if sys.stdin.isatty():
            from pgstaging.console import Console
            c = Console()
            c.set_config(str(conffile), recheck = False)
            c.cmdloop()

            # exiting the console also exit main program
            sys.exit(0)

        else:
            # loop over input lines            
            for line in sys.stdin:
                try:
                    commands.parse_input_line_and_run_command(line)
                except StagingRuntimeException, e:
                    sys.exit(1)
                except Exception, e:
                    raise

            sys.exit(0)

    try:
        commands.run_command(conffile, command, args)
    except StagingRuntimeException, e:
        sys.exit(1)
    except Exception, e:
        raise

    sys.exit(0)
