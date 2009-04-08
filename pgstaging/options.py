##
## pg_staging.py common options
##

VERSION = 0.6

VERBOSE = False
TERSE   = False
DEBUG   = False
DRY_RUN = False

BUFSIZE = 8 * 1024 * 1024

TMPDIR              = None    # given via -t option
DEFAULT_TMPDIR      = '/tmp'  # hardcoded default for when no setup is made
DEFAULT_CONFIG_FILE = "/etc/hm-tools/pg_staging.ini"
CLIENT_SCRIPT       = "staging-client.sh"

# when using cat file.pgs | pg_staging.py
COMMENT             = "#"

class NotYetImplementedException(Exception):
    """ Please try again """
    pass

class CouldNotGetDumpException(Exception):
    """ HTTP Return code was not 200 """
    pass

class CouldNotConnectPostgreSQLException(Exception):
    """ Just that, check the config """
    pass

class CreatedbFailedException(Exception):
    """ create db error """
    pass

class PGRestoreFailedException(Exception):
    """ pg_restore failure """
    pass

class CouldNotGetPgBouncerConfigException(Exception):
    """ the ssh to get current pgbouncer config failed  """
    pass

class WrongNumberOfArgumentsException(Exception):
    """ The command didn't receive what it wants to work """
    pass

class UnknownBackupDateException(Exception):
    """ most of Staging object methods require set_backup_date() """
    pass

class NoArgsCommandLineException(Exception):
    """ Signal we want to get the console """
    pass

class UnknownSectionException(Exception):
    """ What section you want?  """
    pass

class UnknownOptionException(Exception):
    """ What option are you talking about? """
    pass

class SubprocessException(Exception):
    """ we used subprocess.call(command) and it returns non zero """
    pass

class ParseDumpFileException(Exception):
    """ can not parse the dump file: allopass_db.`date -I`.dump """
    pass

