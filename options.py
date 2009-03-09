##
## pg_staging.py common options
##

VERSION = 0.2

VERBOSE = False
DRY_RUN = False
DEFAULT_CONFIG_FILE = "/etc/hm-tools/pg_staging.ini"

class NotYetImplementedException(Exception):
    """ Please try again """
    pass

class CouldNotGetDumpException(Exception):
    """ HTTP Return code was not 200 """
    pass

class CouldNotConnectPostgreSQLException(Exception):
    """ Just that, check the config """
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

