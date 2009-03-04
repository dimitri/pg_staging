##
## pg_staging.py common options
##

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

