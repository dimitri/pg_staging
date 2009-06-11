##
## pg_staging.py common options
##

VERSION = 0.7

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

# where to store the pg_staging console history
CONSOLE_HISTORY     = "~/.pg_staging_history"
