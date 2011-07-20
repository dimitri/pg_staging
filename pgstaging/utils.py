# Exceptions and utilities
import shlex, subprocess

RET_CODE = 0
RET_OUT  = 1
RET_ERR  = 2
RET_BOTH = 4

PRE_SQL  = -1
POST_SQL =  1

def run_command(command,
                expected_retcodes = 0, returning = RET_CODE,
                stdin = None, stdout = subprocess.PIPE):
    """run a command and raise an exception if retcode not in expected_retcode"""
    from options import VERBOSE, DEBUG
    if VERBOSE:
        print command

    # we want expected_retcode to be a tuple but will manage integers
    if type(expected_retcodes) == type(0):
        expected_retcodes = (expected_retcodes,)

    # we want the command to be a list, but accomodate when given a string
    cmd = command
    if type(cmd) == type('string'):
        cmd = shlex.split(command)

    proc = subprocess.Popen(cmd,
                            stdin  = stdin,
                            stdout = stdout,
                            stderr = subprocess.PIPE)

    out, err = proc.communicate()

    if proc.returncode not in expected_retcodes:
        if DEBUG:
            print out

        # when nothing gets to stderr, add stdout to Detail
        if err.strip() == '':
            err = out

        mesg  = 'Error [%d]: %s' % (proc.returncode, command)
        mesg += '\nDetail: %s' % err
        raise SubprocessException, mesg

    # not the cleanest API one could imagine
    if returning == RET_CODE:
        return proc.returncode
    elif returning == RET_OUT:
        return out
    elif returning == RET_ERR:
        return err
    elif returning == RET_BOTH:
        return (out, err)
    else:
        return proc.returncode

def scp(host, src, dst):
    """ scp src host:dst """
    command = "scp %s %s:/tmp" % (src, host)
    return run_command(command)

def ssh_cat(host, filename):
    """ ssh host cat filename """
    command = "ssh %s cat %s" % (host, filename)
    return run_command(command, returning = RET_OUT)

def run_client_script(host, args, use_sudo = True, ret = RET_OUT):
    """ ssh host sudo CLIENT_SCRIPT args """
    from options import CLIENT_SCRIPT

    if use_sudo:
        sudo = "sudo"
    else:
        sudo = ""

    str_args = " ".join([str(x) for x in args])
    command  = "ssh %s %s %s %s" % (host, sudo, CLIENT_SCRIPT, str_args)
    return run_command(command, returning = ret )

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


class StagingRuntimeException(Exception):
    """ Got an exception while running a command  """
    pass

class UnknownCommandException(Exception):
    """ What command did you want me to run exactly? """
    pass

class ExportFileAlreadyExistsException(Exception):
    """ Use --force to overwrite existing export filename """
    pass

class NoActiveDatabaseException(Exception):
    """ please specify a valid database choice """
    pass
