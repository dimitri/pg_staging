##
## Staging Class to organise processing
##

import os

from options import VERBOSE, DRY_RUN
from options import NotYetImplementedException, CouldNotGetDumpException
import pgbouncer, restore

class Staging:
    """ Staging Object relates to a database name, where to find the backups
    and a destination where to restore it"""

    def __init__(self,
                 dbname,
                 backup_host,
                 backup_base_url,
                 host,
                 dbuser,
                 dbowner,
                 postgres_port,
                 pgbouncer_port,
                 pgbouncer_conf,
                 pgbouncer_rcmd,
                 keep_bases  = 2,
                 auto_switch = True,
                 use_sudo    = True):
        """ Create a new staging object, configured """

        self.dbname          = dbname
        self.backup_host     = backup_host
        self.backup_base_url = backup_base_url
        self.host            = host
        self.dbuser          = dbuser
        self.dbowner         = dbowner
        self.postgres_port   = postgres_port
        self.pgbouncer_port  = pgbouncer_port
        self.pgbouncer_conf  = pgbouncer_conf
        self.pgbouncer_rcmd  = pgbouncer_rcmd
        self.keep_bases      = keep_bases
        self.auto_switch     = auto_switch
        self.use_sudo        = use_sudo

        # init separately, we don't have the information when we create the
        # Staging object from configuration.
        self.backup_date     = None
        self.backup_filename = None

    def set_backup_date(self, date = None):
        """ set the backup date choosen by the user """

        if date is None:
            import datetime
            self.backup_date = datetime.date.today().isoformat()
        else:
            self.backup_date = date
            
        self.dated_dbname = "%s_%s" % (self.dbname,
                                       self.backup_date.replace('-', ''))

        self.backup_filename = "%s%s.%s.dump" \
                               % (self.backup_base_url,
                                  self.dbname,
                                  self.backup_date)

        if VERBOSE:
            print "backup filename is '%s'" % self.backup_filename
            print "target database backup date is '%s'" % self.dated_dbname

    def get_dump(self):
        """ get the dump file from the given URL """
        if not self.backup_date:
            raise UnknownBackupDateException
        
        import tempfile, httplib

        tmp_prefix = "%s.%s." % (self.dbname, self.backup_date)

        dump_fd, filename = tempfile.mkstemp(suffix = ".dump",
                                             prefix = tmp_prefix)

        conn = httplib.HTTPConnection(self.backup_host)
        conn.request("GET", self.backup_filename)
        r = conn.getresponse()

        if r.status != 200:
            raise CouldNotGetDumpException, r.reason

        os.write(dump_fd, r.read())

        return dump_fd, filename

    def restore(self):
        """ launch a pg_restore for the current staging configuration """

        # first attempt to establish the connection to remote server
        # no need to fetch the big backup file unless this succeed
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner)

        # while connected, try to create the database
        r.createdb()

        # now, download the dump we need.
        dump_fd, filename = self.get_dump()

        if VERBOSE:
            os.system("ls -l %s" % filename)

        r.pg_restore(filename)

        if VERBOSE:
            print "Restore is done, removing the dump file '%s'" % filename

        os.close(dump_fd)
        os.unlink(filename)

        raise NotYetImplementedException

    def switch(self):
        """ edit pgbouncer configuration file to have canonical dbname point
        to given date (backup_date) """
        pass

    def drop(self):
        """ drop the given database: dbname_%(backup_date) """
        pass
