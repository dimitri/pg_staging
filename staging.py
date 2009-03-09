##
## Staging Class to organise processing
##

import os, httplib

from options import NotYetImplementedException
from options import CouldNotGetDumpException
from options import PGRestoreFailedException
import pgbouncer, restore

class Staging:
    """ Staging Object relates to a database name, where to find the backups
    and a destination where to restore it"""

    def __init__(self,
                 section,
                 backup_host,
                 backup_base_url,
                 host,
                 dbname,
                 dbuser,
                 dbowner,
                 maintdb,
                 postgres_port,
                 pgbouncer_port,
                 pgbouncer_conf,
                 pgbouncer_rcmd,
                 remove_dump = True,
                 keep_bases  = 2,
                 auto_switch = True,
                 use_sudo    = True):
        """ Create a new staging object, configured """

        self.section         = section
        self.dbname          = dbname
        self.backup_host     = backup_host
        self.backup_base_url = backup_base_url
        self.host            = host
        self.dbuser          = dbuser
        self.dbowner         = dbowner
        self.maintdb         = maintdb
        self.postgres_port   = postgres_port
        self.pgbouncer_port  = pgbouncer_port
        self.pgbouncer_conf  = pgbouncer_conf
        self.pgbouncer_rcmd  = pgbouncer_rcmd
        self.remove_dump     = remove_dump == "True"
        self.keep_bases      = int(keep_bases)
        self.auto_switch     = auto_switch == "True"
        self.use_sudo        = use_sudo    == "True"

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

        from options import VERBOSE
        if VERBOSE:
            print "backup filename is '%s'" % self.backup_filename
            print "target database backup date is '%s'" % self.dated_dbname

    def list_backups(self):
        """ return a list of available backup files for self.dbname """
        from apache_listing import ApacheListingParser
        
        conn = httplib.HTTPConnection(self.backup_host)
        conn.request("GET", self.backup_base_url)
        r = conn.getresponse()

        if r.status != 200:
            raise CouldNotGetDumpException, r.reason

        # fill up a buffer with the response
        import cStringIO
        buf = cStringIO.StringIO()

        # now parse the apache listing
        for chunk in r.read():
            buf.write(chunk)

        # don't forget to re-position 
        buf.seek(0)
        alp = ApacheListingParser(buf, self.dbname)
        return alp.parse()

    def get_dump(self):
        """ get the dump file from the given URL """
        if not self.backup_date:
            raise UnknownBackupDateException
        
        filename = "/tmp/%s.%s.dump" % (self.dbname, self.backup_date)
        dump_fd  = open(filename, "wb")

        conn = httplib.HTTPConnection(self.backup_host)
        conn.request("GET", self.backup_filename)
        r = conn.getresponse()

        if r.status != 200:
            mesg = "Could not get dump '%s': %s" % (self.backup_filename,
                                                    r.reason)
            raise CouldNotGetDumpException, mesg

        dump_fd.write(r.read())
        dump_fd.close()

        return dump_fd, filename

    def restore(self):
        """ launch a pg_restore for the current staging configuration """
        from options import VERBOSE

        # first attempt to establish the connection to remote server
        # no need to fetch the big backup file unless this succeed
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb)

        # while connected, try to create the database
        r.createdb()

        # now, download the dump we need.
        dump_fd, filename = self.get_dump()

        # and restore it
        mesg = None
        try:
            if VERBOSE:
                os.system("ls -l %s" % filename)
            r.pg_restore(filename)

        except Exception, e:
            mesg  = "Error: couldn't pg_restore from '%s'" % (filename)
            mesg += "\nDetail: %s" % e

        if self.remove_dump:
            if VERBOSE:
                print "rm %s" % filename
            os.unlink(filename)

        if mesg:
            raise PGRestoreFailedException, mesg

    def switch(self):
        """ edit pgbouncer configuration file to have canonical dbname point
        to given date (backup_date) """
        raise NotYetImplementedException, "switch is not yet implemented"

    def drop(self):
        """ drop the given database: dbname_%(backup_date) """

        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb)
        
        r.dropdb()
        
