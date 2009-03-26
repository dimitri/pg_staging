##
## Staging Class to organise processing
##

import os, httplib

from options import NotYetImplementedException
from options import CouldNotGetDumpException
from options import PGRestoreFailedException
from options import SubprocessException
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
                 postgres_major,
                 pgbouncer_port,
                 pgbouncer_conf,
                 remove_dump   = True,
                 keep_bases    = 2,
                 auto_switch   = True,
                 use_sudo      = True,
                 pg_restore    = "/usr/bin/pg_restore",
                 pg_restore_st = True):
        """ Create a new staging object, configured """

        self.section         = section
        self.dbname          = dbname
        self.backup_host     = backup_host
        self.backup_base_url = backup_base_url
        self.host            = host
        self.dbuser          = dbuser
        self.dbowner         = dbowner
        self.maintdb         = maintdb
        self.postgres_port   = int(postgres_port)
        self.postgres_major  = postgres_major
        self.pgbouncer_port  = int(pgbouncer_port)
        self.pgbouncer_conf  = pgbouncer_conf
        self.remove_dump     = remove_dump == "True"
        self.keep_bases      = int(keep_bases)
        self.auto_switch     = auto_switch == "True"
        self.use_sudo        = use_sudo    == "True"
        self.pg_restore      = pg_restore
        self.pg_restore_st   = pg_restore_st == "True"
        self.schemas         = None
        self.replication     = None

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

        from options import VERBOSE, TERSE
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
        from options import TMPDIR, BUFSIZE
        
        if not self.backup_date:
            raise UnknownBackupDateException
        
        filename = "%s/%s.%s.dump" % (TMPDIR, self.dbname, self.backup_date)
        dump_fd  = open(filename, "wb")

        from options import VERBOSE, TERSE
        if not TERSE:
            print "fetching '%s' from http://%s%s" % (filename,
                                                      self.backup_host,
                                                      self.backup_filename)

        conn = httplib.HTTPConnection(self.backup_host)
        conn.request("GET", self.backup_filename)
        r = conn.getresponse()

        if r.status != 200:
            mesg = "Could not get dump '%s': %s" % (self.backup_filename,
                                                    r.reason)
            raise CouldNotGetDumpException, mesg

        done = False
        while not done:
            data = r.read(BUFSIZE)
            if data:
                dump_fd.write(data)

            done = not data or len(data) < BUFSIZE

        dump_fd.close()

        return filename

    def do_remove_dump(self, filename):
        """ remove dump when self.remove_dump says so """
        from options import VERBOSE
        
        if self.remove_dump:
            if VERBOSE:
                print "rm %s" % filename
            os.unlink(filename)

    def get_nodata_tables(self):
        """ return a list of tables to avoid restoring """

        # we avoid restoring tables which we are a replication subscriber of
        tables = set()
        if self.replication:
            for s in self.replication.sections():
                if self.replication.has_option(s, 'subscriber'):
                    if self.replication.get(s, 'subscriber') == self.section:
                        p = set(self.replication.get(s, 'provides').split(' '))
                        tables = tables.union(p)
        return tables

    def get_catalog(self, filename):
        """ get a cleaned out catalog (nodata tables are commented) """
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore,
                              self.pg_restore_st,
                              self.schemas)

        catalog = r.get_catalog(filename, self.get_nodata_tables())
        return catalog

    def restore(self):
        """ launch a pg_restore for the current staging configuration """
        from options import VERBOSE, TERSE

        # first attempt to establish the connection to remote server
        # no need to fetch the big backup file unless this succeed
        #
        # we will restore from pgbouncer connection, first connection is
        # made to the maintenance database
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore,
                              self.pg_restore_st,
                              self.schemas)

        # while connected, try to create the database
        r.createdb()

        # add the new database to pgbouncer configuration now
        # it could be that the restore will be connected to pgbouncer
        self.pgbouncer_add_database()

        # now, download the dump we need.
        filename = self.get_dump()

        # and restore it
        mesg = None
        try:
            if VERBOSE:
                os.system("ls -l %s" % filename)
            r.pg_restore(filename)

            # only switch pgbouncer configuration to new database when there
            # was no restore error
            if self.auto_switch:
                self.switch()

        except Exception, e:
            mesg  = "Error: couldn't pg_restore from '%s'" % (filename)
            mesg += "\nDetail: %s" % e
            self.do_remove_dump(filename)
            raise PGRestoreFailedException, mesg

        # remove the dump even when there was no exception
        self.do_remove_dump(filename)

    def load(self, filename):
        """ will pg_restore from the already present dumpfile and determine
        the backup date from the file, which isn't removed """
        from options import VERBOSE, TERSE

        # first parse the dump filename
        try:
            dbname, date, ext = filename.split('.')
            self.backup_date  = date.replace('-', '')

            # just validate it's an 8 figures integer
            if len(self.backup_date) != 8:
                raise ValueError
            x = int(self.backup_date)

            self.dated_dbname = "%s_%s" % (self.dbname, self.backup_date)
            
        except ValueError, e:
            mesg = "load: '%s' isn't a valid dump file name" % filename
            raise ParseDumpFileException, mesg
        
        # see comments in previous self.restore() method
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore,
                              self.pg_restore_st,
                              self.schemas)
        r.createdb()
        self.pgbouncer_add_database()

        # now restore the dump
        try:
            if VERBOSE:
                os.system("ls -l %s" % filename)
            r.pg_restore(filename)

        except Exception, e:
            mesg  = "Error: couldn't pg_restore from '%s'" % (filename)
            mesg += "\nDetail: %s" % e
            raise PGRestoreFailedException, mesg        

    def switch(self):
        """ edit pgbouncer configuration file to have canonical dbname point
        to given date (backup_date) """

        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.postgres_port)

        newconffile = p.switch_to_database(self.dbname,
                                           self.dated_dbname,
                                           self.postgres_port)

        self.pgbouncer_update_conf(newconffile)

    def pgbouncer_add_database(self):
        """ edit pgbouncer configuration file to add a database """
        
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.postgres_port)

        newconffile = p.add_database(self.dated_dbname,
                                     self.postgres_port)

        self.pgbouncer_update_conf(newconffile)

    def pgbouncer_update_conf(self, newconffile):
        """ reconfigure targeted pgbouncer with given file """
        import os.path, subprocess
        from options import VERBOSE, TERSE, CLIENT_SCRIPT

        baseconfdir = os.path.dirname(self.pgbouncer_conf)

        if self.use_sudo:
            sudo = "sudo"
        else:
            sudo = ""

        # commands = [(command line, (return, codes, awaited)), ...]
        # default retcode is a tuple containing only 0
        retcode = 0,
        commands = [
            ("scp %s %s:/tmp" % (newconffile, self.host), retcode),
            ("ssh %s %s ./%s %s %s" % (self.host,
                                       sudo,
                                       CLIENT_SCRIPT,
                                       newconffile,
                                       self.pgbouncer_port), retcode)
            ]

        # skip scp when target is localhost
        if self.host in ("localhost", "127.0.0.1"):
            commands = commands[1:]

        for cmd, returns in commands:
            if not TERSE:
                print cmd

            proc = subprocess.Popen(cmd.split(" "),
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE)

            out, err = proc.communicate()

            if proc.returncode not in returns:
                mesg  = 'Error [%d]: %s' % (proc.returncode, cmd)
                mesg += '\nDetail: %s' % err
                raise SubprocessException, mesg

        # if target isn't localhost, rm the local temp file
        # when target host is localhost, we used mv already
        if self.host not in ("localhost", "127.0.0.1"):
            os.unlink(newconffile)
                
    def drop(self):
        """ drop the given database: dbname_%(backup_date) """

        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)
        
        r.dropdb()
        
    def dbsize(self):
        """ return database size, pretty printed """
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)
        
        return self.dated_dbname, r.dbsize()

    def pgbouncer_databases(self):
        """ return pgbouncer database list: name, database, host, port """
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.pgbouncer_port)

        for d in p.databases():
            yield d['name'], d['database'], d['host'], d['port']

        return

    def pgbouncer_pause(self, dbname):
        """ pause given database """
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.pgbouncer_port)

        p.pause(dbname)


    def pgbouncer_resume(self, dbname):
        """ resume given database """
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.pgbouncer_port)

        p.resume(dbname)
