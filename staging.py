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
                 pgbouncer_rcmd,
                 remove_dump   = True,
                 keep_bases    = 2,
                 auto_switch   = True,
                 use_sudo      = True,
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
        self.pgbouncer_rcmd  = pgbouncer_rcmd
        self.remove_dump     = remove_dump == "True"
        self.keep_bases      = int(keep_bases)
        self.auto_switch     = auto_switch == "True"
        self.use_sudo        = use_sudo    == "True"
        self.pg_restore_st   = pg_restore_st == "True"
        self.schemas         = None

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
        if not self.backup_date:
            raise UnknownBackupDateException
        
        filename = "/tmp/%s.%s.dump" % (self.dbname, self.backup_date)
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

        dump_fd.write(r.read())
        dump_fd.close()

        return dump_fd, filename

    def do_remove_dump(self, filename):
        """ remove dump when self.remove_dump says so """
        from options import VERBOSE
        
        if self.remove_dump:
            if VERBOSE:
                print "rm %s" % filename
            os.unlink(filename)

    def restore(self):
        """ launch a pg_restore for the current staging configuration """
        from options import VERBOSE, TERSE

        # first attempt to establish the connection to remote server
        # no need to fetch the big backup file unless this succeed
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore_st,
                              self.schemas)

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
            self.do_remove_dump(filename)
            raise PGRestoreFailedException, mesg

        if self.auto_switch:
            self.switch()

        # remove the dump even when there was no exception
        self.do_remove_dump(filename)

    def switch(self):
        """ edit pgbouncer configuration file to have canonical dbname point
        to given date (backup_date) """
        import os.path, subprocess
        from options import VERBOSE, TERSE

        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.pgbouncer_rcmd,
                                self.dbuser,
                                self.host,
                                self.pgbouncer_port)

        baseconfdir = os.path.dirname(self.pgbouncer_conf)
        newconffile = p.switch_to_database(self.dbname,
                                           self.dated_dbname,
                                           self.postgres_port)

        if self.use_sudo:
            sudo = "sudo"
        else:
            sudo = ""

        commands = [
            "scp %s %s:/tmp" % (newconffile, self.host),

            "ssh %s %s mv /tmp/%s %s" \
            % (self.host, sudo, os.path.basename(newconffile), baseconfdir),

            "ssh %s %s chmod a+r %s/%s" \
            % (self.host, sudo, baseconfdir, os.path.basename(newconffile)),
            
            "ssh %s cd %s && %s ln -sf %s pgbouncer.ini" % \
            (self.host, baseconfdir, sudo, os.path.basename(newconffile)),

            "ssh %s %s %s" % (self.host, sudo, self.pgbouncer_rcmd)
            ]

        for cmd in commands:
            if not TERSE:
                print cmd

            proc = subprocess.Popen(cmd.split(" "),
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE)

            out, err = proc.communicate()

            if proc.returncode != 0:
                # UGLY HACK WARNING
                # it seems it's ok for pgbouncer reload to ret 3
                if cmd.find("/etc/init.d/pgbouncer reload") > -1:
                    if proc.returncode == 3:
                        break
                    
                mesg  = 'Error [%d]: %s' % (proc.returncode, cmd)
                mesg += '\nDetail: %s' % err
                raise SubprocessException, mesg
                
        if VERBOSE:
            print "rm %s" % newconffile
        os.unlink(newconffile)

    def drop(self):
        """ drop the given database: dbname_%(backup_date) """

        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)
        
        r.dropdb()
        
    def dbsize(self):
        """ return database size, pretty printed """
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)
        
        return self.dated_dbname, r.dbsize()

    def pgbouncer_databases(self):
        """ return pgbouncer database list: name, database, host, port """
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.pgbouncer_rcmd,
                                self.dbuser,
                                self.host,
                                self.pgbouncer_port)

        for d in p.databases():
            yield d['name'], d['database'], d['host'], d['port']

        return
