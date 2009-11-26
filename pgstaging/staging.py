##
## Staging Class to organise processing
##

import os, httplib, time, psycopg2

import pgbouncer, restore, londiste, utils
from utils import NotYetImplementedException
from utils import CouldNotGetDumpException
from utils import PGRestoreFailedException
from utils import SubprocessException
from utils import ExportFileAlreadyExistsException

class Staging:
    """ Staging Object relates to a database name, where to find the backups
    and a destination where to restore it"""

    def __init__(self,
                 section,
                 backup_host,
                 backup_base_url,
                 dumpall_url,
                 host,
                 dbname,
                 dbuser,
                 dbowner,
                 maintdb,
                 db_encoding,
                 postgres_port,
                 postgres_major,
                 pgbouncer_port,
                 pgbouncer_conf,
                 remove_dump    = True,
                 keep_bases     = 2,
                 auto_switch    = True,
                 use_sudo       = True,
                 pg_restore     = "/usr/bin/pg_restore",
                 pg_restore_st  = True,
                 restore_vacuum = True,
                 restore_jobs   = 1,
                 tmpdir         = None):
        """ Create a new staging object, configured """

        self.creation_time   = time.time()

        self.section         = section
        self.dbname          = dbname
        self.backup_host     = backup_host
        self.backup_base_url = backup_base_url
        self.dumpall_url     = dumpall_url
        self.host            = host
        self.dbuser          = dbuser
        self.dbowner         = dbowner
        self.maintdb         = maintdb
        self.db_encoding     = db_encoding
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
        self.restore_vacuum  = restore_vacuum == "True"
        self.restore_jobs    = int(restore_jobs)
        self.replication     = None
        self.tmpdir          = tmpdir
        self.sql_path        = None
        self.base_backup_cmd = None
        self.wal_archive_cmd = None

        self.schemas         = []
        self.schemas_nodata  = []
        self.search_path     = []

        # init separately, we don't have the information when we create the
        # Staging object from configuration.
        self.backup_date     = None
        self.backup_filename = None

    def parse_date(self, date):
        """ parse user given date """
        import datetime

        if date.find('-') > -1:
            y, m, d = date.split('-')
            return datetime.date(int(y), int(m), int(d))

        elif len(date) == 8:
            dummy = int(date) # poor man's parsing to raise exception
            y = date[0:4]
            m = date[4:6]
            d = date[6:8]
            return datetime.date(int(y), int(m), int(d))

        else:
            raise ValueError, "Unable to parse date: '%s'" % date

    def set_backup_date(self, date = None):
        """ set the backup date choosen by the user """
        import datetime
        
        if date is None or date == "today":
            backup_date = datetime.date.today()
        else:
            backup_date = self.parse_date(date)

        self.backup_date = backup_date.isoformat()
            
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

    def timing(self):
        """ return time elapsed since staging object creation """
        return time.time() - self.creation_time

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

    def wget(self, host, url, outfile):
        """ fetch the given url at given host and return where we stored it """
        from options import TERSE, BUFSIZE

        filename = "%s/%s" % (self.tmpdir, outfile)

        if not TERSE:
            print "fetching '%s'\n    from http://%s%s" % (filename,
                                                           host,
                                                           url)
            
        import time
        start_time = time.time()

        dump_fd = open(filename, "wb")
        conn    = httplib.HTTPConnection(host)
        conn.request("GET", url)
        r = conn.getresponse()

        if r.status != 200:
            mesg = "Could not get dump '%s': %s" % (url, r.reason)
            raise CouldNotGetDumpException, mesg

        done = False
        while not done:
            data = r.read(BUFSIZE)
            if data:
                dump_fd.write(data)

            done = not data or len(data) < BUFSIZE

        dump_fd.close()

        end_time = time.time()
        self.wget_timing = end_time - start_time

        return filename

    def init_cluster(self):
        """ init a PostgreSQL cluster from pg_dumpall -g sql script """
        # unused here, in fact
        self.dated_dbname = None
        
        basename = "%s.%s" % (self.section, os.path.basename(self.dumpall_url))
        filename = self.wget(self.backup_host, self.dumpall_url, basename)

        # the restore object host the source sql file method
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)

        # if necessary, add a new postgres database to pgbouncer setup just
        # to be able to replay the globals dumpall file, which will \connect
        # postgres
        #
        # this is needed in case of multi cluster support with a single
        # pgbouncer instance.
        if self.maintdb != 'postgres':
            self.pgbouncer_add_database('postgres')

        # psql -f filename
        r.source_sql_file(filename)

        # don't forget to clean up the mess
        os.unlink(filename)

        if self.maintdb != 'postgres':
            self.pgbouncer_del_database('postgres')
        
        return    

    def get_dump(self):
        """ get the dump file from the given URL """
        if not self.backup_date:
            raise UnknownBackupDateException
        
        filename = "%s.%s.dump" % (self.dbname, self.backup_date)
        return self.wget(self.backup_host,                            # host
                         "%s/%s" % (self.backup_base_url, filename),  # url
                         filename)                                    # out

    def do_remove_dump(self, filename):
        """ remove dump when self.remove_dump says so """
        from options import VERBOSE
        
        if self.remove_dump:
            if VERBOSE:
                print "rm %s" % filename
            os.unlink(filename)

    def get_nodata_tables(self):
        """ return a list of tables to avoid restoring """

        if self.replication:
            l = londiste.londiste(self.replication, self.section,
                                  self.dbname, self.dated_dbname, self.tmpdir)

            return l.get_nodata_tables()
        
        return

    def get_triggers(self, filename):
        """ get a list of triggers with the functions attached to them """
        self.dated_dbname = None
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore,
                              self.pg_restore_st,
                              connect = False)

        return r.get_trigger_funcs(filename)

    def get_catalog(self, filename):
        """ get a cleaned out catalog (nodata tables are commented) """
        self.dated_dbname = None
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.postgres_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore,
                              self.pg_restore_st,
                              self.schemas,
                              self.schemas_nodata,
                              connect = False)

        catalog = r.get_catalog(filename, self.get_nodata_tables())
        return catalog.getvalue()

    def restore(self):
        """ launch a pg_restore for the current staging configuration """
        from options import VERBOSE, TERSE, DEBUG

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
                              self.schemas,
                              self.schemas_nodata)

        # while connected, try to create the database
        r.createdb(self.db_encoding)

        # add the new database to pgbouncer configuration now
        # it could be that the restore will be connected to pgbouncer
        self.pgbouncer_add_database()

        # source the extra SQL files (generator function)
        for sql in self.psql_source_files(utils.PRE_SQL):
            if VERBOSE:
                print "psql -f %s" % sql

        # now, download the dump we need.
        filename = self.get_dump()

        # and restore it
        mesg = None
        try:
            if VERBOSE:
                os.system("ls -l %s" % filename)
            
            r.restore_jobs = self.restore_jobs
            secs = r.pg_restore(filename, self.get_nodata_tables())

            # only switch pgbouncer configuration to new database when there
            # was no restore error
            if self.auto_switch:
                self.switch()

        except Exception, e:
            if DEBUG:
                raise
            mesg  = "Error: couldn't pg_restore from '%s'" % (filename)
            mesg += "\nDetail: %s" % e
            raise PGRestoreFailedException, mesg

        # set the database search_path if non default
        self.set_database_search_path()

        # remove the dump even when there was no exception
        self.do_remove_dump(filename)

        # if told to do so, now vacuum analyze the database
        vacuum_timing = self.vacuumdb()

        # source the extra SQL files
        for sql in self.psql_source_files(utils.POST_SQL):
            if VERBOSE:
                print "psql -f %s" % sql

        return self.wget_timing, secs, vacuum_timing

    def load(self, filename):
        """ will pg_restore from the already present dumpfile and determine
        the backup date from the file, which isn't removed """
        from options import VERBOSE, TERSE, DEBUG

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
                              self.schemas,
                              self.schemas_nodata)
        r.createdb(self.db_encoding)
        self.pgbouncer_add_database()

        # now restore the dump
        try:
            if VERBOSE:
                os.system("ls -l %s" % filename)
            
            r.restore_jobs = self.restore_jobs
            secs = r.pg_restore(filename, self.get_nodata_tables())

        except Exception, e:
            if DEBUG:
                raise
            mesg  = "Error: couldn't pg_restore from '%s'" % (filename)
            mesg += "\nDetail: %s" % e
            raise PGRestoreFailedException, mesg

        # set the database search_path if non default
        self.set_database_search_path()

        return secs

    def dump(self, filename, force = False):
        """ launch a pg_restore for the current staging configuration """
        from options import VERBOSE, TERSE

        # first attempt to establish the connection to remote server
        # no need to fetch the big backup file unless this succeed
        #
        # we will restore from pgbouncer connection, first connection is
        # made to the maintenance database
        r = restore.pgrestore(self.dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore)

        fullname = os.path.join(self.tmpdir, filename)

        try:
            secs = r.pg_dump(fullname, force = force)
        except ExportFileAlreadyExistsException:
            print "Error: dump file '%s' already exists" % fullname
            return None

        if not TERSE:
            os.system('ls -lh %s' % fullname)
        
        return secs

    def pitr(self, target, value):
        """ launch a Point In Time Recovery """
        import datetime
        from options import VERBOSE

        if self.base_backup_cmd is None or self.wal_archive_cmd is None \
           or self.pitr_basedir is None:
            raise Exception, "Error: please configure PITR"
        
        cluster = datetime.date.today().isoformat().replace('-', '')
        cluster = os.path.join(self.pitr_basedir, cluster)

        if VERBOSE:
            print "pitr:  target  %s [%s]" % (value, target)
            print "pitr:    base  %s" % self.base_backup_cmd
            print "pitr:     wal  %s" % self.wal_archive_cmd
            print "pitr: basedir  %s" % cluster

        client_args = [cluster, self.base_backup_cmd, self.wal_archive_cmd]
        if target:
            client_args += [target, value]

        utils.run_client_script(self.host, client_args, self.use_sudo)

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

    def pgbouncer_add_database(self, dbname = None):
        """ edit pgbouncer configuration file to add a database """
        from options import TERSE

        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.postgres_port)

        if dbname is None:
            dbname = self.dated_dbname

        newconffile = p.add_database(dbname, self.postgres_port)

        self.pgbouncer_update_conf(newconffile)

        if not TERSE:
            print "added a pgbouncer database %s" % dbname

    def pgbouncer_del_database(self, dbname):
        """ edit pgbouncer configuration file to add a database """
        from options import VERBOSE
        
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.postgres_port)

        newconffile = p.del_database(dbname)

        self.pgbouncer_update_conf(newconffile)

        if VERBOSE:
            print "deleted a pgbouncer database %s" % dbname

    def pgbouncer_update_conf(self, newconffile):
        """ reconfigure targeted pgbouncer with given file """
        import os.path
        from options import VERBOSE, TERSE, CLIENT_SCRIPT

        baseconfdir = os.path.dirname(self.pgbouncer_conf)

        # skip scp when target is localhost
        if self.host not in ("localhost", "127.0.0.1"):
            utils.scp(self.host, newconffile, '/tmp')
            
        utils.run_client_script(self.host,
                                ["pgbouncer", newconffile, self.pgbouncer_port],
                                self.use_sudo)

        # if target isn't localhost, rm the local temp file
        # when target host is localhost, we used mv already
        if self.host not in ("localhost", "127.0.0.1"):
            os.unlink(newconffile)
                
    def drop(self, dbname = None):
        """ drop the given database: dbname_%(backup_date) """
        from options import TERSE
        
        if dbname is None:
            dbname = self.dated_dbname
        
        r = restore.pgrestore(dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)

        # pause the database beforehand
        self.pgbouncer_pause(dbname)

        # and dropdb now that there's no more connection to it
        try:
            r.dropdb()
        except psycopg2.ProgrammingError, e:
            # database still is in pgbouncer setup but has already been
            # dropped, database %%% does not exist error
            if not TERSE:
                print "Cleaning up pgbouncer for non-existing database %s" \
                      % dbname

        # and remove it from pgbouncer configuration
        self.pgbouncer_del_database(dbname)

    def purge(self):
        """ keep only self.keep_bases databases """
        from options import TERSE, VERBOSE
        
        dlist = [d[1].strip('"')
                 for d in self.pgbouncer_databases()
                 if d[0].strip('"') != self.dbname \
                 and d[1].strip('"').startswith(self.dbname)]
        dlist.sort()

        if len(dlist) <= self.keep_bases:
            if not TERSE:
                print "cleandb: we keep %d databases and have only %d" %\
                      (self.keep_bases, len(dlist)) \
                      + ", skipping" 
                
            return

        for d in dlist[:-2]:
            self.drop(d)
        return

    def vacuumdb(self):
        """ run VACUUM VERBOSE ANALYZE on the database """
        r = restore.pgrestore(self.dated_dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)

        return r.vacuumdb()


    def dbsize(self, dbname = None):
        """ return database size, pretty printed """
        if dbname is not None:
            dated_dbname = dbname
        else:
            dated_dbname = self.dated_dbname
            
        r = restore.pgrestore(dated_dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)

        size, pretty = r.dbsize()
        return dated_dbname, size, pretty

    def dbsizes(self):
        """ generate database name, sizes for all databases in the section """
        import psycopg2
        from options import VERBOSE
        
        for name, database, host, port in self.pgbouncer_databases():
            if name != self.dbname and name.find(self.dbname) == 0:
                try:
                    n, size, pretty = self.dbsize(name)
                    yield name, size, pretty
                except psycopg2.ProgrammingError, e:
                    # database does not exists
                    if VERBOSE:
                        print "%s does not exists" % name
                    yield name, -1, -1

        return

    def pg_size_pretty(self, size):
        """ return the size, pretty printed """
        r = restore.pgrestore(self.dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)

        return r.pg_size_pretty(size)

    def psql_connect(self):
        """ connect to the given database """
        r = restore.pgrestore(self.dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore)

        # no file name to psql_source_file means sys.stdin
        return r.psql_source_file()

    def psql_source_files(self, phase):
        """ connect to the given database and run some scripts """
        from options import VERBOSE, TERSE
        if not self.sql_path:
            if not TERSE:
                print "There's no custom SQL file to load"
            return

        if phase == utils.POST_SQL:
            sql_path = os.path.join(self.sql_path, 'post')
            
        elif phase == utils.PRE_SQL:
            sql_path = os.path.join(self.sql_path, 'pre')

        else:
            raise Exception, "INTERNAL: psql_source_files is given unknown phase"

        if not os.path.isdir(sql_path):
            if VERBOSE:
                print "skipping '%s' which is not a directory" % sql_path
            return
        
        r = restore.pgrestore(self.dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major,
                              self.pg_restore)

        filenames = [x
                     for x in os.listdir(sql_path)
                     if len(x) > 4 and x[-4:] == '.sql']
        filenames.sort()

        for filename in filenames:
            yield filename
            out = r.psql_source_file(os.path.join(sql_path, filename))

            if VERBOSE:
                print out

        return 

    def show(self, setting):
        """ return setting value """
        r = restore.pgrestore(self.dbname,
                              self.dbuser,
                              self.host,
                              self.pgbouncer_port,
                              self.dbowner,
                              self.maintdb,
                              self.postgres_major)

        return r.show(setting)

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
        from options import VERBOSE
        if VERBOSE:
            print "pause %s;" % dbname
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.pgbouncer_port)

        p.pause(dbname)


    def pgbouncer_resume(self, dbname):
        """ resume given database """
        from options import TERSE
        if not TERSE:
            print "resume %s;" % dbname
        p = pgbouncer.pgbouncer(self.pgbouncer_conf,
                                self.dbuser,
                                self.host,
                                self.pgbouncer_port)

        p.resume(dbname)

    def set_database_search_path(self):
        """ set search path """

        if self.search_path:
            r = restore.pgrestore(self.dated_dbname,
                                  self.dbuser,
                                  self.host,
                                  self.pgbouncer_port,
                                  self.dbowner,
                                  self.maintdb,
                                  self.postgres_major)

            r.set_database_search_path(self.search_path)

    def prepare_then_run_londiste(self):
        """ prepare .ini files for all concerned providers """
        if self.replication:
            l = londiste.londiste(self.replication, self.section,
                                  self.dbname, self.dated_dbname,
                                  self.tmpdir, clean = True)

            # first the tickers
            for t, host in l.tickers():
                filename = t.write()
                if filename:
                    t.start(host, filename, self.use_sudo)
                    yield t.section, filename

            # now the londiste daemons
            for p, host in l.providers():
                filename = l.write(p)
                l.start(p, host, filename, self.use_sudo)
                
                yield p, filename
                
        return


    def control_service(self, service, action):
        """ remotely start/stop/restart/status a service, using CLIENT_SCRIPT """
        from options import VERBOSE

        if service not in ('londiste', 'ticker', 'pgbouncer'):
            raise Exception, "Error: unknown service '%s'" % service
        
        args = [action, service]

        if service in ('londiste', 'ticker') and not self.replication:
            raise Exception, "Error: no replication in your setup"

        if service == 'pgbouncer':
            bargs = args + [self.pgbouncer_port]
            out   = utils.run_client_script(self.host, bargs, self.use_sudo)

            if (action == 'status' or VERBOSE) and out:
                print out

            return

        if service == 'londiste':
            l = londiste.londiste(self.replication, self.section,
                                  self.dbname, self.dated_dbname,
                                  self.tmpdir, clean = True)

            for t, host in l.tickers():
                targs = [ action, 'ticker',
                          os.path.basename( t.get_config_filename() ) ]
                out   = utils.run_client_script(host, targs, self.use_sudo)

                if (action == 'status' or VERBOSE) and out:
                    print out

            for p, host in l.providers():
                pargs = args + [ p, os.path.basename( l.get_config_filename(p) ) ]
                out   = utils.run_client_script(host, pargs, self.use_sudo)

                if (action == 'status' or VERBOSE) and out:
                    print out

            return

        if service == 'ticker':
            l = londiste.londiste(self.replication, self.section,
                                  self.dbname, self.dated_dbname,
                                  self.tmpdir, clean = True)

            for t, host in l.tickers():
                targs = args + [ os.path.basename( t.get_config_filename() ) ]
                out   = utils.run_client_script(self.host, targs, self.use_sudo)

                if (action == 'status' or VERBOSE) and out:
                    print out

        return
