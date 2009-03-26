##
## pg_restore support class
##

import os, psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from options import NotYetImplementedException
from options import CouldNotConnectPostgreSQLException
from options import CreatedbFailedException
from options import PGRestoreFailedException

class pgrestore:
    """ Will launch correct pgrestore binary to restore a dump file to some
    remote database, which we have to create first """

    def __init__(self, dbname, user, host, port, owner, maintdb, major,
                 restore_cmd = "/usr/bin/pg_restore", st = True,
                 schemas = [],
                 connect = True):
        """ dump is a filename """
        from options import VERBOSE

        self.dbname      = dbname
        self.user        = user
        self.host        = host
        self.port        = int(port)
        self.owner       = owner
        self.maintdb     = maintdb
        self.major       = major
        self.restore_cmd = restore_cmd
        self.st          = st
        self.schemas     = schemas

        self.dsn    = "dbname='%s' user='%s' host='%s' port=%d" \
                      % (self.maintdb, self.user, self.host, self.port)
        self.mconn  = None

        if not connect:
            return

        try:
            self.mconn = psycopg2.connect(self.dsn)
        except Exception, e:
            mesg  = "Error: could not connect to server '%s'" % host
            mesg += "\nDetail: %s" % e
            mesg += "\nHint: Following command might help to debug:"
            mesg += "\n  psql -U %s -h %s -p %s %s " \
                    % (user, host, port, self.maintdb)
            raise CouldNotConnectPostgreSQLException, mesg

        if VERBOSE:
            print "Connected to %s" % self.dsn

    def __del__(self):
        """ destructor, close the PG connection """
        if self.mconn is not None:
            self.mconn.close()
            self.mconn = None

    def createdb(self):
        """ connect to remote PostgreSQL server to create the new database"""
        from options import VERBOSE, TERSE
        
        if VERBOSE:
            print "createdb -O %s %s" % (self.owner, self.dbname)

        try:
            # create database can't run from within a transaction
            self.mconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.mconn.cursor()
            curs.execute('CREATE DATABASE "%s" WITH OWNER "%s"' \
                         % (self.dbname, self.owner))
            curs.close()
        except Exception, e:
            mesg = 'Error: createdb: %s' % e
            raise CreatedbFailedException, mesg

        if not TERSE:
            print "created database '%s' owned by '%s'" % (self.dbname,
                                                           self.owner)

    def dropdb(self):
        """ connect to remote PostgreSQL server to drop database"""
        from options import VERBOSE, TERSE

        if VERBOSE:
            print "dropdb %s" % self.dbname

        try:
            # drop database can't run from within a transaction
            self.mconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.mconn.cursor()
            curs.execute('DROP DATABASE "%s"' % self.dbname)
            curs.close()
        except Exception, e:
            raise

        if not TERSE:
            print 'droped database "%s"' % self.dbname

    def create_schemas(self):
        """ Create the database needed schemas """
        from options import VERBOSE, TERSE
        if not TERSE:
            print "Create Schemas %s" % ' '.join(self.schemas)

        try:
            dsn  = "dbname='%s' user='%s' host='%s' port=%d" \
                          % (self.dbname, self.user, self.host, self.port)
            conn = psycopg2.connect(dsn)
            curs = conn.cursor()
            
            for s in self.schemas:
                # public will have been created at createdb time
                if s != 'public':

                    sql = 'CREATE SCHEMA "%s" AUTHORIZATION "%s";'\
                          % (s, self.owner)

                    if VERBOSE:
                        print " ", sql                        
                    curs.execute(sql)

            conn.commit()
            conn.close()
        except Exception, e:
            raise

    def pg_restore(self, filename):
        """ restore dump file to new database """
        from options import VERBOSE, TERSE

        if VERBOSE:
            os.system("ls -l %s" % self.restore_cmd)
            if self.schemas:
                print "Restoring only schemas:", self.schemas

            # now is a good time to create those schemas
            self.create_schemas()

        # Single Transaction?
        st = ""
        if self.st:
            st = "-1"

        # Exclude some schemas at restore time?
        schemas = None
        if self.schemas:
            schemas = ' '.join(['-n "%s"' % x for x in self.schemas])

        ## cmd = "%s %s -h %s -p %d -U %s -d %s %s %s" \
        ##       % (pgr, st,
        ##          self.host, self.port, self.owner, self.dbname,
        ##          schemas, filename)

        cmd = [self.restore_cmd,
               st,
               "-h", self.host,
               "-p %d" % self.port,
               "-U", self.owner,
               "-d", self.dbname,
               schemas,
               filename
               ]
        cmd = [x for x in cmd if x is not None and x != '']

        if not TERSE:
            print " ".join(cmd)

        ## for some reason subprocess.Popen() is unable to see pg_restore
        ## stderr and return code
        ##
        out  = os.popen(" ".join(cmd))
        line = 'stupid init value'
        while line != '':
            line = out.readline()
            if VERBOSE:
                print line[:-1]

        returncode = out.close()

        if VERBOSE:
            print "pg_restore return:", returncode

        if returncode != 0:
            mesg = "pg_restore returned %d" % returncode
            raise PGRestoreFailedException, mesg

    def get_catalog(self, filename, tables):
        """ return the backup catalog, pg_restore -l, commenting table data """
        from options import VERBOSE

        cmd = [self.restore_cmd, "-l", filename]

        import subprocess
        proc = subprocess.Popen(cmd,
                                stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE)

        out, err = proc.communicate()

        if proc.returncode != 0:
            raise PGRestoreFailedException, err

        from cStringIO import StringIO
        catalog = StringIO()
        
        for line in out:
            match = False
            for s, t in [(x.split('.')[0], x.split('.')[1]) for x in tables]:
                if line.find('TABLE DATA %s %s' % (s, t)) > -1:
                    match = True

            if match:
                catalog.write(';%s' % line)
            else:
                catalog.write(line)

        return catalog

    def dbsize(self):
        """ return pretty printed dbsize """

        try:
            curs = self.mconn.cursor()
            curs.execute('SELECT pg_size_pretty(pg_database_size(%s));',
                         [self.dbname])

            dbsize = curs.fetchone()[0]            
            curs.close()
        except Exception, e:
            raise
        

        return dbsize
