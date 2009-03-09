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

    def __init__(self, dbname, user, host, port, owner, maintdb, major):
        """ dump is a filename """
        from options import VERBOSE

        self.dbname  = dbname
        self.user    = user
        self.host    = host
        self.port    = int(port)
        self.owner   = owner
        self.maintdb = maintdb
        self.major   = major

        self.dsn    = "dbname='%s' user='%s' host='%s' port=%d" \
                      % (self.maintdb, self.user, self.host, self.port)
        self.conn   = None

        try:
            self.conn = psycopg2.connect(self.dsn)
        except Exception, e:
            mesg  = "Error: could not connect to server '%s'" % host
            mesg += "\nDetail: %s" % e
            mesg += "\nHint: Following command might help to debug:"
            mesg += "\n  psql -U %s -h %s -p %s %s " \
                    % (user, host, port, dbname)
            raise CouldNotConnectPostgreSQLException, mesg

        if VERBOSE:
            print "Connected to %s" % self.dsn

    def __del__(self):
        """ destructor, close the PG connection """
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def createdb(self):
        """ connect to remote PostgreSQL server to create the new database"""
        from options import VERBOSE
        
        if VERBOSE:
            print "createdb -O %s %s" % (self.owner, self.dbname)

        try:
            # create database can't run from within a transaction
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.conn.cursor()
            curs.execute('CREATE DATABASE "%s" WITH OWNER "%s"' \
                         % (self.dbname, self.owner))
            curs.close()
        except Exception, e:
            mesg = 'Error: createdb "%s": %s' % (self.dbname, e)
            raise CreatedbFailedException, mesg

        if VERBOSE:
            print "created database '%s' owned by '%s'" % (self.dbname,
                                                           self.owner)

    def dropdb(self):
        """ connect to remote PostgreSQL server to drop database"""
        from options import VERBOSE

        if VERBOSE:
            print "dropdb %s" % self.dbname

        try:
            # drop database can't run from within a transaction
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.conn.cursor()
            curs.execute('DROP DATABASE "%s"' % self.dbname)
            curs.close()
        except Exception, e:
            raise

        print 'droped database "%s"' % self.dbname

    def pg_restore(self, filename):
        """ restore dump file to new database """
        from options import VERBOSE

        pgr = "/usr/lib/postgresql/%s/bin/pg_restore" % self.major

        if VERBOSE:
            os.system("ls -l %s" % pgr)
        
        cmd = "%s -1 -U %s -d %s %s" \
              % (pgr, self.owner, self.dbname, filename)

        if VERBOSE:
            print cmd

        import subprocess
        code = subprocess.call(cmd.split(" "))

        ## out  = os.popen(cmd)
        ## line = 'stupid init value'
        ## while line != '':
        ##     line = out.readline()
        ##     # output what pg_restore has to say, don't forget to chop \n
        ##     print line[:-1]
        ##
        ## code = out.close()

        if code != 0:
            raise PGRestoreFailedException, "See previous output"
