##
## pg_restore support class
##

import psycopg2
from options import VERBOSE, DRY_RUN
from options import NotYetImplementedException
from options import CouldNotConnectPostgreSQLException

class pgrestore:
    """ Will launch correct pgrestore binary to restore a dump file to some
    remote database, which we have to create first """

    def __init__(self, dbname, user, host, port, owner):
        """ dump is a filename """
        self.dbname = dbname
        self.user   = user
        self.host   = host
        self.port   = int(port)
        self.owner  = owner

        self.dsn    = "dbname='%s' user='%s' host='%s' port=%d" \
                      % (self.dbname, self.user, self.host, self.port)
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

        if VERBOSE:
            print "createdb -O %s %s" % (self.owner, self.dbname)

        try:
            curs = self.conn.cursor()
            curs.execute("CREATE DATABASE %s WITH OWNER %s",
                         [self.dbname, self.owner])
            curs.close()
        except Exception, e:
            raise

        if VERBOSE:
            print "createdb ok"

    def dropdb(self):
        """ connect to remote PostgreSQL server to drop database"""

        if VERBOSE:
            print "dropdb %s" % self.dbname

    def pg_restore(self, filename):
        """ restore dump file to new database """

        if VERBOSE:
            print "pg_restore < %s" % filename
