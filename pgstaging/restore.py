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
                 restore_cmd = "/usr/bin/pg_restore", st = False,
                 schemas = [], schemas_nodata = [],
                 connect = True):
        """ dump is a filename """
        from options import VERBOSE

        self.dbname         = dbname
        self.user           = user
        self.host           = host
        self.port           = int(port)
        self.owner          = owner
        self.maintdb        = maintdb
        self.major          = major
        self.restore_cmd    = restore_cmd
        self.st             = st
        self.schemas        = schemas
        self.schemas_nodata = schemas_nodata

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

    def source_sql_file(self, filename):
        """ load the given SQL file into the maintenance connection """
        from options import VERBOSE

        # we use the psql console in order to support extended commands
        cmd = "psql -U %s -h %s -p %s -f %s %s " \
              % (self.user, self.host, self.port, filename, self.maintdb)

        if VERBOSE:
            print cmd

        out  = os.popen(cmd)
        line = 'stupid init value'
        while line != '':
            line = out.readline()
            if VERBOSE:
                print line[:-1]

        returncode = out.close()
        return returncode

    def createdb(self, encoding):
        """ connect to remote PostgreSQL server to create the new database"""
        from options import VERBOSE, TERSE
        
        if VERBOSE:
            print "createdb -O %s -E %s %s" % (self.owner,
                                               encoding, self.dbname)

        try:
            # create database can't run from within a transaction
            self.mconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.mconn.cursor()
            curs.execute('CREATE DATABASE "%s" ' % self.dbname + \
                         'WITH OWNER "%s" ENCODING \'%s\'' % (self.owner,
                                                              encoding))
            curs.close()
        except Exception, e:
            mesg = 'Error: createdb: %s' % e
            raise CreatedbFailedException, mesg

        if not TERSE:
            print "created database '%s' owned by '%s', encoded in %s" \
                  % (self.dbname, self.owner, encoding)

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

    def pg_restore(self, filename, excluding_tables = []):
        """ restore dump file to new database """
        from options import VERBOSE, TERSE

        if VERBOSE:
            os.system("ls -l %s" % self.restore_cmd)
            if self.schemas:
                print "Restoring only schemas:", self.schemas

        # Single Transaction?
        st = ""
        if self.st:
            if VERBOSE:
                print "Notice: pg_restore will work in a single transaction"
            st = "-1"

        # Exclude some schemas at restore time?
        catalog = ""
        if self.schemas or self.schemas_nodata:
            catalog = "-L %s" % self.get_catalog(filename,
                                                 excluding_tables,
                                                 out_to_file = True)

        cmd = [self.restore_cmd,
               st,
               "-h", self.host,
               "-p %d" % self.port,
               "-U", self.owner,
               "-d", self.dbname,
               catalog,
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

        if returncode is not None and returncode != 0:
            mesg = "pg_restore returned %d" % returncode
            raise PGRestoreFailedException, mesg

    def get_catalog(self, filename, tables, out_to_file = False):
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

        # out is a simple string, so we split on \n or read one char at a
        # time in the loop, which isn't what we want
        #
        # here's what the DATA lines we're after look like:
        #
        #3; 2615 122814 SCHEMA - pgq postgres
        #6893; 0 0 ACL - pgq postgres
        #3385; 1259 123008 TABLE londiste subscriber_table payment
        #1206; 1247 123043 TYPE londiste ret_subscriber_table postgres
        #1118; 1247 122925 TYPE pgq ret_batch_event postgres
        #142; 1255 122813 FUNCTION public txid_visible_in_snapshot(bigint, txid_snapshot) postgres
        #70; 1255 1487229 FUNCTION public upper(ip4r) postgres
        #2526; 2617 1487283 OPERATOR public # postgres
        #2524; 2617 1487281 OPERATOR public & postgres
        #2647; 2616 1487309 OPERATOR CLASS public btree_ip4_ops postgres
        #3961; 2605 1487223 CAST pg_catalog CAST (cidr AS public.ip4r)
        #6662; 0 788811 TABLE DATA payment abocb_code payment
        #6663; 0 788819 TABLE DATA payment abocb_renew payment
        #6664; 0 788825 TABLE DATA payment acte_code payment
        #3380; 1259 122980 SEQUENCE londiste provider_seq_nr_seq payment
        #6904; 0 0 SEQUENCE OWNED BY londiste provider_seq_nr_seq payment
        #6905; 0 0 SEQUENCE SET londiste provider_seq_nr_seq payment
        #4301; 2604 122984 DEFAULT londiste nr payment
        #4656; 1259 56340 INDEX archives ap_rev_2004 webadmin
        #6236; 2620 15995620 TRIGGER jdb www_to_reporting_logger webadmin
        #6014; 2606 56535 FK CONSTRAINT archives rev_2001_id_compte_fkey webadmin

        # tables are schema.table, we want (schema, table)
        splitted_tables = [(x.split('.')[0], x.split('.')[1]) for x in tables]

        # for meta data (md_) commands, filter_out what's neither in schemas
        # nor in schemas_nodata
        md_schemas = self.schemas
        if self.schemas_nodata:
            md_schemas += self.schemas_nodata

        # schemas here are used to filter what to restore (values not in
        # self.schemas are filtered out)
        if md_schemas:
            md_schemas.append('pg_catalog')

        # which triggers calls which function (schema qualified) cache
        triggers = self.get_trigger_funcs(filename)
        
        for line in out.split('\n'):
            filter_out = False

            if line.find('SCHEMA') > -1            \
                   or line.find('ACL') > -1        \
                   or line.find('TABLE') > -1      \
                   or line.find('TYPE') > -1       \
                   or line.find('FUNCTION') > -1   \
                   or line.find('OPERATOR') > -1   \
                   or line.find('CAST') > -1       \
                   or line.find('TABLE DATA') > -1 \
                   or line.find('SEQUENCE') > -1   \
                   or line.find('VIEW') > -1       \
                   or line.find('COMMENT') > -1    \
                   or line.find('DEFAULT') > -1    \
                   or line.find('INDEX') > -1      \
                   or line.find('TRIGGER') > -1    \
                   or line.find('CONSTRAINT') > -1:

                try:
                    a, b, c, d = line.split()[3:7]

                    if a in ('ACL', 'SCHEMA'):
                        if b == '-':
                            schema = c
                        else:
                            schema = b

                    elif a == 'COMMENT':
                        if b == '-' and c == 'SCHEMA':
                            schema = d

                    elif b == 'CLASS':
                        schema = c
                        
                    elif b == 'DATA':
                        schema = c
                        table  = d

                    elif a == 'SEQUENCE':
                        if b == 'OWNED' and c == 'BY':
                            schema = d
                        elif b == 'SET':
                            schema = c
                        else:
                            schema = b

                    elif a == 'FK' and b == 'CONSTRAINT':
                        schema = c

                    else:
                        schema = b

                    # filter out ACL lines for schemas we want to exclude
                    if a == 'ACL' and b == '-' and c in self.schemas:
                        filter_out = True

                    # check self.schemas (contains data we want to restore)
                    if not filter_out and schema not in md_schemas:
                        filter_out = True

                    # check TRIGGER function dependancy
                    if not filter_out and a == 'TRIGGER':
                        # triggers[schema][trigger_name] = [f1, f2, f3]
                        if b in triggers and c in triggers[b]:
                            for f in triggers[b][c]:
                                s = f.split('.')[0]

                                if s not in self.schemas:
                                    filter_out = True
                                    break
                                
                    # filter out TABLE DATA section for schemas_nodata
                    if not filter_out and self.schemas_nodata is not None \
                           and a == 'TABLE' and b == 'DATA' \
                           and schema in self.schemas_nodata:
                        filter_out = True

                    # then tables
                    if not filter_out and a == 'TABLE' and b == 'DATA':
                        for s, t in splitted_tables:
                            if not filter_out and schema == s and table == t:
                                filter_out = True
                                
                except ValueError, e:
                    # unpack error, line won't match anything, don't filter
                    # out
                    pass

            # filter_out means we turn it into a comment 
            if filter_out:
                catalog.write(';%s\n' % line)
            else:
                catalog.write('%s\n' % line)

        if not out_to_file:
            return catalog

        import tempfile
        fd, realname = tempfile.mkstemp(prefix = '/tmp/staging.',
                                        suffix = '.catalog')

        if VERBOSE:
            print "Dumping filtered catalog to '%s'" % realname
        
        temp = os.fdopen(fd, "wb")
        temp.write(catalog.getvalue())
        temp.close()

        return realname
        
    ##
    # In the catalog, we have such TRIGGER lines:
    #
    # 6236; 2620 15995620 TRIGGER jdb www_to_reporting_logger webadmin
    #
    # The TRIGGER code could depend on a procedure hosted in a schema that
    # we filter out. In this case, we want to also filter out the TRIGGER
    # itself.
    #
    #CREATE TRIGGER www_to_reporting_logger
    #AFTER INSERT OR DELETE OR UPDATE ON daily_journal
    #FOR EACH ROW
    #EXECUTE PROCEDURE pgq.logtriga('www_to_reporting', 'kkvvvvvvvvv', 'jdb.daily_journal');
    #
    # get_trigger_funcs will return a dict of
    #  {'trigger_name': ['procedure']}

    def get_trigger_funcs(self, filename):
        """ return the backup catalog, pg_restore -l, commenting table data """
        from options import VERBOSE

        cmd = [self.restore_cmd, "-s", filename]

        import subprocess
        proc = subprocess.Popen(cmd,
                                stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE)

        out, err = proc.communicate()

        if proc.returncode != 0:
            raise PGRestoreFailedException, err

        # expressions we're searching
        set_search_path     = 'SET search_path = '
        set_search_path_l   = len(set_search_path)
        create_trigger      = 'CREATE TRIGGER'
        create_trigger_l    = len(create_trigger)
        execute_procedure   = 'EXECUTE PROCEDURE'
        execute_procedure_l = len(execute_procedure)
        returns_trigger     = 'RETURNS "trigger"'

        # parsing state and results
        triggers        = {}
        triggers_funcs  = {} # {func: schema} cache
        current_schema  = 'public'
        current_trigger = None

        for line in out.split('\n'):
            if line.find(set_search_path) > -1:
                current_schema = line[set_search_path_l:-1].split(', ')[0]

                if current_schema not in triggers:
                    triggers[current_schema] = {}

                # no need to search for CREATE TRIGGER here
                continue

            if line.find(create_trigger) > -1:
                current_trigger = line[create_trigger_l:].strip()

                if current_trigger not in triggers[current_schema]:
                    # add an empty procedures list
                    triggers[current_schema][current_trigger] = []
                
                continue

            if line.find(returns_trigger) > -1:
                # CREATE FUNCTION partition_board_log() RETURNS "trigger"
                pname = line.split()[2].strip('()')
                triggers_funcs[pname] = current_schema

            if current_trigger:
                start = line.find(execute_procedure)

                if start > -1:
                    start = start + execute_procedure_l
                    pname = line[start:line.find('(', start)].strip()

                    if pname.find('.') == -1:
                        # procedure name is NOT schema qualified
                        ## if pname in triggers_funcs:
                        ##     pname = '%s.%s' % (triggers_funcs[pname], pname)
                        ## else:
                        pname = '%s.%s' % (current_schema, pname)

                    if pname not in triggers[current_schema][current_trigger]:
                        triggers[current_schema][current_trigger].append(pname)
                
                if line.find(';') > -1:
                    current_trigger = None

        return triggers

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
