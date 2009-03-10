##
## Support for pgbouncer queries
##
##
## Old pgbouncer didn't support some settings needed to be able to connect
## with psycopg2 or other libpq based connection libs, so we resort to psql
## subprocess.
##
import os
from options import CouldNotGetPgBouncerConfigException

class pgbouncer:
    """ PgBouncer class to get some data out of special SHOW commands """

    def __init__(self, conffile, reload_cmd,
                 user = 'postgres', host = '127.0.0.1', port = 6432):
        """ pgbouncer instance init """
        self.conffile   = conffile
        self.reload_cmd = reload_cmd
        self.user       = user
        self.host       = host
        self.port       = port
        self.dbname     = 'pgbouncer'

    def get_data(self, command):
        """ get pgbouncer SHOW <command> data """
        alldata = []
        
        psql = 'psql -h %s -p %s -U %s %s -c "SHOW %s;" 2>/dev/null' \
                  % (self.host, self.port, self.user, self.dbname, command)

        i = 0
        
        out  = os.popen(psql)
        line = 'stupid init value'
        while line != '':
            line = out.readline()
            i += 1

            if i == 1:
                # header
                header = [col.strip() for col in line.split('|')]

            elif i == 2:
                # skip second line, full of ---
                continue

            elif line.strip() != '' and line[0] != '(':
                cols = [c.strip() for c in line.split('|')]
                data = {}

                k = 0
                for c in cols:
                    data[header[k]] = c
                    k += 1

                alldata.append(data)

        code = out.close()

        return alldata


    def stats(self):
        """ return stats """
        return self.get_data("STATS")

    def pools(self):
        """ return pools """
        return self.get_data("pools")

    def databases(self):
        """ return databases """
        return self.get_data("databases")

    def get_config(self):
        """ ssh host cat config """
        import subprocess

        
        cmd  = "ssh %s cat %s" % (self.host, self.conffile)
        proc = subprocess.Popen(cmd.split(" "),
                                stdout = subprocess.PIPE)

        out, err = proc.communicate()

        if proc.returncode != 0:
            raise CouldNotGetPgBouncerConfigException, "See previous output"

        return out

    def parse_config(self):
        """ return a ConfigParser object """
        import ConfigParser
        from cStringIO import StringIO

        buf = StringIO()
        buf.write(self.get_config())
        buf.seek(0)

        config = ConfigParser.SafeConfigParser()
        config.readfp(buf, self.conffile)

        return config

    def switch_to_database(self, dbname, real_dbname, pgport):
        """ edit config file to have dbname point to real_dbname, and return
        the temporary filename containing the new config"""
        from options import VERBOSE, TERSE

        conf = self.parse_config()
        
        if real_dbname not in conf.options('databases'):
            dsn = 'dbname="%s" port=%d' % (real_dbname, pgport)
            conf.set('databases', real_dbname, dsn)

        # now set the dbname to point to real_dbname
        dsn = 'dbname="%s" port=%d' % (real_dbname, pgport)
        conf.set('databases', dbname, dsn)

        import tempfile
        fd, realname = tempfile.mkstemp(prefix = '/tmp/pgbouncer.',
                                        suffix = '.ini')
        temp = os.fdopen(fd, "wb")
        conf.write(temp)        
        temp.close()

        if not TERSE:
            print "new pgbouncer.ini generated in '%s'" % realname

        return realname

if __name__ == '__main__':
    p = pgbouncer('/etc/pgbouncer/pgbouncer.ini',
                  'sudo /etc/init.d/pgbouncer/reload')
    print 'Pools', p.pools()

    print
    print 'Config:'
    print p.get_config()
