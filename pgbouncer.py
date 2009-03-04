##
## Support for pgbouncer queries
##

class pgbouncer:
    """ PgBouncer class to get some data out of special SHOW commands """

    def __init__(self, user = 'postgres', host = '127.0.0.1', port = 6000):
        """ pgbouncer instance init """
        self.user = user
        self.host = host
        self.port = port
        self.dbname = 'pgbouncer'

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

    def pause(self, dbname = None):
        """ pause a database, or all of them """
        pass

    

    
