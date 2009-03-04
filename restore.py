##
## pg_restore support class
##

class pgrestore:
    """ Will launch correct pgrestore binary to restore a dump file to some
    remote database, which we have to create first """

    def __init__(self, dbname, dump, user, host, port, owner):
        """ """
        self.dbname = dbname

    def createdb(self):
        """ connect to remote PostgreSQL server to create the new database"""
        pass

    def pg_restore(self):
        """ restore dump file to new database """
        pass

    
