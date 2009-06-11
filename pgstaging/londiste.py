## Support for Skytools londiste (and pgqadm) .ini generation and daemon
## launch
##
import os, os.path, ConfigParser
import utils
from utils import UnknownOptionException, UnknownSectionException
from utils import NotYetImplementedException, SubprocessException

class londiste:
    """ Prepare londiste setup from a central INI file, for a database """

    def __init__(self, config, section, dbname, dbname_instance,
                 tmpdir, clean = False):
        """ londiste init, config is a ConfigParser object  """
        self.config   = config
        self.section  = section
        self.dbname   = dbname
        self.instance = dbname_instance
        self.dbdate   = self.instance.split('_')[-1]
        self.tmpdir   = "%s/%s" % (tmpdir, self.section)

        if clean:
            self.clean(ignore = True)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)

        # each pg_staging.ini section will potentially be refered to by more
        # than one replication provider, so we keep a dict of them
        #
        # ini_files[ provider_section_name ] = ConfigParser()
        self.ini_files = {}

    def providers(self):
        """ list all replications sections where provider == self.dbname """
        for s in self.config.sections():
            if self.config.has_option(s, 'provider'):
                if self.config.get(s, 'provider') == self.section:
                    yield s, self.config.get(s, 'host')
        return

    def subscribers(self):
        """ list all replications sections where subscriber == self.dbname """
        for s in self.config.sections():
            if self.config.has_option(s, 'subscriber'):
                if self.config.get(s, 'subscriber') == self.section:
                    yield s, self.config.get(s, 'host')
        return

    def tickers(self, unique = True):
        """ list all tickers daemon we'll need for this section/dbname """

        tickers = []
        
        for p, host in self.providers():
            if not self.config.has_option(p, 'ticker'):
                mesg = "Replication section '%s' has no 'ticker' option" % p
                raise UnknownOptionException, mesg
            
            t = self.config.get(p, 'ticker')

            if not self.config.has_section(t):
                mesg = "Replication section '%s' does not exists" % t
                raise UnknownSectionException, mesg

            if not self.config.has_option(t, 'job_name'):
                mesg = "Replication Section '%s' has no 'job_name' option" % t
                raise UnknownOptionException, mesg

            if not self.config.has_option(t, 'host'):
                mesg = "Replication Section '%s' has no 'host' option" % t
                raise UnknownOptionException, mesg

            if unique and t in tickers:
                continue

            tickers += [t]
            # the host we return is the host where to run the ticker
            pgq = pgqadm(self.config, t, self.dbname, self.instance, self.tmpdir)
            yield pgq, self.config.get(t, 'host')
            
        return

    def job_name(self, provider):
        """ return the londiste subscriber job_name, must be unique """
        return provider

    def clean_provides(self, value):
        """ return a well formatted list of tables from the ConfigParser value"""
        s = set()
        l = value.split('\n')

        # l could contain table\ntable, split on \n now
        for entry in l:
            for e in entry.split(' '):
                s = s.union([e])

        # check that tables are all schema qualified
        for t in s:
            if len(t) < 4 or t.find('.') == -1:
                mesg = "malformed table name in section %s: '%s'" % (s, t)
                raise Exception, mesg

        return s

    def get_nodata_tables(self):
        """ return a list of tables to avoid restoring """
        # we avoid restoring tables which we are a replication subscriber of
        tables = set()

        for s, host in self.subscribers():
            p = self.clean_provides( self.config.get(s, 'provides') )
            tables = tables.union( s )
                        
        return tables

    def prepare_config(self, provider):
        """ prepare self.londiste_ini to host needed setup """

        self.ini_files[provider] = ConfigParser.SafeConfigParser()
        ini = self.ini_files[provider]

        if provider not in [p for p, host in self.providers()]:
            mesg = "Can't prepare replication for unknown provider " + \
                   "'%s' of '%s'" % (provider, self.dbname)
            raise UnknownSectionException, mesg

        ini.add_section('londiste')
        ini.set('londiste', 'job_name', self.job_name(provider))
        ini.set('londiste', 'pgq_queue_name',
                self.config.get(provider, 'pgq_queue_name'))
        ini.set('londiste', 'pgq_lazy_fetch',
                self.config.defaults()['pgq_lazy_fetch'])
        ini.set('londiste', 'pidfile',
                '/var/run/londiste/%(job_name)s.' + '%s.pid' % self.dbdate)
        ini.set('londiste', 'logfile',
                '/var/log/londiste/%(job_name)s.' + '%s.log' % self.dbdate)
        ini.set('londiste', 'loop_delay', '1.0')
        ini.set('londiste', 'connection_lifetime', '30')

        pdb = self.config.get(provider, 'provider_db')
        sdb = self.config.get(provider, 'subscriber_db')
        ini.set('londiste', 'provider_db',
                pdb.replace(self.dbname, self.instance))
        ini.set('londiste', 'subscriber_db',
                sdb.replace(self.dbname, self.instance))

        return ini

    def get_config_filename(self, provider, conf = None):
        """ return the filename where to store the configuration """
        if conf is None:
            conf = self.prepare_config(provider)

        basename = conf.get('londiste', 'pgq_queue_name').replace('_', '-')
        filename = "%s/%s.%s.ini" % (self.tmpdir, basename, self.dbdate)

        return filename
    
    def write(self, provider, conf = None):
        """ write out computed londiste INI to a file """
        from options import VERBOSE

        if conf is None:
            conf = self.prepare_config(provider)
        filename = self.get_config_filename(provider, conf)
        
        fd = open(filename, "wb")
        conf.write(fd)
        fd.close()

        return filename

    def init_remote(self, provider, host, filename, use_sudo):
        """ send the londiste file for provider to the remote host """
        from options import VERBOSE
        
        utils.scp(host, filename, '/tmp')

        remote_filename = os.path.basename(filename)
        tables = self.clean_provides( self.config.get(provider, 'provides') )
        args   = ['init-londiste', provider, remote_filename] + tables
        out    = utils.run_client_script(host, args, use_sudo)

        if VERBOSE:
            print out
        
        return remote_filename

    def start(self, provider, host, filename, use_sudo, init = True):
        """ start the ticker daemon on remote host """
        from options import VERBOSE

        if init:
            filename = self.init_remote(provider, host, filename, use_sudo)

        args = ['replay', provider, filename]
        out  = utils.run_client_script(host, args, use_sudo)

        if VERBOSE:
            print out

        return 

    def clean(self, ignore = False):
        """ rm -rf self.tmpdir """
        from options import VERBOSE

        if not os.path.isdir(self.tmpdir):
            if ignore:
                return

            mesg = "no such directory: '%s'" % self.tmpdir
            raise StagingRuntimeException, mesg

        for name in os.listdir(self.tmpdir):
            fullname = os.path.join(self.tmpdir, name)
            if VERBOSE:
                print "rm %s" % fullname
            os.unlink(fullname)

        if VERBOSE:
            print "rmdir %s" % self.tmpdir
        os.rmdir(self.tmpdir)

class pgqadm:
    """ Prepare PGQ ticker setup from a central INI file, for a database """

    def __init__(self, config, pgq_section, dbname, dbname_instance, tmpdir):
        """ pgq init, config is a ConfigParser object  """
        self.config   = config
        self.section  = pgq_section
        self.dbname   = dbname
        self.instance = dbname_instance
        self.dbdate   = self.instance.split('_')[-1]
        self.tmpdir   = tmpdir
        self.pgqadm   = ConfigParser.SafeConfigParser()

    def get_config_filename(self, conf = None):
        """ return the filename where to put the configuration by default """
        if conf is None:
            conf = self.prepare_config()
            
        basename = conf.get('pgqadm', 'job_name').replace('_', '-')
        filename = "%s/ticker.%s.%s.ini" % (self.tmpdir, basename, self.dbdate)

        return filename

    def prepare_config(self):
        """ prepare self.pgqadm to host needed setup """

        ini = self.pgqadm

        ini.add_section('pgqadm')
        ini.set('pgqadm', 'job_name',
                self.config.get(self.section, 'job_name'))
        ini.set('pgqadm', 'maint_delay_min', '600')
        ini.set('pgqadm', 'loop_delay', '1.0')
        ini.set('pgqadm', 'use_skylog', '0')
        ini.set('pgqadm', 'connection_lifetime', '21')
        ini.set('pgqadm', 'pidfile',
                '/var/run/londiste/ticker.%(job_name)s.' + '%s.pid'%self.dbdate)
        ini.set('pgqadm', 'logfile',
                '/var/log/londiste/ticker.%(job_name)s.' + '%s.log'%self.dbdate)

        db = self.config.get(self.section, 'db')
        ini.set('pgqadm', 'db', db.replace(self.dbname, self.instance))
        return ini

    def write(self, conf = None):
        """ write out computed pgqadm INI to a file """
        import os.path
        from options import VERBOSE

        if conf is None:
            conf = self.prepare_config()
        filename = self.get_config_filename(conf)

        if os.path.exists(filename):
            return None

        ## if conf is None:
        ##     conf = self.prepare_config()

        fd = open(filename, "wb")
        conf.write(fd)
        fd.close()

        return filename

    def init_remote(self, host, filename, use_sudo):
        """ send the pgqadm file to the remote host """
        from options import VERBOSE
        
        utils.scp(host, filename, '/tmp')
        remote_filename = os.path.basename(filename)

        args = ['init-pgq', remote_filename]
        out  = utils.run_client_script(host, args, use_sudo)

        if VERBOSE:
            print out

        return remote_filename

    def start(self, host, filename, use_sudo, init = True):
        """ start the ticker daemon on remote host """
        from options import VERBOSE

        if init:
            filename = self.init_remote(host, filename, use_sudo)

        args = ['ticker', filename]
        out  = utils.run_client_script(host, args, use_sudo)

        if VERBOSE:
            print out

        return 
        
