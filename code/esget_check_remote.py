"""
Created on Thu Apr  9 11:09:38 2015

@author: hvwaldow
"""
import logging
import os
import re
import sys
import subprocess as sp
import esget_logger
import unittest


class EsgetCheckRemote(object):
    '''
    checks whether remote accounts exist, can be logged into,
    have remote_wget_dir with proper permissions and creates
    subdirectories, if necessary.
    '''

    def __init__(self, config):
        self._log = logging.getLogger(self.__class__.__name__)
        self.hosts = self._gethosts(config)
        self.wget_remote_dir = config.get('DEFAULT', 'wget_remote_dir')
        self.remote_cert_dir = config.get('Paths', 'remote_cert_dir')
        self.remote_certificate = config.get('Paths', 'remote_certificate')
        self.remote_cookies = config.get('Paths', 'remote_cookies')

    def _gethosts(self, config):
        hostsections = [x for x in config.sections() if re.match('HOST_', x)]
        hosts = [config.get(x, 'hostname') for x in hostsections]
        return(hosts)

    def check_ssh(self, host):
        try:
            res = sp.check_call(['ssh', host, '-o ConnectTimeout=5',
                                 '-o BatchMode=yes', 'uname -a'])
        except:
            self._log.error("Can't reach host {} by passwordless login"
                            .format(host))
            sys.exit(1)
        return(res)

    def check_dir(self, host, d):
        proc = sp.Popen(['ssh', host, '-o', 'ConnectTimeout=5', '-o',
                         'BatchMode=yes',
                         'if [ -d {} ]; then echo "1"; else echo "0"; fi'
                         .format(d)],
                        bufsize=-1, stdout=sp.PIPE)
        proc.wait()
        ret = bool(int(proc.communicate(input=None)[0].strip()))
        return(ret)

    def mk_dir(self, host, d):
        sp.check_call(['ssh', host, '-o ConnectTimeout=5',
                       '-o BatchMode=yes', 'mkdir -p', d])

    def check_and_create(self, h):
        for d in [self.wget_remote_dir,
                  self.remote_cookies,
                  os.path.dirname(self.remote_certificate),
                  self.remote_cert_dir]:
            if not self.check_dir(h, d):
                self._log.info("{}:{} not found - creating"
                               .format(h, d))
                try:
                    self.mk_dir(h, d)
                except Exception as e:
                    self._log.error("Failed to create {}:{} - aborting!"
                                    .format(h, d))
                    sys.exit(str(e))

    def do_checks(self):
        for h in self.hosts:
            self.check_ssh(h)
            self.check_and_create(h)


# TESTS
class EsgetCheckRemoteTests(unittest.TestCase):
    def setUp(self):
        CONFIGFILE = "cordex_eur_ALL.cfg"
        import ConfigParser
        config = ConfigParser.SafeConfigParser()
        configfile = os.path.join("../config", CONFIGFILE)
        config.read(configfile)
        config.set('Paths', 'logfile', '../log/debug.log')
        esget_logger.Logging(config)
        self.config = config
        self.cr = EsgetCheckRemote(self.config)

    def testGetHosts(self):
        self.failUnless(len(self.cr._gethosts(self.config)) == 4)
        self.failUnless(self.cr._gethosts(self.config) ==
                        ["bio.ethz.ch", "atmos.ethz.ch", "thermo.ethz.ch",
                         "litho.ethz.ch"])

    def testCheckConnect(self):
        self.failUnless(self.cr.check_ssh(self.cr.hosts[0]) == 0)
        with self.assertRaises(SystemExit) as se:
            self.cr.check_ssh("nonexistent.domain.xx")
        self.assertEqual(se.exception.code, 1)
        with self.assertRaises(SystemExit) as se:
            self.cr.check_ssh("thisuserisnot@atmos.ethz.ch")
        self.assertEqual(se.exception.code, 1)

    def testCheckDir(self):
        h = self.cr.hosts[0]
        self.assertFalse(self.cr.check_dir(h, "nonexistentdir"))
        self.assertTrue(self.cr.check_dir(h, "."))

    def testMkDir(self):
        h = self.cr.hosts[0]
        self.cr.mk_dir(h, 'a_very/new/directory')
        self.assertTrue(self.cr.check_dir(h, 'a_very/new/directory'))
        sp.check_call(['ssh', h, '-o ConnectTimeout=5',
                       '-o BatchMode=yes', 'rm -rf a_very/new/directory'])

    def testCheckandCreate(self):
        h = os.environ['HOST']
        self.cr.wget_remote_dir = 'nonexistent/new'
        self.cr.remote_cert_dir = 'nonexistent/new/globus/certificates'
        self.cr.remote_certificate = 'nonexistent/new/globus/credentials.pem'
        self.cr.remote_cookies = 'nonexistent/new/globus/cookies'
        self.cr.check_and_create(h)
        for d in [self.cr.wget_remote_dir, self.cr.remote_cookies,
                  os.path.dirname(self.cr.remote_certificate),
                  self.cr.remote_cert_dir]:
            self.assertTrue(self.cr.check_dir(h, d))
        sp.check_call(['ssh', h, '-o ConnectTimeout=5',
                       '-o BatchMode=yes', 'rm -rf nonexistent'])


if __name__ == '__main__':
    unittest.main()
