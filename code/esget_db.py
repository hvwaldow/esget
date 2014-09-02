import apsw
import os
from time import time
import cPickle
import datetime
import pandas as pd
import shutil
import logging
import glob


class EsgetDB(object):
    '''Handles database-operations for esget'''

    def __init__(self, config):
        self.dbname = config.get('Paths', 'dbname')
        self.dbarchivedir = config.get('Paths', 'dbarchivedir')
        self._log = logging.getLogger(self.__class__.__name__)

        self.tmpdir = config.get('Paths', 'tmpdir')
        self.pathfields = config.get('DEFAULT', 'pathfields').split(',')
        self.no_chksum_action = config.get('Tuning', 'no_chksum_action')
        self._esgffields = config.get('DEFAULT', 'esgffields').split(',')

    def _mk_createstring(self, table):
        ''' table is either "esgffiles" or "localfiles"'''

        if table == "localfiles":
            s0 = "CREATE TABLE localfiles ("
            s1 = zip(self.pathfields, ["TEXT"]*len(self.pathfields))
            s1 = ','.join([' '.join(x) for x in s1])
            s = s0 + s1 + ",filename TEXT,size INTEGER,md5 TEXT," +\
                "sha256 TEXT,mtime TEXT,unlink_date TEXT)"
            return(s)
        elif table == "esgffiles":
            s0 = "CREATE TABLE esgffiles ("
            s1 = zip(self._esgffields, ["TEXT"]*len(self._esgffields))
            s1 = ','.join([' '.join(x) for x in s1])
            s = s0 + s1 + ",url_http TEXT,url_gsiftp TEXT," +\
                "have INTEGER)"
            return(s)
        else:
            raise AssertionError('no valid table specified')
        return()

    def connect(self):
        conn = apsw.Connection(self.dbname)
        c = conn.cursor()
        return((conn, c))

    def _mktable(self, table, c):
        self._log.info("Creating table {0}".format(table))
        c.execute(self._mk_createstring(table))
        allfields = c.execute("PRAGMA table_info({0})".
                              format(table)).fetchall()
        allfields = [x[1] for x in allfields]
        self._log.info("Creating indices on table {0}".format(table))
        for idx_col in allfields:
            c.execute("CREATE INDEX IF NOT EXISTS {0} ON {2} ({1})".
                      format("idx_"+table+"_"+idx_col, idx_col, table))

    def init_db(self, reset=False):
        '''Initiate database with empty tables "localfiles" and "esgffiles".
        Does nothing if tables already exist, unless reset=True'''
        if not os.path.isfile(self.dbname):
            self._log.info("Creating initial database: {0}".format(self.dbname))
            conn, c = self.connect()
            self._mktable("localfiles", c)
            self._mktable("esgffiles", c)
            conn.close()
        elif reset:
            self._log.info("Resetting database {0}".format(self.dbname))
            conn, c = self.connect()
            c.execute('DROP TABLE IF EXISTS localfiles')
            c.execute('DROP TABLE IF EXISTS esgffiles')
            self._mktable("localfiles", c)
            self._mktable("esgffiles", c)
            conn.close()
        else:
            self._log.info(("Database {0} exists and is not to be " +
                            "reset - doing nothing").format(self.dbname))

    def insert_files(self, recordlist):
        '''Indiscrimantly inserts local files. Should only be used for
        initial population of the DB'''

        istr0 = self.pathfields.extend(
            ['filename', 'size', 'md5', 'sha256', 'mtime', 'unlink_date'])
        insertstring = '(:'+', :'.join(istr0)+')'

        conn, c = self.connect()
        c.execute("BEGIN TRANSACTION")
        self._log.info("Inserting {0} records".format(len(recordlist)))
        t0 = time()
        for allfields in recordlist:
            c.execute("INSERT INTO localfiles VALUES {0}"
                      .format(insertstring), allfields)
        self._log.info("Finished inserting: {0} seconds".
                       format(round(time() - t0)))
        c.execute("COMMIT")
        conn.close()

    def update_files(self, recordlist):
        ''' inserts new files into table "localfiles"'''

        istr0 = self.pathfields + ['filename', 'size', 'md5',
                                   'sha256', 'mtime', 'unlink_date']
        insertstring = '(:'+', :'.join(istr0)+')'

        conn, c = self.connect()
        c.execute("BEGIN TRANSACTION")
        self._log.info("Updateing {0} records".format(len(recordlist)))
        t0 = time()
        for allfields in recordlist:
            has = c.execute('''SELECT 1 from localfiles
                               WHERE sha256 = :sha256 OR md5 = :md5''',
                            allfields).fetchall()
            if has:
                self._log.warning("Trying to insert file we already have: " +
                                  allfields['filename'])
                continue
            else:
                c.execute("INSERT INTO localfiles VALUES {0}"
                          .format(insertstring), allfields)
        self._log.info("Finished updating: {0} seconds".
                       format(round(time() - t0)))
        c.execute("COMMIT")
        conn.close()

    def clear_esgffiles(self):
        ''' removes all records from table esgffiles'''
        self._log.info("clearing esgffiles")
        conn, c = self.connect()
        c.execute('''DELETE from esgffiles''')
        conn.close()

    def sanitize_esgfrecord(self, allfields):
        '''makes a record from ESGF_search digestible for DB'''

        allfields_san = dict()
        for x in self._esgffields:
            try:
                if type(allfields[x]) == list:
                    allfields_san[x] = allfields[x][0]
                else:
                    allfields_san[x] = allfields[x]
            except KeyError as err:
                allfields_san[err.message] = None
        allfields_san["url_http"] = allfields["url"][0]
        if len(allfields["url"]) > 1:
            allfields_san["url_gsiftp"] = allfields["url"][1]
        else:
            allfields_san["url_gsiftp"] = None
        allfields_san["have"] = 0
        return(allfields_san)

    def update_esgffiles(self):
        self.clear_esgffiles()
        conn, c = self.connect()
        insertstring = "INSERT INTO esgffiles VALUES (" +\
                       ", ".join([":" + x for x in self._esgffields]) +\
                       ", :url_http, :url_gsiftp, :have)"
        c.execute('BEGIN TRANSACTION')
        for f in glob.glob(os.path.join(self.tmpdir, "queryres*.cpy")):
            self._log.info("Updating esgffiles from {0}"
                           .format(f))
            esgfres = cPickle.load(open(f, "rb"))
            esgfres = esgfres["result"]["response"]["docs"]
            for allfields in esgfres:
                allfields = self.sanitize_esgfrecord(allfields)
                self._log.info("Inserting {0}"
                               .format(allfields["master_id"]))
                c.execute(insertstring, allfields)
        c.execute('COMMIT')
        conn.close()
        filelist = glob.glob(os.path.join(self.tmpdir, "queryres*.cpy"))
        for f in filelist:
            os.remove(f)
        return()

    def mark_have(self):
        conn, c = self.connect()
        self._log.info("Marking the files we already have.")
        execstring = '''UPDATE esgffiles SET have=1
        WHERE esgffiles.checksum IN (
        SELECT checksum from esgffiles INNER JOIN localfiles ON
        esgffiles.checksum = localfiles.sha256 OR
        esgffiles.checksum = localfiles.md5
        )'''
        c.execute(execstring)
        if self.no_chksum_action == "keep":
            c.execute('''UPDATE esgffiles SET have=1
            WHERE checksum IS NULL''')
            self._log.info("Marking files without checksum as \"have\"")
        count = c.execute('''SELECT COUNT (*) FROM
        (SELECT * FROM esgffiles WHERE have = 1)''').fetchall()[0][0]
        conn.close()
        self._log.info("{0} files marked as \"have\"".format(count))

    def mark_unlink(self):
        conn, c = self.connect()
        curdate = datetime.date.today().isoformat()
        self._log.info("Marking files to unlink")
        c.execute("BEGIN TRANSACTION")
        c.execute('''UPDATE localfiles SET unlink_date = :unlink_date
        WHERE rowid NOT IN (
        SELECT localfiles.rowid FROM localfiles INNER JOIN esgffiles
        ON  esgffiles.checksum = localfiles.sha256 OR
        esgffiles.checksum = localfiles.md5)''', {"unlink_date": curdate})
        if self.no_chksum_action == "keep":
            c.execute('''UPDATE localfiles SET unlink_date=0
            WHERE md5 IS NULL AND sha256 IS NULL''')
            self._log.info("Marking localfiles without checksums" +
                           " as \"not unlink\"")
        count = c.execute('''SELECT COUNT (*) FROM
        (SELECT * from localfiles
        WHERE unlink_date != 0)''').fetchall()[0][0]
        c.execute("COMMIT")
        conn.close()
        self._log.info("{0} files marked as \"unlink\"".format(count))

    def get_unlink_files(self):
        conn, c = self.connect()
        self._log.info("Getting files to be unlinked")
        ulfiles = c.execute('''SELECT filename FROM localfiles
                            WHERE unlink_date != 0''').fetchall()
        ulfiles = [x[0] for x in ulfiles]
        conn.close()
        return(ulfiles)

    def get_download_url(self):
        '''Returns checksum, size and http-url from esgffiles for
        files that should have been downloaded / are to be downloaded'''
        # refactor this with get_download_size()
        self._log.info("Getting list of urls of files to download")
        conn, c = self.connect()
        c.execute("BEGIN TRANSACTION")
        dlurls = c.execute('''SELECT title, size, url_http,
                                     checksum_type, checksum
                              FROM esgffiles
                              WHERE have=0''').fetchall()
        c.execute('COMMIT')
        conn.close()
        if len(dlurls) == 0:
            self._log.info("No files to download")
            dlurls = pd.DataFrame(columns=['title', 'size', 'url_http',
                                           'checksum_type', 'checksum'])
        else:
            dlurls = pd.DataFrame(dlurls)
            dlurls.columns = ['title', 'size', 'url_http',
                              'checksum_type', 'checksum']
            dlurls['size'] = dlurls['size'].astype(float)
            self._log.info("Got table of DataFrame of files to download: {0}".
                           format(dlurls.shape))
        return(dlurls)

    def close_connection(self, conn):
        conn.close()

    def backup_db(self):
        '''Backup/archive esget.db'''
        arcname = os.path.join(self.dbarchivedir,
                               os.path.split(self.dbname)[1] +
                               "."+time.strftime("%Y-%m-%d_%H-%M"))
        shutil.copy(self.dbname, arcname)
        self._log.info("Archived {0}".format(self.dbname))
        return()

    def get_no_localfiles(self):
        '''Returns number of entries in localfiles'''
        conn, c = self.connect()
        no_localfiles = c.execute("SELECT COUNT() FROM localfiles").fetchall()
        return(no_localfiles[0][0])

    def mk_failed_table(self):
        def read_chksums(c, table):
            selstring = 'SELECT checksum, checksum_type FROM esgffiles' \
                        if table == 'esgffiles' \
                        else 'SELECT md5, sha256 FROM localfiles'
            chk_esgf = c.execute(selstring).fetchall()
            return(chk_esgf)
        conn, c = self.connect()
        self._log.info("Making table of failed downloads")
        resloc = read_chksums(c, 'localfiles')
        self._log.info("localfiles has {0} entries.".format(len(resloc)))
        resesgf = read_chksums(c, 'esgffiles')
        self._log.info("esgffiles has {0} entries.".format(len(resesgf)))
        c.execute('''DROP TABLE IF EXISTS failed''')
        c.execute(''' CREATE TABLE failed AS
        SELECT * FROM esgffiles WHERE 1=2''')
        allfields = c.execute("PRAGMA table_info(failed)").fetchall()
        allfields = [x[1] for x in allfields]
        for idx_col in allfields:
            c.execute("CREATE INDEX IF NOT EXISTS {0} ON {2} ({1})".
                      format("idx_failed_"+idx_col, idx_col, "failed"))
        setloc = [max(x) for x in resloc]
        setesgf = [x[0] for x in resesgf]
        failedfiles = [x for x in setesgf if x not in setloc]
        ffstring = "('"+"','".join(failedfiles) + "')"
        c.execute('''INSERT OR REPLACE INTO failed
        SELECT * FROM esgffiles
        WHERE checksum IN {0}'''.format(ffstring))
        no_failed = c.execute('''SELECT COUNT(*) FROM failed''').fetchall()
        no_failed = no_failed[0][0]
        self._log.info("No failed downloads: {0}".format(no_failed))
        conn.close()
        return(no_failed)
