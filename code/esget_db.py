from __future__ import print_function
import numpy as np
#import apsw
import sqlite3
import os
from time import time
import cPickle
import datetime
import pandas as pd
import shutil
import logging
import glob
import re


class EsgetDB(object):
    '''Handles database-operations for esget'''

    def __init__(self, config):
        self.dbname = config.get('Paths', 'dbname')
        self.fileroot = config.get('DEFAULT', 'fileroot')
        self.storageroot = config.get('DEFAULT', 'storageroot')
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
        conn = sqlite3.connect(self.dbname)
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
            conn.commit()
            conn.close()
        elif reset:
            self._log.info("Resetting database {0}".format(self.dbname))
            conn, c = self.connect()
            c.execute('DROP TABLE IF EXISTS localfiles')
            c.execute('DROP TABLE IF EXISTS esgffiles')
            self._mktable("localfiles", c)
            self._mktable("esgffiles", c)
            conn.commit()
            conn.close()
        else:
            self._log.info(("Database {0} exists and is not to be " +
                            "reset - doing nothing").format(self.dbname))

    def insert_files(self, recordlist):
        '''Indiscrimantly inserts local files. Should only be used for
        initial population of the DB'''

        # Modify the filename to be relative to <storageroot>
        for firec in recordlist:
            newname = self.storpath2rempath(firec['filename'], inv=True)
            firec['filename'] = newname
        istr0 = self.pathfields.extend(
            ['filename', 'size', 'md5', 'sha256', 'mtime', 'unlink_date'])
        insertstring = '(:'+', :'.join(istr0)+')'

        conn, c = self.connect()
        self._log.info("Inserting {0} records".format(len(recordlist)))
        t0 = time()
        for allfields in recordlist:
            c.execute("INSERT INTO localfiles VALUES {0}"
                      .format(insertstring), allfields)
        self._log.info("Finished inserting: {0} seconds".
                       format(round(time() - t0)))
        conn.commit()
        conn.close()

    def update_files(self, recordlist):
        '''inserts new files into table "localfiles"'''

        # Modify the filename to be relative to <storageroot>
        for firec in recordlist:
            newname = self.storpath2rempath(firec['filename'], inv=True)
            firec['filename'] = newname
        istr0 = self.pathfields + ['filename', 'size', 'md5',
                                   'sha256', 'mtime', 'unlink_date']
        insertstring = '(:'+', :'.join(istr0)+')'

        conn, c = self.connect()
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
        conn.commit()
        conn.close()

    def clear_esgffiles(self):
        ''' removes all records from table esgffiles'''
        self._log.info("clearing esgffiles")
        conn, c = self.connect()
        exists = c.execute('''SELECT name from sqlite_master
        WHERE type='table' AND name='esgffiles' ''').fetchall()
        if exists:
            c.execute('''DELETE from esgffiles''')
        else:
            self._mktable("esgffiles", c)
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
        conn.commit()
        conn.close()
        filelist = glob.glob(os.path.join(self.tmpdir, "queryres*.cpy"))
        for f in filelist:
            os.remove(f)
        return()

    def mark_have(self):
        '''
        Marks files that are already in localfiles, excluding those that
        are unlinked.
        '''
        conn, c = self.connect()
        self._log.info("Marking the files we already have.")
        execstring = '''UPDATE esgffiles SET have=1
        WHERE esgffiles.checksum IN (
        SELECT checksum from esgffiles INNER JOIN localfiles ON
        (esgffiles.checksum = localfiles.sha256 OR
        esgffiles.checksum = localfiles.md5) AND localfiles.unlink_date == "0"
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
        c.execute('''UPDATE localfiles SET unlink_date = :unlink_date WHERE
        (sha256 NOT IN
        (
        SELECT esgffiles.checksum FROM localfiles INNER JOIN esgffiles
        ON  esgffiles.checksum = localfiles.sha256
        )
        OR md5 NOT IN
        (
        SELECT esgffiles.checksum FROM localfiles INNER JOIN esgffiles
        ON esgffiles.checksum = localfiles.md5
        )) AND unlink_date=="0"''', {"unlink_date": curdate})
        if self.no_chksum_action == "keep":
            c.execute('''UPDATE localfiles SET unlink_date=0
            WHERE md5 IS NULL AND sha256 IS NULL''')
            self._log.info("Marking localfiles without checksums" +
                           " as \"not unlink\"")
        count = c.execute(
            '''SELECT COUNT (*) FROM
            (SELECT * from localfiles
            WHERE unlink_date == :unlink_date)
            ''', {"unlink_date": curdate}).fetchall()[0][0]
        conn.commit()
        conn.close()
        self._log.info("{0} files marked as \"unlink\"".format(count))
        return(curdate)

    def storpath2rempath(self, path, intermed='', inv=False):
        """If inv=False, takes a filepath relative to an arbitrary machine
        (e.g. the storage-machine) and returns path relative to
        <self.fileroot>, i.e. the relative to the
        execute-machine. <intermed> is put in between the constant
        tail and the changed head of the path.

        Else if inv == True, takes a filepath relative to an arbitrary
        machine (e.g. a "worker"-machine) and returns path relative to
        <self.storageroot>, the convention used to store file-location
        in the database-field "filename".

        """

        if not inv:
            retpath = os.path.normpath(path.replace(
                self.storageroot, os.path.join(self.fileroot, intermed)))
        else:
            retpath = os.path.normpath(path.replace(
                self.fileroot, os.path.join(self.storageroot, intermed)))
        return(retpath)

    def get_unlink_files(self, unlink_date):
        conn, c = self.connect()
        self._log.info("Getting files to be unlinked")
        ulfiles = c.execute(
            '''SELECT filename FROM localfiles
            WHERE unlink_date == :unlink_date 
            ''', {"unlink_date": unlink_date}).fetchall()
        ulfiles = [self.storpath2rempath(x[0]) for x in ulfiles]
        conn.close()
        return(ulfiles)

    def get_download_url(self):
        '''Returns checksum, size and http-url from esgffiles for
        files that should have been downloaded / are to be downloaded'''
        # refactor this with get_download_size()
        self._log.info("Getting list of urls of files to download")
        conn, c = self.connect()
        dlurls = c.execute('''SELECT title, size, url_http,
                                     checksum_type, checksum
                              FROM esgffiles
                              WHERE have=0''').fetchall()
        conn.commit()
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

        setloc = np.array([max(x) for x in resloc])
        setesgf = np.array([x[0] for x in resesgf])
        failedidx = np.logical_not(np.in1d(setesgf, setloc))
        failedfiles = setesgf[failedidx]
        ffstring = "('"+"','".join(failedfiles) + "')"
        c.execute('''INSERT OR REPLACE INTO failed
        SELECT * FROM esgffiles
        WHERE checksum IN {0}'''.format(ffstring))
        no_failed = c.execute('''SELECT COUNT(*) FROM failed''').fetchall()
        no_failed = no_failed[0][0]
        self._log.info("No failed downloads: {0}".format(no_failed))
        conn.close()
        return(no_failed)

    def relink_update(self, unlinkdate):
        '''Changes paths into relink-subtree to path into main tree and
        removes unlink-flag for files that were unlinked at unlinkdate'''
        conn, c = self.connect()
        # change filename
        self._log.info(("Changing path for relinked ({})" +
                        " files").format(unlinkdate))
        execstring = '''SELECT filename, rowid FROM localfiles
        WHERE unlink_date="{0}"'''.format(unlinkdate)
        files = c.execute(execstring).fetchall()
        newfnames = [x[0].replace("unlinked/{}/"
                                  .format(unlinkdate), "", 1) for x in files]
        rowids = [x[1] for x in files]
        for rowid in enumerate(rowids):
            execstring = '''UPDATE localfiles
            SET filename="{0}"
            WHERE rowid={1}'''.format(newfnames[rowid[0]], rowid[1])
            c.execute(execstring)
        # change unlink_date
        self._log.info(("Changing unlinked = {}" +
                        " to unlinked = '0'.").format(unlinkdate))
        execstring = '''UPDATE localfiles SET unlink_date='0'
        WHERE unlink_date = '{}' '''.format(unlinkdate)
        c.execute(execstring)
        conn.commit()
        conn.close()

    def check_localfiles(self):
        '''Check whether all files in localfiles are actually there.
        Returns filenames in localfiles that are missing'''
        self._log.info("Checking localfiles for existence ...")
        nofound = []
        conn, c = self.connect()
        nofiles = c.execute("SELECT count(*) FROM localfiles").fetchall()[0][0]
        i = 0
        for fn, unlink_date in c.execute(
                '''SELECT filename,unlink_date FROM localfiles'''):
            # # This not necessary anymore since only "true" paths
            # # to be stored as "filename".
            # if unlink_date != "0":
            #     intermed = "unlinked/{0}".format(unlink_date) 
            # else:
            #     intermed = ""
            intermed = ""
            fnrem = self.storpath2rempath(fn, intermed=intermed)
            i += 1
            print("Completed: {0}%".format(round(100*float(i)/nofiles), 2),
                  end="\r")
            if not os.path.isfile(fnrem):
                self._log.info("File not found: {0}".format(fnrem))
                nofound.append((fn, fnrem))
        conn.close()
        self._log.info("Found {0} missing files.".format(len(nofound)))
        return(nofound)

    def remove_unlinked(self):
        '''Removes database-entries in "localfiles" that
        refer to "unlinked" files.'''
        self._log.info("removing unlinked files from database")
        conn, c = self.connect()
        c.execute('''DELETE FROM localfiles
        WHERE unlink_date!="0"''')
        conn.close()
        return()
        
    def remove_nofound(self):
        '''Removes files from database that are not found on disk'''
        nofound = self.check_localfiles()
        conn, c = self.connect()
        for fn in nofound:
            fn = self.storpath2rempath(fn, inv=True)
            self._log.info("Removing file {0}".format(fn))
            c.execute('''DELETE FROM localfiles
            WHERE filename=:fn''', {"fn": fn})
        conn.close()
        
    def find_relinkable(self):
        '''
        Finds previously unlinked files that are now in table esgffiles again.
        Deletes those entries from localfiles.
        Returns paths.
        '''
        self._log.info("searching re-appeared files in unlink-tree")
        conn, c = self.connect()
        esgfsums = c.execute('SELECT checksum FROM esgffiles').fetchall()
        unlinksums = c.execute('''SELECT md5, sha256 FROM localfiles
        WHERE unlink_date != "0"''').fetchall()
        esgfsums = np.array([x[0] for x in esgfsums])
        unlinksums = np.array([max(x) for x in unlinksums])
        relinkidx = np.in1d(unlinksums, esgfsums)
        relinksums = unlinksums[relinkidx]
        self._log.info("found {} files for re-linking".format(len(relinksums)))
        relinksumssql = "('"+"','".join(relinksums) + "')"
        relinkfiles = c.execute('''SELECT filename FROM localfiles
        WHERE sha256 IN {} OR md5 IN  {}'''
                            .format(relinksumssql, relinksumssql)).fetchall()
        relinkfiles = [x[0] for x in relinkfiles]
        self._log.info("Deleting {} files from the unlink-tree."
                       .format(len(relinkfiles)))
        c.execute('''DELETE FROM localfiles WHERE sha256 IN {} OR md5 IN  {}'''
                  .format(relinksumssql, relinksumssql))
        c.close()
        return(relinkfiles)


    def change_path(self, pathchanges):
        """Takes a list of pairs (<original path>, <new path>)
        and changes the "files"-field in localfiles accordingly"""
        newpaths = [x[1] for x in pathchanges]
        # extract the "unlinked/<unlink_date>/"-part from pathchanges[1]
        intermeds = [re.search("unlinked/.*?/", x).group()
                     for x in newpaths]
        origstorpaths = [self.storpath2rempath(x[0], inv=True)
                         for x in pathchanges]
        newstorpaths = [self.storpath2rempath(x, inv=True)
                        for x in newpaths]
        pathchanges = zip(origstorpaths, newstorpaths)
 
        self._log.info("Updating {0} filenames".format(len(pathchanges)))
        conn, c = self.connect()
        for p in pathchanges:
            c.execute('''UPDATE localfiles
            SET filename = '{1}'
            WHERE filename = '{0}' '''.format(p[0], p[1]))
        conn.commit()
        conn.close()
        
