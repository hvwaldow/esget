""" esget_fs implements functions that operate mainly in the directory tree
where ESGET data is stored. Used to find files, examine paths, and move and
rename files."""

import logging
import datetime
import os
import re
import hashlib
import shutil
import glob
from time import ctime


class EsgetFS(object):
    '''Handles filesystem operations inside the
    ESGET storage directory-tree'''

    def __init__(self, config):
        self.fileroot = config.get('DEFAULT', 'fileroot')
        self.storageroot = config.get('DEFAULT', 'storageroot')
        self._log = logging.getLogger(self.__class__.__name__)
        self.pathfields = config.get('DEFAULT', 'pathfields').split(',')
        self.dbname = config.get('Paths', 'dbname')
        self.wget_tmp_dir = config.get('Paths', 'wget_dir')
        self.wget_log_dir = config.get('Paths', 'wget_log_dir')
        self.tmpdir = config.get('Paths', 'tmpdir')
        self.logfile = config.get('Paths', 'logfile')
        self.publicdbpath = os.path.join(self.fileroot,
                                         config.get('Tuning', 'publicdbdir'),
                                         os.path.basename(self.dbname))
        self.no_storagefiles = config.get('Tuning',
                                          'no_storagefiles').split(',')

    def path2unlinkpath(self, path, unlinkroot):
        """Returns the full unlink-path for a file originally
        at <path> with the new root <unlinkroot>"""

        def _splitpath(path, pathlist):
            """Splits a path recursively into a list of path components"""
            if path in ('/', ''):
                return(pathlist[-1::-1])
            else:
                path, tail = os.path.split(path)
                pathlist.append(tail)
                return(_splitpath(path, pathlist))

        elements = _splitpath(path, [])
        tailpath = os.path.join(*elements[len(elements) -
                                          len(self.pathfields) - 1:])
        unlinkpath = os.path.join(unlinkroot, tailpath)
        return(unlinkpath)

    def unlink(self, ulfiles):
        '''Moves files to be unliked into special subdirectory'''
        success = []
        if len(ulfiles) == 0:
            self._log.info("No files to unlink")
            return()
        curdate = datetime.date.today().isoformat()
        unlinkpath = os.path.join(self.fileroot, "unlinked", "{0}"
                                  .format(curdate))
        os.umask(0022)
        if not os.path.exists(unlinkpath):
            self._log.info("Making unlink - directory {0}".format(unlinkpath))
            os.makedirs(unlinkpath, 0755)
        self._log.info("Moving {0} files into {1}"
                       .format(len(ulfiles), unlinkpath))
        for f in ulfiles:
            fulpath = self.path2unlinkpath(f, unlinkpath)
            try:
                os.renames(f, fulpath)
                success.append((f, fulpath))
                self._log.info("Unlinked {0}.".format(f))
            except:
                self._log.error("Could not unlink {0}.".format(f))
        return(success)

    def rm_files(self, filelist):
        '''
        Removes files from storage. Mainly used to get rid of previously
        unlinked files that are now to be downloaded again.
        '''
        filelist = [x.replace(self.storageroot, self.fileroot, 1)
                    for x in filelist]
        for f in filelist:
            self._log.info("Removing {0}".format(f))
            os.remove(f)

    def relink(self, unlinkdir):
        '''Moves previously unliked files back into main storage-tree'''
        unlinkpath = os.path.join(self.fileroot, "unlinked", unlinkdir)
        if not os.path.exists(unlinkpath):
            self._log.error(("Unlink-directory {0} does not exists. " +
                             "Doing nothing.").format(unlinkpath))
            return()

        def split_path(path):
            ''' Recursive function to create a list of path elements'''
            folders = list()
            if path == '' or path == '/':
                return([path])
            else:
                rest, folder = os.path.split(path)
                folders = split_path(rest)
                folders.append(folder)
                return(folders)

        def mkdestpath(sourcepathlist, unlinkdir):
            destpathlist = [x for x in sourcepathlist
                            if x not in ['unlinked', unlinkdir]]
            return(os.path.join(*destpathlist))

        for root, dirs, files in os.walk(unlinkpath):
            sources = [os.path.join(root, f) for f in files]
            sourceelements = [split_path(x) for x in sources]
            destpaths = [mkdestpath(x, unlinkdir) for x in sourceelements]
            sourcedest = zip(sources, destpaths)
            for s, d in sourcedest:
                if not os.path.exists(os.path.dirname(d)):
                    os.makedirs(os.path.dirname(d))
                    self._log.info("Created directory {0}"
                                   .format(os.path.dirname(d)))
                shutil.move(s, d)
                self._log.info("Moved {0} to {1}".format(s, d))
        # Check whether the whole unlink tree is empty:
        allfiles = list()
        for root, dirs, files in os.walk(unlinkpath):
            allfiles.extend(files)
        if len(allfiles) > 0:
            self._log.warning(("Files remain that were not moved. " +
                               "Not deleting unlink-directory {0}")
                              .format(unlinkdir))
            print(allfiles)
            return(1)
        else:
            self._log.info("Removing unlink-directory {0}".format(unlinkdir))
            shutil.rmtree(unlinkpath)
        return(0)

    def _calc_checksum(self, fn, chksumtype):
        self._log.info("{0} : Calculating checksum".format(fn))
        f = open(fn, "rb")
        fcontent = f.read()
        f.close()
        chksum = {'md5': hashlib.md5, 'sha256': hashlib.sha256}\
                 [chksumtype.lower()](fcontent).hexdigest()
        self._log.info("Checksum {0} {1}".format(chksumtype, chksum))
        return(chksum)

    def _move_file(self, storepath, dlpath):
        self._log.info("Moving to {0}".format(storepath))
        if not os.path.exists(storepath):
            self._log.info("Makinf directory: {0}".format(storepath))
            os.makedirs(storepath)
        try:
            shutil.copy(dlpath, storepath)
        except:
            self._log.error(("Problem copying file\n" +
                             "from: {0}\n" +
                             "to: {1}. " +
                             "Leaving untuched")
                            .format(dlpath, storepath))
            return()
        os.remove(dlpath)
        return()

    def check_dl(self, esgetdb_obj):
        ''' checks downloads (exist, checksum comparison), constructs path
        for storgae-tree, moves files from download- to storage-tree,
        updates table "localfiles" of database'''

        # DEBUG
        # fundo_move = open("/home/hvwaldow/scripts/esg/esgdl/log/undo_move.tmp", "w")
        selectfields = self.pathfields + \
            ['checksum', 'checksum_type', 'url_http']
        selectstr = ','.join(selectfields)
        conn, c = esgetdb_obj.connect()
        csel = c.execute('''SELECT {0}
                            FROM esgffiles
                            WHERE have=0'''.format(selectstr))
        recordlist = list()
        for fil in csel:
            filevals = dict(zip(selectfields, fil))
            nonefields = [x[0] for x in filevals.items() if x[1] is None]
            filevals.update(dict(zip(nonefields,
                                     ['UNSPECIFIED']*len(nonefields))))
            dlpath = os.path.join(self.fileroot, 'download',
                                  re.sub('^http://.+?/(.+?\.nc)\|.*$', '\g<1>',
                                         filevals['url_http']))
            storelist = [filevals[x] for x in self.pathfields]
            storepath = os.path.join(self.fileroot, *storelist)
            basename = os.path.basename(dlpath)
            fullstorepath = os.path.join(storepath, basename)
            # DEBUG
            # fundo_move.write("mv {0} {1}\n"
            #                  .format(fullstorepath, os.path.dirname(dlpath)))
            if not os.path.isfile(dlpath):
                self._log.info("{0} : Not found".format(dlpath))
                continue
            if filevals['checksum'] != "UNSPECIFIED":
                chkondisk = self._calc_checksum(dlpath, filevals['checksum_type'])
                if chkondisk == filevals['checksum']:
                    self._log.info("{0} : OK".format(dlpath))
                    self._move_file(storepath, dlpath)
                else:
                    self._log.error("{0} : Wrong checksum. Deleting!"
                                    .format(dlpath))
                    os.remove(dlpath)
                    continue
            else:
                self._log.warning(("{0} : No remote checksum available." +
                                   "Checking size > 0").format(dlpath))
                if os.path.getsize(dlpath) > 0:
                    self._log.info("Size > 0. OK")
                    self._move_file(storepath, dlpath)
                else:
                    self._log.info("{0} : size = 0. Deleting".format(dlpath))
                    os.remove(dlpath)
                    continue
            # append to recordlist
            filevals['filename'] = fullstorepath
            filevals['size'] = os.path.getsize(fullstorepath)
            if ((filevals['checksum_type'] is not None) and
                (filevals['checksum'] is not None)):
                filevals[filevals['checksum_type'].lower()] = filevals['checksum']
            if 'sha256' not in filevals:
                filevals['sha256'] = None
            if 'md5' not in filevals:
                filevals['md5'] = None
            filevals['mtime'] = ctime(os.path.getmtime(storepath))
            filevals['unlink_date'] = 0
            recordlist.append(filevals)

        esgetdb_obj.close_connection(conn)
        # update database table localfiles
        esgetdb_obj.update_files(recordlist)
        # DEBUG
        # fundo_move.close()

    def finish_cycle(self):
        ''' Stuff to do when one cyle is complete'''

        for root, dirs, files in os.walk(self.fileroot, topdown=True):
            dirs = [x for x in dirs if x not in self.no_storagefiles]
            files = [x for x in files if x not in self.no_storagefiles]
            for d in dirs:
                os.chmod(os.path.join(root, d), 0755)
            for f in files:
                os.chmod(os.path.join(root, f), 0644)
        # copy database
        if not os.path.isdir(os.path.dirname(self.publicdbpath)):
            self._log.info("Creating public database directory {0}"
                           .format(os.path.dirname(self.publicdbpath)))
            os.mkdir(os.path.dirname(self.publicdbpath))
        shutil.copy(self.dbname, self.publicdbpath)
        os.chmod(self.publicdbpath, 0644)

    def cleanup(self, cleanlog=False):
        '''Cleans tmp - directory and wget-log'''
        self._log.info("Cleaning up tmp directory")
        for f in glob.glob(os.path.join(self.wget_tmp_dir, "*")):
            os.remove(f)
        for f in glob.glob(os.path.join(self.tmpdir, "job*")):
            os.remove(f)
        self._log.info("Cleaning {0}".format(self.wget_log_dir))
        for f in glob.glob(os.path.join(self.wget_log_dir, "*")):
            os.remove(f)
        if cleanlog:
            self._log.info("Removing logfile {0}".format(self.logfile))
            os.remove(self.logfile)
