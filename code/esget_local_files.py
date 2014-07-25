import numpy as np
import os
import hashlib
import sys
import time
import cPickle
import logging
import multiprocessing as mp

sys.setrecursionlimit(10000)


def mk_all_fields(fn):
    ''' Collects filesystem info for specific file and calculates checksums.
    This function outside a class because used in multiprocessing.'''

    f = open(fn, "rb")
    fcontent = f.read()
    f.close()
    md5 = hashlib.md5(fcontent).hexdigest()
    sha256 = hashlib.sha256(fcontent).hexdigest()
    size = os.path.getsize(fn)
    mtime = time.ctime(os.path.getmtime(fn))
    allfields = dict([('size', size), ('md5', md5), ('sha256', sha256),
                      ('mtime', mtime), ('filename', fn), ('unlink_date', 0)])
    return(allfields)


class LocalFiles(object):
    ''' Handles information gathering from the local filesystem'''

    def __init__(self, config):
        self.fileroot = config.get('DEFAULT', 'fileroot')
        self.no_storagefiles = config.get('Tuning', 'no_storagefiles').split(',')
        subdirs = os.walk(self.fileroot, topdown=True).next()[1]
        self.domains = [x for x in subdirs if x not in self.no_storagefiles]
        self.local_fs_record = config.get('Paths', 'local_fs_record')
        self.pathfields = config.get('DEFAULT', 'pathfields').split(',')
        self._log = logging.getLogger(self.__class__.__name__)
        self.paths = []
        self.recordlist = []

    def mkfilelist(self):
        ''' Finds all files in self.domains below self.fileroot'''

        paths = list()
        self._log.info("scanning filesystem below " + self.fileroot)
        for d in self.domains:
            dpaths = list()
            self._log.info("scanning domain {0}".format(d))
            root = os.path.join(self.fileroot, d)
            for dirpath, dirs, files in os.walk(root):
                if len(files) > 0:
                    if len(dirs) > 0:
                        self._log.error("Something wrong with the directory" +
                                        " structure:\n{0} contains files" +
                                        " and directories".format(dirpath))
                    dpaths.extend([os.path.join(dirpath, fi) for fi in files])
            self._log.info("Found {0} files in domain {1}"
                           .format(len(dpaths), d))
            paths.extend(dpaths)
        self.paths = paths
        self._log.info("Found {0} files in total.".format(len(paths)))

    def estimate_scanning_time(self, samplesize, chunksize=None):
        ''' Estimates time necessary to extract all info about
        local files and calculates good chunksize for pool.map'''

        #  preliminary results on atmos indicate that
        #  chunksizes resulting in using 2/3 of available cores are good.
        if not chunksize:
            chunksize = samplesize / (mp.cpu_count()*2/3)
        if len(self.paths) < 1:
            self._log.error("No paths collected - You either forgot " +
                            "to run mkfilelist first, or the " +
                            "storage-tree {0} is empty."
                            .format(self.fileroot))
            return(0)

        if len(self.paths) < samplesize:
            self._log.error(("Number of paths ({0}) must be greater " +
                            "than samplesize ({1}).")
                            .format(len(self.paths), samplesize))
            return(0)
        self._log.info("Collecting information and calculating " +
                       "checksums for {0} files".format(samplesize))
        self._log.info("Chunksize = {0}".format(chunksize))
        self._log.info("Estimating time needed ...")
        t0 = time.time()
        pool = mp.Pool()
        recordlist = pool.map(mk_all_fields, self.paths[0:samplesize],
                              chunksize=chunksize)
        t1 = time.time() - t0
        t_est = t1 / samplesize * len(self.paths) / 60.0 / 60.0
        self._log.info("Estimated time necessary: {0:.2f} hours."
                       .format(t_est))
        return(0)

    def scan_filesystem(self):
        '''Scans the filesystem and records file-info and checksums.
        Writes an intermediate results to local_fs_record_file if it
        is not None'''
        if len(self.paths) < 1:
            self._log.error("No paths collected - You either forgot " +
                            "to run mkfilelist first, or the " +
                            "storage-tree {0} is empty."
                            .format(self.fileroot))

        chunksize = int(np.ceil(len(self.paths) / (mp.cpu_count()*2/3)))
        self._log.info("Scanning {0} files ...".format(len(self.paths)))
        self._log.info("Number of cores: {0}, chunksize: {1}"
                       .format(mp.cpu_count(), chunksize))
        t0 = time.time()
        pool = mp.Pool()
        self.recordlist = pool.map(mk_all_fields,
                                   self.paths, chunksize=chunksize)
        cPickle.dump(self.recordlist,
                     open(self.local_fs_record, "w"), protocol=2)
        t1 = (time.time() - t0) / 60.0 / 60.0
        self._log.info("Scanning of {0} files finished in {1:.2f} hours"
                       .format(len(self.paths), t1))

    def mk_fullrecordlist(self):
        '''Expands each filename of recordlist into fields
        associated with the filepath. If class attribute "recordlist"
        (which is usually provided by scan.filesystem()) is empty,
        an attempt is made to read the file <local_fs_record>'''

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

        if len(self.recordlist) == 0:
            self._log.warning("Recordlist empty, trying to read from file")
            self.get_rec_fromfile()
            self._log.info("Got {0} local file paths from file"
                           .format(len(self.recordlist)))
        for f in self.recordlist:
            dirpath = os.path.dirname(f['filename'])
            pathvalues = split_path(dirpath)
            pathvalues = pathvalues[-len(self.pathfields):]
            f.update(dict(zip(self.pathfields, pathvalues)))

    def get_rec_fromfile(self):
        ''' Read recordlist (with 6 elements) from intermediate file.
        Used for debugging'''

        try:
            fh = open(self.local_fs_record, "r")
            self.recordlist = cPickle.load(fh)
            fh.close()
        except:
            self._log.error("Could not read {0}. Aborting"
                            .format(self.local_fs_record))
            sys.exit(1)

    def rm_tmp_recordlist(self):
        ''' Removes temporary file holding the result of the local scan'''
        os.remove(self.local_fs_record)
        self._log.info("Removed local scan result {0}"
                       .format(self.local_fs_record))
