# This helps fixing files left in download, because they existed already in
# the storage tree.
# The bug causing that situation is now fixed.


import os
import sys
os.chdir("/home/hvwaldow/esget/code/misc")
sys.path.append("..")
import cPickle
import hashlib
import numpy as np
import time
import multiprocessing as mp
import json
import ConfigParser
# import shutil
import esget_logger
import esget_db
# import esget_fs
# import esget_local_files


CFILE = "cordex_eur_t.cfg"
config = ConfigParser.SafeConfigParser()
configfile = os.path.join("../../config", CFILE)
config.read(configfile)

# Initialize root logger
esget_logger.Logging(config)

RESETDB = False
#  fromto.json is extracted from the log and contains a list
#  of a 2-element list of [sourcefile, destinationpath]
fromtofile = open("/home/hvwaldow/esget/log/fromto.json", "r")
fromto = json.load(fromtofile)
PREFIX = '/net/atmos'  # machine dependent
# PREFIX = '/'  # machine dependent
local_fs_record = "./tmp_fsrecord.cpy"
pathfields = config.get('DEFAULT', 'pathfields').split(',')

## check whether src and destination exist and have the same size
# res = list()
# for x in fromto:
#     destfile = os.path.join(PREFIX, x[1].strip("/"), os.path.basename(x[0]))
#     srcfile = os.path.join(PREFIX, x[0].strip("/"))
#     if os.path.exists(destfile):
#         if os.path.getsize(destfile) == os.path.getsize(srcfile):
#             res.append(1)
#             print("OK")
#         else:
#             print("differing sizes:")
#             print("{0}".format(x[0]))
#             print("{0}".format(x[1]))
#     else:
#         print("destination does not exists:")
#         print(destfile)

# print("All OK?  {0}".format(all(res)))

## moving (overwriting) MAKE SURE "PREFIX" is CORRECT!
# for x in fromto:
#     destfile = os.path.join(PREFIX, x[1].lstrip("/"))
#     srcfile = os.path.join(PREFIX, x[0].lstrip("/"))
#     print("F: {0}".format(srcfile))
#     print("T: {0}".format(destfile))
#     print("")        
#     shutil.copy(srcfile, destfile)

config = ConfigParser.SafeConfigParser()
configfile = os.path.join("../../config", CFILE)
config.read(configfile)

# # Initialize database
C = esget_db.EsgetDB(config)
C.init_db(reset=False)

# make recordlist for updating db


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


def scan_filesystem(paths, local_fs_record):
    '''Scans the filesystem and records file-info and checksums.
    Writes an intermediate results to local_fs_record_file if it
    is not None'''
    if len(paths) < 1:
        print("No paths collected")
    chunksize = int(np.ceil(len(paths) / (mp.cpu_count()*2/3)))
    print("Scanning {0} files ...".format(len(paths)))
    print("Number of cores: {0}, chunksize: {1}"
          .format(mp.cpu_count(), chunksize))
    t0 = time.time()
    pool = mp.Pool()
    recordlist = pool.map(mk_all_fields,
                          paths, chunksize=chunksize)
    cPickle.dump(recordlist,
                 open(local_fs_record, "w"), protocol=2)
    t1 = (time.time() - t0) / 60.0 / 60.0
    print("Scanning of {0} files finished in {1:.2f} hours"
          .format(len(paths), t1))


def mk_fullrecordlist(local_fs_record, pathfields):
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

    try:
        fh = open(local_fs_record, "r")
        recordlist = cPickle.load(fh)
        fh.close()
    except:
        print("Could not read {0}. Aborting"
              .format(local_fs_record))
        sys.exit(1)

    for f in recordlist:
        dirpath = os.path.dirname(f['filename'])
        pathvalues = split_path(dirpath)
        pathvalues = pathvalues[-len(pathfields):]
        f.update(dict(zip(pathfields, pathvalues)))
    return(recordlist)




# paths = [os.path.join(PREFIX, x[1].lstrip('/'), os.path.basename(x[0])) for x in fromto]
# allthere = all([os.path.exists(x) for x in paths])
# print("All there: {0}".format(allthere))
# scan_filesystem(paths, local_fs_record)
recordlist = mk_fullrecordlist(local_fs_record, pathfields)
C.update_files(recordlist)




# class LocalFiles(object):
#     ''' Handles information gathering from the local filesystem'''

#     def __init__(self, config):
#         self.fileroot = config.get('DEFAULT', 'fileroot')
#         self.no_storagefiles = config.get('Tuning', 'no_storagefiles').split(',')
#         subdirs = os.walk(self.fileroot, topdown=True).next()[1]
#         self.domains = [x for x in subdirs if x not in self.no_storagefiles]
#         self.local_fs_record = config.get('Paths', 'local_fs_record')
#         self.pathfields = config.get('DEFAULT', 'pathfields').split(',')
#         self._log = logging.getLogger(self.__class__.__name__)
#         self.paths = []
#         self.recordlist = []

#     def mkfilelist(self):
#         ''' Finds all files in self.domains below self.fileroot'''

#         paths = list()
#         self._log.info("scanning filesystem below " + self.fileroot)
#         for d in self.domains:
#             dpaths = list()
#             self._log.info("scanning domain {0}".format(d))
#             root = os.path.join(self.fileroot, d)
#             for dirpath, dirs, files in os.walk(root):
#                 if len(files) > 0:
#                     if len(dirs) > 0:
#                         self._log.error("Something wrong with the directory" +
#                                         " structure:\n{0} contains files" +
#                                         " and directories".format(dirpath))
#                     dpaths.extend([os.path.join(dirpath, fi) for fi in files])
#             self._log.info("Found {0} files in domain {1}"
#                            .format(len(dpaths), d))
#             paths.extend(dpaths)
#         self.paths = paths
#         self._log.info("Found {0} files in total.".format(len(paths)))

#     def estimate_scanning_time(self, samplesize, chunksize=None):
#         ''' Estimates time necessary to extract all info about
#         local files and calculates good chunksize for pool.map'''

#         #  preliminary results on atmos indicate that
#         #  chunksizes resulting in using 2/3 of available cores are good.
#         if not chunksize:
#             chunksize = samplesize / (mp.cpu_count()*2/3)
#         if len(self.paths) < 1:
#             self._log.error("No paths collected - You either forgot " +
#                             "to run mkfilelist first, or the " +
#                             "storage-tree {0} is empty."
#                             .format(self.fileroot))
#             return(0)

#         if len(self.paths) < samplesize:
#             self._log.error(("Number of paths ({0}) must be greater " +
#                             "than samplesize ({1}).")
#                             .format(len(self.paths), samplesize))
#             return(0)
#         self._log.info("Collecting information and calculating " +
#                        "checksums for {0} files".format(samplesize))
#         self._log.info("Chunksize = {0}".format(chunksize))
#         self._log.info("Estimating time needed ...")
#         t0 = time.time()
#         pool = mp.Pool()
#         recordlist = pool.map(mk_all_fields, self.paths[0:samplesize],
#                               chunksize=chunksize)
#         t1 = time.time() - t0
#         t_est = t1 / samplesize * len(self.paths) / 60.0 / 60.0
#         self._log.info("Estimated time necessary: {0:.2f} hours."
#                        .format(t_est))
#         return(0)

#     def scan_filesystem(self):
#         '''Scans the filesystem and records file-info and checksums.
#         Writes an intermediate results to local_fs_record_file if it
#         is not None'''
#         if len(self.paths) < 1:
#             self._log.error("No paths collected - You either forgot " +
#                             "to run mkfilelist first, or the " +
#                             "storage-tree {0} is empty."
#                             .format(self.fileroot))

#         chunksize = int(np.ceil(len(self.paths) / (mp.cpu_count()*2/3)))
#         self._log.info("Scanning {0} files ...".format(len(self.paths)))
#         self._log.info("Number of cores: {0}, chunksize: {1}"
#                        .format(mp.cpu_count(), chunksize))
#         t0 = time.time()
#         pool = mp.Pool()
#         self.recordlist = pool.map(mk_all_fields,
#                                    self.paths, chunksize=chunksize)
#         cPickle.dump(self.recordlist,
#                      open(self.local_fs_record, "w"), protocol=2)
#         t1 = (time.time() - t0) / 60.0 / 60.0
#         self._log.info("Scanning of {0} files finished in {1:.2f} hours"
#                        .format(len(self.paths), t1))

#     def mk_fullrecordlist(self):
#         '''Expands each filename of recordlist into fields
#         associated with the filepath. If class attribute "recordlist"
#         (which is usually provided by scan.filesystem()) is empty,
#         an attempt is made to read the file <local_fs_record>'''

#         def split_path(path):
#             ''' Recursive function to create a list of path elements'''
#             folders = list()
#             if path == '' or path == '/':
#                 return([path])
#             else:
#                 rest, folder = os.path.split(path)
#                 folders = split_path(rest)
#                 folders.append(folder)
#                 return(folders)

#         if len(self.recordlist) == 0:
#             self._log.warning("Recordlist empty, trying to read from file")
#             self.get_rec_fromfile()
#             self._log.info("Got {0} local file paths from file"
#                            .format(len(self.recordlist)))
#         for f in self.recordlist:
#             dirpath = os.path.dirname(f['filename'])
#             pathvalues = split_path(dirpath)
#             pathvalues = pathvalues[-len(self.pathfields):]
#             f.update(dict(zip(self.pathfields, pathvalues)))


# LF = esget_local_files.LocalFiles(config)


# # # updating the database



# # titles = "'"+"','".join([os.path.basename(x[0]) for x in fromto])+"'"

# # selectfields = F.pathfields + \
# #     ['checksum', 'checksum_type', 'url_http']
# # selectstr = ','.join(selectfields)

# # conn, c = C.connect()
# # csel = c.execute('''SELECT {0}
# # FROM esgffiles
# # WHERE title IN ({1})'''.format(selectstr, titles)).fetchall()
