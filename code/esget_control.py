# use esget to download a subset of Cordex for testing
import sys
import os

import ConfigParser
reload(ConfigParser)

import esget_logger
reload(esget_logger)

import esget_db
reload(esget_db)

import esget_esgf
reload(esget_esgf)

import esget_fs
reload(esget_fs)

import esget_wget
reload(esget_wget)

import esget_local_files
reload(esget_local_files)

# This clears the database, all info about local files and their checksums
# is lost. Pretty much imples  SCAN=True.
RESETDB = False

# Scans local storage-tree, calculates checksums and updates the
# table "localfiles" of the database.
# This should only be necessary initially, and to fix a corrupt
# table of local files. This makes only sense with RESETDB = True
SCAN = False

# Necessary before any downloading can occur. Queries the ESGF-database,
# (re)creates the table "esgffiles", marks files that need downloading,
# and moves outdated files from main storage-tree to "unliked" tree.
ESGSEARCH = True

# Download files from ESGF thredds-servers to "download"-directory.
DOWNLOAD = True

# Checks files (size, existence, checksum), moves them to
# the storage-tree, and updates table "localfiles" in database.
# Fixes permissions, and copies database into a public space
FINISH = True

# Cleans temporary file and the log of the wget scripts.
# THIS IS NECESSARY BEFORE THE NEXT RUN!
CLEAN = True

# Whether to clear the log. If False, logging information for
# subsequent runs is appended.
CLEANLOG = False


def scanlocal(LF, C, rm_tmp_scanres=False):
    # Makes a list of a files
    # This should be integrated into a higher level routine
    LF.mkfilelist()

    # Estimates time to scan local storage-tree (calculating checksums)
    # based on number of files in parameter
    LF.estimate_scanning_time(100)

    # Collects all info (e.g. checksums) from local files
    LF.scan_filesystem()

    # derives field values from local path
    LF.mk_fullrecordlist()

    # update / insert database with local storage-tree
    C.update_files(LF.recordlist)

    if rm_tmp_scanres:
        LF.rm_tmp_recordlist()


def esgsearch(S):
    low_file_warning = S.check_gateways()
    if low_file_warning:
        print("Less files found than already in localfiles. Aborting.")
        exit(0)
    S.query()


def postqueryupdate(C, F):
    # Put query results into database
    C.update_esgffiles()

    # Tag the files in esgffiles that we already have in localfiles
    C.mark_have()

    # Tag files that we have but that are not in esgffiles
    C.mark_unlink()

    # Unlink files
    F.unlink(C.get_unlink_files())
    # F.relink("2014-07-21")


def download(W):
    W.run_esget_getter()


def finish(F, C):
    F.check_dl(C)
    F.finish_cycle()
    if CLEAN:
        cleanlog = CLEANLOG
        F.cleanup(cleanlog=cleanlog)


if __name__ == '__main__':
    # get configuration
    config = ConfigParser.SafeConfigParser()
    configfile = os.path.join("../config", sys.argv[1])
    config.read(configfile)

    # Initialize root logger
    esget_logger.Logging(config)

    # Initialize database
    C = esget_db.EsgetDB(config)
    C.init_db(reset=True if RESETDB else False)

    # Initialize filesystem-operations module
    F = esget_fs.EsgetFS(config)

    # Initialize local filesystem - scan module
    LF = esget_local_files.LocalFiles(config)

    # Take inventory of local storage-tree
    if SCAN:
        scanlocal(LF, C)

    # Initialize ESG-search module
    S = esget_esgf.ESG_search(config, C)

    # Search ESGF for requested GeoMIP files
    # the results are written to tmpdir
    # Update database
    if ESGSEARCH:
        esgsearch(S)
        postqueryupdate(C, F)

    # Initialize wget-download module
    W = esget_wget.EsgetWget(config, C)

    # Download
    if DOWNLOAD:
        download(W)

    # checking downloads / moving files
    if FINISH:
        finish(F, C)
