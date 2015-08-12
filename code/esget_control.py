# Main script for downloading a subset of Cordex
import sys
import os
# import cPickle
###############
import ConfigParser
# reload(ConfigParser)
import esget_logger
# reload(esget_logger)
import esget_db
reload(esget_db)
import esget_esgf
# reload(esget_esgf)
import esget_fs
# reload(esget_fs)
import esget_wget
# reload(esget_wget)
import esget_local_files
# reload(esget_local_files)
import esget_check_remote

# ###### Edit to modify the behaviour of Esget ################################

# Moves "unlinked" files in the unlinked/<RELINK> tree back into
# the main storage tree.
# Sets the respective "unlink_date" - flag in the database to '0'.
# Implies that all following flags (RESETDB ... CLEANLOG) are False
#RELINK = '2014-11-25'
RELINK = False

# This clears the database, all info about local files and their checksums
# is lost. Pretty much imples  SCAN=True.
RESETDB = False

# Scans local storage-tree, calculates checksums and updates the
# table "localfiles" of the database.
# This should only be necessary initially, and to fix a corrupt
# table of local files. This makes only sense with RESETDB = True
# ATTENTION: Scan should only be used to create a special database
# that represents the whole storage-tree. A ESGSEARCH - operation
# with that database better uses a query-definition that represents
# a superset of the whole storage-tree, or else files will be in-
# correctly unlinked.
SCAN = False

# Necessary before any downloading can occur. Queries the ESGF-database,
# (re)creates the table "esgffiles", marks files that need downloading,
# and moves outdated files from main storage-tree to "unliked" tree.
ESGSEARCH = True

# Download files from ESGF thredds-servers to "download"-directory.
DOWNLOAD = False

# Checks files (size, existence, checksum), moves them to
# the storage-tree, and updates table "localfiles" in database.
# Fixes permissions, and copies database into a public space
FINISH = False

# Cleans temporary file and the log of the wget scripts.
# THIS IS NECESSARY BEFORE THE NEXT RUN!
CLEAN = False

# Experience shows that failure to download a file is often
# intermittent -- it will download next time.
# If CYCLE == "until_stable", the task will be repeated until the number
# of failed downloads in two successive runs remains constant.
# If CYCLE == "forever", the task will be repeated forever.
# Set CYCLE = False if only one run of the task is desired.
# CYCLE == True implies CLEAN == True
CYCLE = False
# CYCLE = "until_stable"

## TODO ##
## Disabled cycling. Thsi needs to be re-worked. Proper separation
## between search and download

# Whether to clear the log. If False, logging information for
# subsequent runs is appended.
CLEANLOG = False

# ##############################################################################


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
    unlinkdate = C.mark_unlink()

    # Unlink files
    unlinked = F.unlink(C.get_unlink_files(unlinkdate))
    C.change_path(unlinked)

    # Search unlinked files that are now to be downloaded,
    # remove them from localfiles,
    # delete them from disk.
    F.rm_files(C.find_relinkable())


def download(W):
    W.run_esget_getter()


def finish(F, C):
    F.check_dl(C)
    F.finish_cycle()
    no_failed = C.mk_failed_table()
    return(no_failed)


def clean(F):
    cleanlog = CLEANLOG
    F.cleanup(cleanlog=cleanlog)


def relink(F, C, unlinkdate):
    ret = F.relink(unlinkdate)
    if not ret:
        C.relink_update(unlinkdate)


if __name__ == '__main__':

    if CYCLE:
        CLEAN = True
    if RELINK:
        RESETDB = False
        SCAN = False
        ESGSEARCH = False
        DOWNLOAD = False
        FINISH = False
        CLEAN = False
        CYCLE = False
        CLEANLOG = False

    # get configuration
    config = ConfigParser.SafeConfigParser()
    configfile = os.path.join("../config", sys.argv[1])
    # FOR DEBUGGING
    #configfile = os.path.join("../config", "cordex_eur_ALL.cfg") 
    config.read(configfile)

    # Initialize root logger
    esget_logger.Logging(config)

    # Initialize database
    C = esget_db.EsgetDB(config)
    C.init_db(reset=True if RESETDB else False)

    # Initialize filesystem-operations module
    F = esget_fs.EsgetFS(config)

    if RELINK:
        relink(F, C, RELINK)
        sys.exit(0)

    # Take inventory of local storage-tree
    if SCAN:
        # Initialize local filesystem - scan module
        LF = esget_local_files.LocalFiles(config)
        scanlocal(LF, C)

    if ESGSEARCH:
        S = esget_esgf.ESG_search(config, C)
        esgsearch(S)
        #postqueryupdate(C, F)

    # Download
    if DOWNLOAD:
        W = esget_wget.EsgetWget(config, C)
        CHECK = esget_check_remote.EsgetCheckRemote(config)
        CHECK.do_checks()
        download(W)

    # checking downloads / moving files
    if FINISH:
        no_failed = finish(F, C)

    # cleaning up
    if CLEAN:
        clean(F)
