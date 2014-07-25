# use esget to download Urs' GeoMIP request

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

# get configuration

config = ConfigParser.SafeConfigParser()
config.read("../config/geomip_getter.cfg")

# Initialize root logger
esget_logger.Logging(config)

# Initialize database
C = esget_db.EsgetDB(config)
## C.init_db(reset=True)

# Take inventory of local storage-tree
LF = esget_local_files.LocalFiles(config)

# ## This block can be skipped if the file <local_fs_record>
# ## is present. ###########################################

# Makes a list of a files
# This should be integrated into a higher level routine
## LF.mkfilelist()

# Estimates time to scan local storage-tree (calculating checksums)
# based on number of files in parameter
## LF.estimate_scanning_time(100)

# Collects all info (e.g. checksums) from local files
## LF.scan_filesystem()
# ### END SCANBLOCK ###########################################

## This block can be skipped if the locafiles - table is up-to-date
# #################################################################
# derives field values from local path
## LF.mk_fullrecordlist()

# update / insert database with local storage-tree
## C.update_files(LF.recordlist)
# ### END UPDATE LOCAL BLOCK ######################################

# Search ESGF for requested GeoMIP files
# the results are written to tmpdir
S = esget_esgf.ESG_search(config, C)
low_file_warning = S.check_gateways()
if low_file_warning:
    print("Less files found than already in localfiles. Aborting.")
    exit(0)
S.query()

# Put query results into database
C.update_esgffiles()

# Tag the files in esgffiles that we already have in localfiles
C.mark_have()

# Tag files that we have but that are not in esgffiles
C.mark_unlink()

# Unlink files
F = esget_fs.EsgetFS(config)
F.unlink(C.get_unlink_files())
#F.relink("2014-07-21")

# Starting Downloads
W = esget_wget.EsgetWget(config, C)
W.run_esget_getter()

# checking downloads / moving files
F.check_dl(C)
F.finish_cycle()

