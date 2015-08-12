## Test path changes after unlinking

CONFIGFILE = "cordex_eur_ALL.cfg"
DATABASE = "../../db/cordex_eur_ALL.db"

import sys
sys.path.insert(0, "..")
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

RESETDB = False

config = ConfigParser.SafeConfigParser()
configfile = os.path.join("../../config", CONFIGFILE)
config.read(configfile)
config.set('Paths', 'logfile', '../../log/debug.log')
esget_logger.Logging(config)

C = esget_db.EsgetDB(config)
testpath_f = "/net/atmos/data/CORDEX/EUR-11/rcp85/mon/tasmin/IPSL-INERIS/IPSL-IPSL-CM5A-MR/WRF331F/r1i1p1/1/tasmin_EUR-11_IPSL-IPSL-CM5A-MR_rcp85_r1i1p1_IPSL-INERIS-WRF331F_v1_mon_203101-204012.nc"
testpath_fuling ="/net/atmos/data/CORDEX/unlinked/2015-03-20/EUR-11/rcp85/mon/tasmin/IPSL-INERIS/IPSL-IPSL-CM5A-MR/WRF331F/r1i1p1/1/tasmin_EUR-11_IPSL-IPSL-CM5A-MR_rcp85_r1i1p1_IPSL-INERIS-WRF331F_v1_mon_203101-204012.nc"

res = C.change_path([(testpath_f, testpath_fuling)])

