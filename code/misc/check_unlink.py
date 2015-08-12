# checks whether the filename and the flag "unlink_date" match

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
#reload(esget_db)
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
C.dbname = DATABASE
C.init_db(reset=True if RESETDB else False)

ulfiles = C.get_unlink_files()
import os.path
fnotexist = [x for x in ulfiles if not os.path.isfile(x)]
if len(fnotexist) > 0:
    print("some don't exist")
else:
    print("all exist")
