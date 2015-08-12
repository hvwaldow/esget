import esget_db
reload(esget_db)
import sys
import os
import ConfigParser
import esget_logger
config = ConfigParser.SafeConfigParser()
configfile = os.path.join("../config", "cordex_eur_ALL.cfg")
config.read(configfile)
esget_logger.Logging(config)
C = esget_db.EsgetDB(config)
