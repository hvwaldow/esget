# A template to write debug-code that is being run from
# <esget_home>/code/misc but possibly from another machine, so that
# beginning path-elements of <esget_home> differ from the entry in
# the config file

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
reload(esget_wget)
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
# Initialize wget-download module
W = esget_wget.EsgetWget(config, C)
testdlurls = W.esgetDBobj.get_download_url().iloc[0:2,]
W.get_wget_download_files(W.split_jobs(testdlurls))

chunk = W.filechunks.pop()
host = W.hostq.get()

#W.wget_template = "../../config/templates/wget_template_new.sh"
text = W.mk_wget_file(chunk, host)
print(text)

#name = host + "_{0}".format(jobcounter)
