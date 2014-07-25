import numpy as np
import urllib2
import json
import cPickle
import sys
from time import time, strftime
import logging
import multiprocessing as mp
import os


class ESG_searchdef(object):
    '''Interface of search definition (configfiles) and
    searchstrings for ESGF REST-API (provided by mksearchstring)'''

    def __init__(self, config):
        self._log = logging.getLogger(self.__class__.__name__)
        self.filename = config.get('Paths', 'searchdeffile')
        self.gwfile = os.path.join(config.get('Paths', 'gatewaydir'),
                                   config.get('Tuning', 'gatewayfile'))
        self.timeout = config.getfloat('Tuning', 'timeout')
        self.ssdict = self._getdict()
        self.searchstring = self.mksearchstring()
        # self.outputfile = self.getoutputfile()
        self.gwdict = self._read_gwdict()
        self.resultlist = []

    def _getdict(self):
        self._log.info("Reading search definition from {0}."
                       .format(self.filename))
        f = open(self.filename, "r")
        ssdict = json.load(f)
        f.close()
        return(ssdict)

    def _read_gwdict(self):
        self._log.info("Reading gateways from {0}."
                       .format(self.gwfile))
        fp = open(self.gwfile, "r")
        gwdict = json.load(fp)
        fp.close()
        return(gwdict)

    def del_gateway(self, gw):
        self._log.info("Deleting gateway {0}."
                       .format(gw))
        try:
            del self.gwdict[gw]
        except KeyError:
            self._log.error("Gateway to be deleted ({0})".format(gw)
                            + " is not in gwdict")

    def insert_gateway(self, gw, url):
        self._log.info("Inserting gateway {0} : {1}."
                       .format(gw, url))
        self.gwdict[gw] = url

    def mksearchstring(self):
        # filter out empty values and meta_info
        sskeys = [k for k in self.ssdict.keys()
                  if (bool(self.ssdict[k]) and k[0] != '_')]
        # list of searchstring pieces with single values
        sslist_single = ["{}={}".format(k, self.ssdict[k]) for k in sskeys
                         if type(self.ssdict[k]) != list]
        # list of searchstring pieces with multiple values
        sskeys_multi = [k for k in sskeys if type(self.ssdict[k]) == list]
        sslist_multi = []
        for k in sskeys_multi:
            sslist_multi.extend(["{}={}".
                                 format(k, v) for v in self.ssdict[k]])
        sstring = '&'.join(sslist_single+sslist_multi)
        return(sstring)

    # def getoutputfile(self):
    #     return(self.ssdict['_outfile'])


def exec_search(gw, searchstring, timeout):
    ''' Performs one specific search. Has to be outside class,
    because called in multiprocessing context'''
    gwurl = gw[1]
    t0 = time()
    queryurl = gwurl+"?"+searchstring
    try:
        result = urllib2.urlopen(queryurl, timeout=timeout)
    except urllib2.HTTPError as e:
        qtime = time() - t0
        return({"gw": gw[0], "url": gw[1], "searchstring": searchstring,
                "qtime": qtime, "result": e.reason})
    except urllib2.URLError as e:
        qtime = time() - t0
        return({"gw": gw[0], "url": gw[1], "searchstring": searchstring,
                "qtime": qtime, "result": e.reason})
    except:
        qtime = time() - t0
        return({"gw": gw[0], "url": gw[1], "searchstring": searchstring,
                "qtime": qtime, "result": sys.exc_info()[0]})
    qtime = time() - t0
    searchresult = json.load(result)
    return({"gw": gw[0], "url": gw[1], "searchstring": searchstring,
            "qtime": qtime, "result": searchresult})


class ESG_search(ESG_searchdef):
    def __init__(self, config, esgdb_obj):
        ESG_searchdef.__init__(self, config)
        self._log = logging.getLogger(self.__class__.__name__)
        self.querylimit = config.getint('Tuning', 'querylimit')
        self.no_queryproc = config.getint('Tuning', 'no_queryproc')
        self.tmpdir = config.get('Paths', 'tmpdir')
        self.current_num_files = esgdb_obj.get_no_localfiles()

    def check_gateways(self):
        '''Queries all gateways for number of available files;
        checks response time,'''
        self.check_gw_results = dict()
        searchstr = self.searchstring + "&limit=0"
        self.check_gw_pool = mp.Pool(min(mp.cpu_count, len(self.gwdict)))
        for gw in self.gwdict.iteritems():
            self._log.info("Querying gateway {0} with timeout {1}s"
                           .format(gw[0], self.timeout))
            self.check_gw_pool.apply_async(exec_search,
                                           args=(gw, searchstr,
                                                 self.timeout),
                                           callback=self.log_search)
        self.check_gw_pool.close()
        self.check_gw_pool.join()
        self.select_gw()

    def log_search(self, searchres):
        ''' Collects serarch results. Has to return no matter what, else
        calling sub-processes hang'''
        try:
            if type(searchres["result"]) != dict:
                self._log.error("Error querying gateway {0}: {1}".
                                format(searchres["gw"], searchres["result"]))
            else:
                self._log.info("Returned from querying {0}".
                               format(searchres["gw"]))
                self.check_gw_results[searchres["gw"]] = {
                    "qtime": searchres["qtime"],
                    "numFound": searchres["result"]["response"]["numFound"]}
        except:
            self._log.error("Something unexpected happened" +
                            "inside log_search: {0}".format(sys.exc_info()[0]))
            return()

    def select_gw(self):
        '''Selects 1. gateway with maximum number of files,
        2. fastest one in case of ties. Records total number of records'''
        low_file_warning = False
        res = self.check_gw_results
        nfiles = [(x, res[x]["numFound"]) for x in res]
        if len(nfiles) <= 0:
            self._log.error("No usable gateway present. Aborting!")
            sys.exit(1)
        self._log.info("Numfiles at gateways: {0}".format(nfiles))
        ma = max([x[1] for x in nfiles])
        if ma < self.current_num_files:
            self._log.warning("Found less files than currently in database.")
            low_file_warning = True
        magw = [x[0] for x in nfiles if x[1] == ma]
        nqtime = [(x, res[x]["qtime"]) for x in res]
        self._log.info("Response times gateways: {0}".format(nqtime))
        nqtime_filt = [x for x in nqtime if x[0] in magw]
        qmin = min([x[1] for x in nqtime_filt])
        miniloc = [x for x in nqtime_filt if x[1] == qmin]
        self.gw = miniloc[0][0]
        self.numfiles = res[self.gw]["numFound"]
        self._log.info("Chosen gateway: {0} with {1}"
                       .format(self.gw, res[self.gw]))
        return(low_file_warning)

    def query(self):
        self.query_return_count = 0
        no_queries = int(np.ceil(self.numfiles/float(self.querylimit)))
        self._log.info("{0} queries to be scheduled".format(no_queries))
        offsets = np.arange(0, self.numfiles, self.querylimit)
        searchstrings = [self.searchstring+"&limit={0}&offset={1}"
                         .format(self.querylimit, o) for o in offsets]
        self._log.info("Making process-pool of size {0} for queries"
                       .format(self.no_queryproc))
        querypool = mp.Pool(self.no_queryproc)
        for sstring in searchstrings:
            querypool.apply_async(exec_search,
                                  args=((self.gw, self.gwdict[self.gw]),
                                        sstring, self.timeout),
                                  callback=self.log_query)
        querypool.close()
        querypool.join()

    def log_query(self, queryres):
        ''' Collects query results. Has to return no matter what, else
        calling sub-processes hang'''

        try:
            if type(queryres["result"]) != dict:
                self.query_return_count += 1
                self._log.error("Error querying gateway {0}: {1}".
                                format(queryres["gw"], queryres["result"]))
                badquerypath = os.path.join(self.tmpdir, "bad_query_{0}.cpy"
                                            .format(self.query_return_count))
                self._log.info("Written spurious result to {0}."
                               .format(badquerypath))
                f = open(badquerypath, 'w')
                cPickle.dump(queryres, f, protocol=2)
                f.close()
            else:
                self.query_return_count += 1
                self._log.info("Returned from querying {0} ({1})".
                               format(queryres["gw"], self.query_return_count))
                f = open(os.path.join(self.tmpdir, "queryres_{0}.cpy"
                                      .format(self.query_return_count)), 'w')
                cPickle.dump(queryres, f, protocol=2)
                f.close()

        except:
            self._log.error("Something unexpected happened" +
                            " inside log_search: {0}"
                            .format(sys.exc_info()[0]))
            badquerypath = os.path.join(
                self.tmpdir, "bad_query_{0}.cpy".format(
                    strftime("%Y-%m-%d-%H-%M-%S")))
            self._log.info("Written spurious result to {0}."
                           .format(badquerypath))
            f = open(badquerypath, 'w')
            cPickle.dump(queryres, f, protocol=2)
            f.close()
            return()
