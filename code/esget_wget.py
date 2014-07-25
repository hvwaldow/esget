import logging
import numpy as np
import re
import os
import cPickle
import Queue as Q
from time import asctime
from multiprocessing import Process
import subprocess as sp


class EsgetWget(object):
    '''Handles splitting of the job, writing wget files, starting download,
    checking success and updating the database'''

    def __init__(self, config, esgetDB_obj):
        self._log = logging.getLogger(self.__class__.__name__)
        self.maxchunksize = config.getfloat('Tuning', 'maxchunksize')
        self.hosts = self._mkhostsdict(config)
        self.wget_template = config.get('Paths', 'wget_template')
        self.wget_dir_prefix = config.get('Paths', 'wget_dir')
        self.wget_log_dir = config.get('Paths', 'wget_log_dir')
        self.tmpdir = config.get('Paths', 'tmpdir')
        self.esgetDBobj = esgetDB_obj
        self.hostq = self.mk_hostq()
        self.esgserver = config.get('DEFAULT', 'esgf_id_server')
        self.esgid = config.get('DEFAULT', 'esgf_id')
        self.esgpass = config.get('DEFAULT', 'esgf_id_password')

    def _mkhostsdict(self, config):
        hostsections = [x for x in config.sections() if re.match('HOST_', x)]
        hostdict = dict([(config.get(x, 'hostname'),
                          [config.getint(x, 'no_jobs'),
                           config.get(x, 'fileroot')]) for x in hostsections])
        return(hostdict)

    def split_jobs(self, dlsizes):
        '''Splits an array as returned from get_download size
        into chunks with total size <= maxchunksize'''

        def _r_split(dlsizes, maxsize):
            if dlsizes['cumsum'].iloc[-1] <= maxsize:
                return([dlsizes])
            # print("dlsizes[2]:")
            # print(dlsizes[2].head(n=10))
            # print("maxsize {0}".format(maxsize))
            # print("maxindex {0}".format(max(np.where(dlsizes[2] <= maxsize)[0])))

            split = max(np.where(dlsizes['cumsum'] <= maxsize)[0])
            res = [dlsizes.iloc[0:split+1]]
            dlsizes['cumsum'] = dlsizes['cumsum'] - \
                dlsizes.iloc[split]['cumsum']
            dlsizes = dlsizes.iloc[split+1:]
            return(res + _r_split(dlsizes, maxsize))

        largestfile = max(dlsizes['size'])
        self._log.info("Largest File: {0}".format(largestfile))
        if largestfile > self.maxchunksize:
            self._log.info("Setting maxchunksize to largest file: {0}"
                           .format(largestfile + 1000))
            self.maxchunksize = largestfile + 1000
        dlsizes['cumsum'] = np.cumsum(dlsizes['size'])
        self._log.info("Splitting {0} downloads into chunks < {1} GB"
                       .format(dlsizes.shape[0], self.maxchunksize/10**9))
        res = _r_split(dlsizes, self.maxchunksize)
        self._log.info("Splitting resulted in {0} chunks".format(len(res)))
        return(res)

    def get_wget_download_files(self, chunks):
        '''Return file descriptions for the "embedded" file description
           in the wget-files. "chunks" is the output of split_jobs.'''
        self._log.info("Constructing wget-suitable file-descriptions")
        reurl = re.compile('(http://.*?\.nc).*')

        def l_url(e_url):
            return(re.sub(reurl, '\g<1>', e_url))

        def none2empty(x):
            return('' if x is None else x)

        filechunks = []
        for ch in chunks:
            line = ["'{0}' '{1}' '{2}' '{3}'"
                    .format(x[1]['title'], l_url(x[1]['url_http']),
                            none2empty(x[1]['checksum_type']),
                            none2empty(x[1]['checksum']))
                    for x in ch.iterrows()]
            line1 = "\n".join(line)
            filechunks.append(line1)

        self.filechunks = filechunks
        self._log.info("Constructed list of {0} chunks"
                       .format(len(filechunks)))
        return()

    def mk_hostq(self):
        hostq = Q.Queue()
        for h in self.hosts:
            self._log.info("Adding to hostqueue: {0} times {1}"
                           .format(h, self.hosts[h][0]))
            for h0 in range(0, self.hosts[h][0]):
                hostq.put(h)
        return(hostq)

    def mk_wget_file(self, filechunk, host):
        self._log.info("Making wget-file for host {0}".format(host))
        prefix = os.path.join(self.hosts[host][1], "download")
        f = open(self.wget_template, "r")
        wget = f.read()
        f.close()
        wgetopts = '" -nv -x -nH -P ' + prefix + ' "'
        wget = re.sub('server=', 'server=' + '"' + self.esgserver + '"',
                      wget, count=1)
        wget = re.sub('username=', 'username=' + '"' + self.esgid + '"',
                      wget, count=1)
        wget = re.sub('pass=', 'pass=' + '"' + self.esgpass + '"',
                      wget, count=1)
        wget = re.sub('customwget=', 'customwget=' + wgetopts, wget, count=1)
        wget = re.sub('download_files=',
                      'download_files='+'"'+filechunk+'"', wget, count=1)
        return(wget)

    # def start_job(self, text, machine, name, pipe):
    def start_job(self, text, machine, name):
        try:
            self._log.info("starting job: {0}".format(name))
            targetfile = os.path.join(self.wget_dir_prefix, "wget_"+name+".sh")
            tmpfilename = os.path.join(self.wget_dir_prefix,
                                       "wget_"+name+".tmp")
            wget_logfile = os.path.join(self.wget_log_dir, name)
            tmp = open(tmpfilename, "w")
            tmp.write(text)
            tmp.close()
            os.chmod(tmpfilename, 0744)
            sp.check_call(['scp', tmpfilename, machine+":"+targetfile])
            sp.check_call(['ssh', machine, "chmod", "u+x", targetfile])
            sp.check_call(['ssh', machine, targetfile, "2>&1", ">{0}"
                           .format(wget_logfile)])
            sp.check_call(['ssh', machine, "rm", targetfile])
        except Exception, e:
            self._log.error("job {0} failed: {1}".format(name, str(e)))
            f = open(os.path.join(self.wget_log_dir, name), 'w')
            f.write("FAILED: {0}".format(asctime()))
            f.close()
        return()

    def start_all(self):
        jobcounter = 0
        self.procdict = {}
        while len(self.filechunks) > 0:
            self._log.info("{0} filechunks left".format(len(self.filechunks)))
            self._log.info("waiting for new job-slot ...")
            while self.hostq.empty():
                for pr in self.procdict:
                    if (not self.procdict[pr]["process"].is_alive() and
                        self.procdict[pr]["end"] == "running"):
                        self._log.info("Found finished process: {0}:{1},"
                                       .format(self.procdict[pr]["host"],
                                               self.procdict[pr]["process"])
                                       + "closing")
                        self.procdict[pr]["end"] = asctime()
                        self.hostq.put(self.procdict[pr]["host"])
                        # somehow establish communication with DB-updater
                        # instead of dumping to file here
                        procdes = dict(self.procdict[pr])
                        del procdes["process"]
                        f = open(os.path.join(self.tmpdir,
                                              "job_"+pr+".cpy"), 'w')
                        cPickle.dump(procdes, f, protocol=2)
                        f.close()
            jobcounter += 1
            host = self.hostq.get()
            chunk = self.filechunks.pop()
            text = self.mk_wget_file(chunk, host)
            name = host + "_{0}".format(jobcounter)
            # parent_conn, child_conn = Pipe(duplex=False)
            p = Process(target=self.start_job,
                        # args=(text, host, name, child_conn),
                        args=(text, host, name),
                        name=name)
            p.start()
            self.procdict[name] = {"process": p,
                                   "host": host, "start": asctime(),
                                   "end": "running", "files": chunk}
            self._log.info("Started process: {0}:{1}".format(host, p))
        return()

    def finish_all(self):
        self._log.info("Waiting for all processes to finish")
        running = [x for x in self.procdict
                   if self.procdict[x]["end"] == "running"]
        while len(running) > 0:
            stopped = [x for x in running
                       if not self.procdict[x]["process"].is_alive()]
            for x in stopped:
                # if not self.procdict[x]["pipe"].closed:
                self._log.info("Closing process: {0}:{1}"
                               .format(self.procdict[x]["host"],
                                       self.procdict[x]["process"]))
                self.procdict[x]["end"] = asctime()
                # self.procdict[x]["output"] = self.procdict[x]["pipe"]\
                #                                  .recv()
                # self.procdict[x]["pipe"].close()
                self.hostq.put(self.procdict[x]["host"])
                # somehow establish communication with DB-updater
                procdes = dict(self.procdict[x])
                del procdes["process"]
                # del procdes["pipe"]
                f = open(os.path.join(self.tmpdir, "job_"+x+".cpy"), 'w')
                cPickle.dump(procdes, f, protocol=2)
                f.close()
            running = [x for x in self.procdict
                       if self.procdict[x]["end"] == "running"]
        self._log.info("All processes finished")
        return()

    def run_esget_getter(self):
        self._log.info("Running esget_getter ...")
        dlurls = self.esgetDBobj.get_download_url()
        if len(dlurls) == 0:
            self._log.info("Skipping download")
        else:
            self.get_wget_download_files(self.split_jobs(dlurls))
            self.start_all()
            self.finish_all()
