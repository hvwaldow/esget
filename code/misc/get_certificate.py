from myproxy.client import MyProxyClient
import os
import time

server = "pcmdi9.llnl.gov"
username = "haraldesg"
password = "ESGiertt11"
outfile = "./globus/credentials.pem"
cacertdir = "./globus/certificates"
lifetime = 60


def certificate_get(mpc, server, username, password,
                    lifetime=43200, cacertdir, outfile):
    cert = mpc.logon(username, password, bootstrap=True, updateTrustRoots=False)
    with open(outfile, "w") as ofile:
        for c in cert:
            ofile.write(c)


def certificate_age(outfile):
    return(time.time() - os.stat(outfile).st_mtime)


if __name__ == "__main__":
    mpc = MyProxyClient(hostname=server, caCertDir=cacertdir)
    #cred = certificate_get(mpc, server, username, password, lifetime, cacertdir)
    #certificate_write(cred, outfile)
    print(certificate_age(outfile))
