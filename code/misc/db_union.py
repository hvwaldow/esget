#!/usr/bin/python

import apsw
import os
import glob
import re
import sys

'''Creates  a database of the union of the "localfiles"-table in the
individual db- files in esget/db'''

dbdir = "/home/hvwaldow/esget/db"
newdbname = os.path.join(dbdir, "cordex_eur_ALL_localfiles.db")


def duplicate_tables(c, otherdb, tables):
    ''' Duplicates tables (without content) of otherdb'''
    c.execute('''ATTACH  '{0}' as db1'''.format(otherdb))
    for table in tables:
        sql = c.execute('''SELECT sql from db1.sqlite_master
                           WHERE type='table' AND
                                 name='{0}' '''
                        .format(table)).fetchall()[0][0]

        c.execute(sql)
        sql = c.execute('''SELECT sql from db1.sqlite_master
                           WHERE type='index' AND
                           tbl_name='{0}' '''
                        .format(table)).fetchall()
        sql = [x[0] for x in sql]
        for sqll in sql:
            c.execute(sqll)
    c.execute('''DETACH db1''')


def mk_unionselect(table, nodbfiles):
    select1 = ["db" + str(x) for x in range(0, nodbfiles)]
    select1 = zip(["SELECT * from {{0}}.{0}".format(table)]*nodbfiles, select1)
    select1 = [x[0].format(x[1]) for x in select1]
    select1 = " UNION ".join(select1)
    return(select1)

if __name__ == "__main__":

    if os.path.exists(newdbname):
        print("Target-DB already exists - aborting!")
        sys.exit(1)

    # # collect cordex-database - files
    # dbfiles = glob.glob(os.path.join(dbdir, "*.db"))
    # # remove cordex_test.db and cordex_wim.db
    # dbfiles = [x for x in dbfiles if not (re.search("wim", x)
    #                                       or re.search("test", x))]

    dbfiles = [os.path.join(dbdir, x) for x in
               ["cordex_eur_fx.db", "cordex_eur_precip.db",
                "cordex_eur_radiation.db", "cordex_eur_t.db",
                "cordex_eur_wind.db", "cordex_eur_mon_sem_fx.db",
                "cordex_eur_pressure.db", "cordex_eur_surface.db",
                "cordex_eur_vapour.db"]]

    nodbfiles = len(dbfiles)

    # database connection
    db = apsw.Connection(newdbname)
    c = db.cursor()

    duplicate_tables(c, dbfiles[0], ["localfiles", "failed"])
    select_local = mk_unionselect("localfiles", nodbfiles)
    select_failed = mk_unionselect("failed", nodbfiles)
    for dbf in enumerate(dbfiles):
        c.execute('''ATTACH '{0}' AS db{1}'''.format(dbf[1], dbf[0]))
    c.execute('''INSERT INTO localfiles {0}'''.format(select_local))
    c.execute('''INSERT INTO failed {0}'''.format(select_failed))
    db.close()
