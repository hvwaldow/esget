#!/usr/bin/python

'''Takes a database file as an argument, compares localfiles and esgffiles,
and creates a table "failed_dl" that contains files that did not show
up in the local storage tree.'''

import sys
import apsw


dbname = sys.argv[1]


def connect(dbname):
    conn = apsw.Connection(dbname)
    c = conn.cursor()
    return((conn, c))


def read_chksums(c, table):
    selstring = 'SELECT checksum, checksum_type FROM esgffiles' \
                if table == 'esgffiles' \
                else 'SELECT md5, sha256 FROM localfiles'
    chk_esgf = c.execute(selstring).fetchall()
    return(chk_esgf)


def mk_failed_table(c):
    c.execute('''DROP TABLE IF EXISTS failed''')
    c.execute(''' CREATE TABLE failed AS
                  SELECT * FROM esgffiles WHERE 1=2''')
    allfields = c.execute("PRAGMA table_info(failed)").fetchall()
    allfields = [x[1] for x in allfields]
    for idx_col in allfields:
        c.execute("CREATE INDEX IF NOT EXISTS {0} ON {2} ({1})".
                  format("idx_failed_"+idx_col, idx_col, "failed"))


def get_failed(c, resloc, resesgf):
    setloc = [max(x) for x in resloc]
    setesgf = [x[0] for x in resesgf]
    failedfiles = [x for x in setesgf if x not in setloc]
    ffstring = "('"+"','".join(failedfiles) + "')"
    c.execute('''INSERT OR REPLACE INTO failed
                 SELECT * FROM esgffiles
                 WHERE checksum IN {0}'''.format(ffstring))

if __name__ == "__main__":
    conn, c = connect(dbname)
    resloc = read_chksums(c, 'localfiles')
    resesgf = read_chksums(c, 'esgffiles')
    print("Files in esgf: {0},\n Files in local: {1}"
          .format(len(resesgf), len(resloc)))
    assert(len(resloc) <= len(resesgf))
    mk_failed_table(c)
    get_failed(c, resloc, resesgf)


