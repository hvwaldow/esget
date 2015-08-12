import numpy as np
import matplotlib.pyplot as plt
import apsw


def connect(dbname):
    conn = apsw.Connection(dbname)
    c = conn.cursor()
    return((conn, c))

dbname = "/net/bio/c2sm/hvwaldow/esget_cordex/db/cordex_eur_ALL.db"
conn, c = connect(dbname)

# experiments = [x[0] for x in c.execute("SELECT DISTINCT experiment from localfiles").fetchall()]
experiments = [u'evaluation', u'historical', u'rcp26', u'rcp45', u'rcp85']

domains = ["EUR-11", "EUR-44"]

institutes = [x[0] for x in c.execute("SELECT DISTINCT institute from localfiles").fetchall()]


local11=[]
for ex in experiments:
    local11.append(c.execute('''SELECT count(*) from localfiles
    WHERE experiment == "{0}" AND unlink_date == '0' AND
    domain LIKE "EUR-11%"'''.format(ex)).fetchall()[0][0])

local44=[]
for ex in experiments:
    local44.append(c.execute('''SELECT count(*) from localfiles
    WHERE experiment == "{0}" AND unlink_date == '0' AND
    domain LIKE "EUR-44%"'''.format(ex)).fetchall()[0][0])

esgf11 = []
for ex in experiments:
    esgf11.append(c.execute('''SELECT count(*) from esgffiles
    WHERE experiment == "{0}" AND
    domain LIKE "EUR-11%"'''.format(ex)).fetchall()[0][0])

esgf44 = []
for ex in experiments:
    esgf44.append(c.execute('''SELECT count(*) from esgffiles
    WHERE experiment == "{0}" AND
    domain LIKE "EUR-44%"'''.format(ex)).fetchall()[0][0])

ind = np.arange(len(experiments))
width = 0.35
fig, ax = plt.subplots()


rects11loc = plt.bar(ind, local11, width, color="b", label="EUR-11 (local)")
rects11esgf = plt.bar(ind, np.array(esgf11)-np.array(local11), width, color='0.5', bottom=local11)

rects44loc = plt.bar(ind + width, local44, width, color="r",
                     label="EUR-44 (local)")
rests44esgf = plt.bar(ind+width, np.array(esgf44) - np.array(local44), width,
                      color='0.5', bottom=local44, label="avail @ ESGF")
plt.legend(loc='upper left', fontsize=26)
plt.xticks(ind + width, ('eval', 'hist', 'rcp26', 'rcp45', 'rcp85'), fontsize=26)
fig.canvas.draw()
ylabels = np.array([int(item.get_text())/1000 for item in ax.get_yticklabels()])
ylabels = np.array(ylabels, dtype="S4")
ax.set_yticklabels(ylabels)
plt.yticks(fontsize=26)
ax.set_ylabel('# files [X 1000]', fontsize=26)

plt.tight_layout()
plt.show()
