# name matching using locality-sensitive hashing (simhash)
# these are mostly idempotent

from itertools import chain, repeat, product
from collections import defaultdict
from math import ceil

import sqlite3
import numpy as np
import pandas as pd
import networkx as nx
try:
    from distance.cdistance import levenshtein
except:
    from distance import levenshtein

import simhash as sh

# standardize firm name
def name_standardize(name):
    name = re.sub(r'\'S|\(.*\)|\.', ' ', name)
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'[ ]{2,}', ' ', name)
    return name.strip()

def generate_names(con):
    print('generating owner names')

    apply = pd.read_sql('select patnum,appname from apply', con)
    grant = pd.read_sql('select patnum,appname from grant', con)

    apply['name'] = apply['appname'].apply(name_standardize)
    grant['name'] = grant['appname'].apply(name_standardize)

    names = pd.concat([apply['name'], grant['name']], ignore_index=True)
    names = names.rename('name').rename_axis('id').reset_index()
    names.to_sql('names', con)

    con.commit()

# k = 8, thresh = 4 works well
def owner_cluster(con, cur, nitem=None, reverse=True, nshingle=2, store=True, **kwargs):
    print('generating hashes and pairs')

    c = sh.Cluster(**kwargs)

    cmd = 'select ownerid,name from owner'
    if reverse:
        cmd += ' order by rowid desc'
    if nitem:
        cmd += ' limit %i' % nitem

    name_dict = {}
    for (i,(ownerid,name)) in enumerate(cur.execute(cmd)):
        words = name.split()
        shings = list(sh.shingle(name,nshingle))

        features = shings + words
        weights = list(np.linspace(1.0,0.0,len(shings))) + list(np.linspace(1.0,0.0,len(words)))

        c.add(features,weights=weights,label=ownerid)
        name_dict[ownerid] = name

        if i%10000 == 0:
            print(i)

    ipairs = c.unions
    npairs = [(name_dict[i1],name_dict[i2]) for (i1,i2) in ipairs]
    print('Found %i pairs' % len(ipairs))

    if store:
        cur.execute('drop table if exists pair')
        cur.execute('create table pair (ownerid1 int, ownerid2 int, name1 text, name2 text)')
        cur.executemany('insert into pair values (?,?,?,?)',[(o1,o2,n1,n2) for ((o1,o2),(n1,n2)) in zip(ipairs,npairs)])
        con.commit()
    else:
        return (ipairs,npairs)

# compute distances on owners in same cluster
def find_components(con, cur, thresh=0.85, store=True):
    print('finding firm components')

    cmd = 'select * from pair'

    def dmetr(name1,name2):
        maxlen = max(len(name1),len(name2))
        ldist = levenshtein(name1,name2,max_dist=int(ceil(maxlen*(1.0-thresh))))
        return (1.0 - float(ldist)/maxlen) if (ldist != -1 and maxlen != 0) else 0.0

    dists = []
    close = []
    name_dict = {}
    name_std = {}

    for (o1,o2,n1,n2) in cur.execute(cmd):
        if o1 not in name_dict:
            n1s = name_standardize_strong(n1)
            name_dict[o1] = n1
            name_std[o1] = n1s
        else:
            n1s = name_std[o1]
        if o2 not in name_dict:
            n2s = name_standardize_strong(n2)
            name_dict[o2] = n2
            name_std[o2] = n2s
        else:
            n2s = name_std[o2]

        d = dmetr(n1s,n2s)

        dists.append((o1,o2,d))
        if d > thresh:
            close.append((o1,o2))

    G = nx.Graph()
    G.add_edges_from(close)
    comps = sorted(nx.connected_components(G),key=len,reverse=True)

    if store:
        cur.execute('drop table if exists component')
        cur.execute('create table component (compid int, ownerid int)')
        cur.executemany('insert into component values (?,?)',chain(*[zip(repeat(cid),comp) for (cid,comp) in enumerate(comps)]))
        con.commit()
    else:
        comp_names = [[name_std[id] for id in ids] for ids in comps]
        return comp_names

# must be less than 1000000 components
def merge_components(con, cur):
    print('merging firm components')

    print('matching owners to firms')
    cur.execute('drop table if exists owner_firm')
    cur.execute('create table owner_firm (ownerid int, firm_num int)')
    cur.execute('insert into owner_firm select ownerid,compid+1000000 from owner left join component using(ownerid)')
    cur.execute('update owner_firm set firm_num=ownerid where firm_num is null')

    print('merging into compustat')
    cur.execute('drop table if exists compustat_merge')
    cur.execute("""create table compustat_merge as select compustat.*,compustat_owner.ownerid,owner_firm.firm_num
                   from compustat left join compustat_owner using(gvkey,year)
                   left join owner_firm using(ownerid)""")

    print('merging into patents')
    cur.execute('drop table if exists patent_merge')
    cur.execute("""create table patent_merge as select patent.*,patent_owner.ownerid,owner_firm.firm_num
                   from patent left join patent_owner using(patnum)
                   left join owner_firm using(ownerid)""")

    print('merging into assignments')
    cur.execute('drop table if exists assign_merge')
    cur.execute("""create table assign_merge as select assign_use.*,assign_owner.assigneeid,assign_owner.assignorid,assignee_firm.firm_num as dest_fn,assignor_firm.firm_num as source_fn
                   from assign_use left join assign_owner on assign_use.assignid=assign_owner.assignid
                   left join owner_firm as assignee_firm on assign_owner.assigneeid=assignee_firm.ownerid
                   left join owner_firm as assignor_firm on assign_owner.assignorid=assignor_firm.ownerid""")

    print('generating simplified patents')
    cur.execute('drop table if exists patent_basic')
    cur.execute('create table patent_basic (patnum text, firm_num int, fileyear int, grantyear int, state text, country text, ipc text, ipcver text)')
    cur.execute("insert into patent_basic select patnum,firm_num,substr(filedate,1,4),substr(grantdate,1,4),state,country,ipc,ipcver from patent_merge")
    cur.execute('create unique index patent_basic_idx on patent_basic(patnum)')

    print('generating simplified assignments')
    cur.execute('drop table if exists assign_info')
    cur.execute('create table assign_info (assignid integer primary key, patnum int, source_fn int, dest_fn int, execyear int, recyear int, state text, country text)')
    cur.execute("insert into assign_info select assignid,patnum,source_fn,dest_fn,substr(execdate,1,4),substr(recdate,1,4),assignee_state,assignee_country from assign_merge where typeof(patnum) is 'integer'")

    print('generating assignments at transaction level')
    cur.execute('drop table if exists assign_bulk')
    cur.execute('create table assign_bulk (source_fn int, dest_fn int, execyear int, ntrans int)')
    cur.execute('insert into assign_bulk select source_fn,dest_fn,execyear,count(*) from assign_info group by source_fn,dest_fn,execyear')

    con.commit()

if __name__ == "__main__":
    import argparse

    # parse input arguments
    parser = argparse.ArgumentParser(description='Create firm name clusters.')
    parser.add_argument('--db', type=str, default=None, help='database file to store to')
    args = parser.parse_args()

    # open database
    con = sqlite3.connect(args.db)

    # go through steps
    generate_names(con)
    owner_cluster(con, cur)
    find_components(con, cur)
    merge_components(con, cur)
