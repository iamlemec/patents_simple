# name matching using locality-sensitive hashing (simhash)
# these are mostly idempotent

from itertools import chain, repeat
from math import ceil

import re
import sqlite3
import numpy as np
import pandas as pd
import networkx as nx
try:
    from distance.cdistance import levenshtein
except:
    from distance import levenshtein

import simhash as sh
from standardize import standardize_weak, standardize_strong

def unique_names(con):
    print('generating names')

    apply = pd.read_sql('select appnum,appname from apply where appname is not null', con)
    grant = pd.read_sql('select patnum,appname from grant where appname is not null', con)

    apply = apply[apply['appname'].str.len()>0]
    grant = grant[grant['appname'].str.len()>0]

    apply['name'] = apply['appname'].apply(standardize_weak)
    grant['name'] = grant['appname'].apply(standardize_weak)

    names = pd.concat([apply['name'], grant['name']]).drop_duplicates().reset_index(drop=True)
    names = names.rename('name').rename_axis('id').reset_index()
    names.to_sql('name', con, index=False, if_exists='replace')

    apply = pd.merge(apply, names, how='left', on='name')
    grant = pd.merge(grant, names, how='left', on='name')

    apply[['appnum', 'id']].to_sql('apply_match', con, index=False, if_exists='replace')
    grant[['patnum', 'id']].to_sql('grant_match', con, index=False, if_exists='replace')

    con.commit()
    print(f'found {len(names)} names')

# k = 8, thresh = 4 works well
def filter_pairs(con, nshingle=2, k=8, thresh=4):
    print('filtering pairs')

    c = sh.Cluster(k=k, thresh=thresh)
    name_dict = {}

    names = pd.read_sql('select id,name from name', con)
    for i, id, name in names.itertuples():
        words = name.split()
        shings = list(sh.shingle(name, nshingle))

        features = shings + words
        weights = list(np.linspace(1.0, 0.0, len(shings))) + list(np.linspace(1.0, 0.0, len(words)))

        c.add(features, weights=weights, label=id)
        name_dict[id] = name

        if i > 0 and i % 100_000 == 0:
            print(f'{i}: {len(c.unions)}')

    pairs = pd.DataFrame([(i1, i2, name_dict[i1], name_dict[i2]) for i1, i2 in c.unions], columns=['id1', 'id2', 'name1', 'name2'])
    pairs.to_sql('pair', con, index=False, if_exists='replace')

    con.commit()
    print(f'found {len(pairs)} pairs')

# compute distances on owners in same cluster
def find_groups(con, thresh=0.85):
    print('finding matches')

    def dmetr(name1, name2):
        max_len = max(len(name1), len(name2))
        max_dist = int(ceil(max_len*(1.0-thresh)))
        ldist = levenshtein(name1, name2, max_dist=max_dist)
        return (1.0 - float(ldist)/max_len) if (ldist != -1 and max_len != 0) else 0.0

    dists = []
    close = []
    name_dict = {}
    name_std = {}

    pairs = pd.read_sql('select id1,id2,name1,name2 from pair', con)
    for i, id1, id2, name1, name2 in pairs.itertuples():
        if id1 not in name_dict:
            n1std = standardize_strong(name1)
            name_dict[id1] = name1
            name_std[id1] = n1std
        else:
            n1std = name_std[id1]
        if id2 not in name_dict:
            n2std = standardize_strong(name2)
            name_dict[id2] = name2
            name_std[id2] = n2std
        else:
            n2std = name_std[id2]

        d = dmetr(n1std, n2std)

        dists.append((id1, id2, d))
        if d > thresh:
            close.append((id1, id2))

        if i > 0 and i % 100_000 == 0:
            print(f'{i}: {len(close)}')

    G = nx.Graph()
    G.add_edges_from(close)
    comps = sorted(nx.connected_components(G), key=len, reverse=True)

    cmap = pd.DataFrame(chain(*[zip(repeat(fid), ids) for fid, ids in enumerate(comps)]), columns=['firm_num', 'id'])
    cmap.to_sql('match', con, index=False, if_exists='replace')

    con.commit()
    print(f'found {len(comps)} groups')

# must be less than 1000000 components
def merge_firms(con, base=1_000_000):
    print('merging firms')

    names = pd.read_sql('select * from name', con)
    match = pd.read_sql('select * from match', con)
    firms = pd.merge(names, match, how='left', on='id')
    firms['firm_num'] = firms['firm_num'].fillna(firms['id']+base).astype(np.int)
    firms[['firm_num', 'id']].to_sql('firm', con, index=False, if_exists='replace')

    apply = pd.read_sql('select * from apply_match', con)
    grant = pd.read_sql('select * from grant_match', con)

    apply = pd.merge(apply, firms, on='id')
    grant = pd.merge(grant, firms, on='id')

    apply[['appnum', 'firm_num']].to_sql('apply_firm', con, index=False, if_exists='replace')
    grant[['patnum', 'firm_num']].to_sql('grant_firm', con, index=False, if_exists='replace')

    con.commit()

def get_groups(con):
    return pd.read_sql('select * from match join name on match.id=name.id order by firm_num', con)

if __name__ == "__main__":
    import argparse

    # parse input arguments
    parser = argparse.ArgumentParser(description='Create firm name clusters.')
    parser.add_argument('--db', type=str, default=None, help='database file to store to')
    args = parser.parse_args()

    # go through steps
    with sqlite3.connect(args.db) as con:
        unique_names(con)
        filter_pairs(con)
        find_groups(con)
        merge_firms(con)
