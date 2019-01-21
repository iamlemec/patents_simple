#!/usr/bin/env python3
# coding: UTF-8

import argparse
import sqlite3
from mectools import db
import mectools.hyper as hy

parser = argparse.ArgumentParser(description='IPC code mapper.')
parser.add_argument('--db', type=str, help='database file to store to')
parser.add_argument('--clobber', action='store_true', help='delete old database')
parser.add_argument('--ptype', type=str, default='apply', help='whether to do applications or grants')
parser.add_argument('--chunk', type=int, default=100000, help='chunk size to fetch')
args = parser.parse_args()

# open db
con = db.connect(args.db)

# apply or grant
out_table = f'ipc_{args.ptype}'
inp_table = args.ptype
patid = 'appnum' if args.ptype == 'apply' else 'patnum'

# handle init
if args.clobber:
    con.delete(out_table)
    con.create(out_table, [(patid, 'text'), ('version', 'text'), ('code', 'text')])

# ipc generator
def gen_ipc(df):
    for i, (pn, vr, i1, i2) in df.iterrows():
        if i1 is not None and len(i1) > 0:
            yield pn, vr, i1
        if i2 is not None and len(i2) > 0:
            for i in i2.split(';'):
                yield pn, vr, i

# tools
tot = 0
for df in hy.progress(con.table(inp_table, columns=[patid, 'ipcver', 'ipc1', 'ipc2'], chunksize=args.chunk), per=1):
    ipcs = gen_ipc(df)
    con.insert(out_table, ipcs, n=3)

# clean up
con.commit()
con.close()

