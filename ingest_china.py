#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import argparse
import sqlite3
from copy import copy
from collections import OrderedDict

import schema

# parse input arguments
parser = argparse.ArgumentParser(description='China patent parser.')
parser.add_argument('path',type=str,help='path of file to parse')
parser.add_argument('--db',type=str,default=None,help='database file to store to')
parser.add_argument('--clobber',action='store_true',help='delete database and restart')
parser.add_argument('--output',type=int,default=0,help='print out patents per')
parser.add_argument('--limit',type=int,default=0,help='only parse n patents')
parser.add_argument('--chunk',type=int,default=1000,help='chunk insert size')
args = parser.parse_args()

# for later
write = args.db is not None
pper = args.output
limit = args.limit
chunk = args.chunk

# reverse dict
rtrans = {v:k for (k,v) in schema.cn_trans.items()}

# default values
skeys = sorted(schema.cn_keys)
nkeys = len(skeys)
default = OrderedDict([(k,None) for k in skeys])

# database setup
if write:
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    if args.clobber:
        cur.execute('drop table if exists patent')
        cur.execute('drop index if exists idx_patnum')
    sig = ', '.join(['%s text'%k for k in skeys])
    cur.execute('create table if not exists patent (%s)'%sig)
    cur.execute('create unique index if not exists idx_patnum on patent (patnum)')

# storage
pats = []
cmd = 'insert or replace into patent values (%s)' % ','.join(['?' for _ in skeys])
def commit_patents():
    cur.executemany(cmd,pats)
    con.commit()
    del(pats[:])

# chunking express
n = 0
def add_patent(p):
    global n
    n += 1

    # storage
    if write:
        pats.append(list(p.values()))
        if len(pats) >= chunk:
            commit_patents()

    # output
    if pper > 0:
        if n % pper == 0:
            print('pat = %d'%n)
            for (k,v) in p.items():
                print('%s = %s'%(k,v))
            print()

    # break
    if limit > 0:
        if n >= limit:
            return False

    return True

# parse file
n = 0
pat = None
for (i,line) in enumerate(open(args.path,encoding='gb2312',errors='ignore')):
    # skip empty lines
    line = line.strip()
    if len(line) == 0:
        continue

    # start patent
    if line == '<REC>':
        # store current
        if pat is not None:
            if not add_patent(pat):
                break

        # set defaults
        pat = copy(default)

        # clear buffer
        tag = None
        buf = None

        continue

    # start tag
    ret = re.match('<([^\x00-\x7F][^>]*)>=(.*)',line)
    if ret:
        # store old
        if tag in rtrans:
            k = rtrans[tag]
            pat[k] = buf

        # start new
        (tag,buf) = ret.groups()
    else:
        # continue existing
        buf += line

if write:
    # close database
    commit_patents()
    cur.close()
    con.close()
