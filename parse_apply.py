#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import argparse
import sqlite3
from lxml.etree import iterparse, tostring, XMLPullParser
from copy import copy
from collections import OrderedDict
from itertools import chain

from parse_tools import *

# parse input arguments
parser = argparse.ArgumentParser(description='patent application parser')
parser.add_argument('path', type=str, help='path of file to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--clobber', action='store_true', help='delete database and restart')
parser.add_argument('--output', type=int, default=0, help='print out patents per')
parser.add_argument('--limit', type=int, default=0, help='only parse n patents')
parser.add_argument('--chunk', type=int, default=1000, help='chunk insert size')
args = parser.parse_args()

# for later
write = args.db is not None

# detect generation
fdir, fname = os.path.split(args.path)
if fname.startswith('pab'):
    gen = 2
elif fname.startswith('ipab'):
    gen = 3
else:
    raise Exception('Unknown format')

apply_keys = [
    'appdate', # Application date
    'pubdate', # Publication date
    'appnum', # Application number
    # 'pubnum', # Publication number
    'ipcver', # IPC version
    'ipc1', # IPC code 1
    'ipc2', # IPC code 2
    'appname', # Applicant name
    'path', # Data Path
    # 'address', # Address
    'city', # City
    'state', # State
    'country', # Application Country
    'title', # Title
    'abstract', # Abstract
    'gen', # USPTO data format
]

# default values
skeys = sorted(apply_keys)
nkeys = len(skeys)
default = OrderedDict([(k, None) for k in skeys])
default['gen'] = gen
default['path'] = fname

# database setup
if write:
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    if args.clobber:
        cur.execute('drop table if exists apply')
        cur.execute('drop index if exists idx_appnum')
    sig = ', '.join([f'{k} text' for k in skeys])
    cur.execute(f'create table if not exists apply ({sig})')
    cur.execute('create unique index if not exists idx_appnum on apply (appnum)')

# storage
pats = []
qsig = ','.join(['?' for _ in skeys])
cmd = f'insert or replace into apply values ({qsig})'
def commit_patents():
    cur.executemany(cmd, pats)
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
        if len(pats) >= args.chunk:
            commit_patents()

    # output
    if args.output > 0:
        if n % args.output == 0:
            print(f'pat = {n}')
            for (k, v) in p.items():
                print(f'{k} = {v}')
            print()

    # break
    if args.limit > 0:
        if n >= args.limit:
            return False

    return True

# parse it up
print(f'Parsing {fname}, gen {gen}')
if gen == 2:
    main_tag = 'patent-application-publication'

    def handle_patent(elem):
        pat = copy(default)

        # top-level section
        bib = elem.find('subdoc-bibliographic-information')

        # publication data
        pub = bib.find('document-id')
        if pub is not None:
            pat['pubdate'] = get_text(pub, 'document-date')

        # application data
        app = bib.find('domestic-filing-data')
        if app is not None:
            pat['appnum'] = get_text(app, 'application-number/doc-number')
            pat['appdate'] = get_text(app, 'filing-date')

        # title
        tech = bib.find('technical-information')
        pat['title'] = get_text(tech, 'title-of-invention')

        # ipc code
        ipcsec = tech.find('classification-ipc')
        if ipcsec is not None:
            pat['ipcver'] = get_text(ipcsec, 'classification-ipc-edition').lstrip('0')
            ipclist = list(gen2_ipc(ipcsec))
            pat['ipc1'] = ipclist.pop(0) if len(ipclist) > 0 else ''
            pat['ipc2'] = ';'.join(ipclist)

        # assignee information
        pat['appname'] = get_text(bib, 'assignee/organization-name')

        # first inventor address
        resid = bib.find('inventors/first-named-inventor/residence')
        if resid is not None:
            address = resid.find('residence-us')
            if address is None:
                address = resid.find('residence-non-us')
            if address is not None:
                pat['city'] = get_text(address, 'city')
                pat['state'] = get_text(address, 'state')
                pat['country'] = get_text(address, 'country-code')

        # abstract
        abst = elem.find('subdoc-abstract')
        if abst is not None:
            pat['abstract'] = raw_text(abst, sep=' ')

        # roll it in
        return add_patent(pat)
elif gen == 3:
    main_tag = 'us-patent-application'

    def handle_patent(elem):
        pat = copy(default)

        # top-level section
        bib = elem.find('us-bibliographic-data-application')
        pubref = bib.find('publication-reference')
        appref = bib.find('application-reference')

        # published patent
        pubinfo = pubref.find('document-id')
        pat['pubdate'] = get_text(pubinfo, 'date')

        # filing date
        pat['appnum'] = get_text(appref, 'document-id/doc-number')
        pat['appdate'] = get_text(appref, 'document-id/date')
        pat['appname'] = get_text(bib, 'assignees/assignee/addressbook/orgname')

        # title
        pat['title'] = get_text(bib, 'invention-title')

        # ipc code
        ipclist = []
        ipcsec = bib.find('classification-ipc')
        if ipcsec is not None:
            pat['ipcver'] = get_text(ipcsec, 'edition').lstrip('0')
            ipclist = list(gen3a_ipc(ipcsec))
        else:
            ipcsec = bib.find('classifications-ipcr')
            if ipcsec is not None:
                pat['ipcver'] = get_text(ipcsec, 'classification-ipcr/ipc-version-indicator/date')
                ipclist = list(gen3r_ipc(ipcsec))
        pat['ipc1'] = ipclist.pop(0) if len(ipclist) > 0 else ''
        pat['ipc2'] = ';'.join(ipclist)

        # first inventor address
        address = bib.find('parties/applicants/applicant/addressbook/address')
        if address is None:
            address = bib.find('us-parties/us-applicants/us-applicant/addressbook/address')
        if address is not None:
            pat['city'] = get_text(address, 'city')
            pat['state'] = get_text(address, 'state')
            pat['country'] = get_text(address, 'country')

        # abstract
        abspar = elem.find('abstract')
        if abspar is not None:
            pat['abstract'] = raw_text(abspar, sep=' ')

        # roll it in
        return add_patent(pat)

# parse mangled xml
pp = XMLPullParser(tag=main_tag, events=['end'], recover=True)
def handle_all():
    for (_, pat) in pp.read_events():
        if not handle_patent(pat):
            return False
    return True

with open(args.path, errors='ignore') as f:
    pp.feed('<root>\n')
    for line in f:
        if line.startswith('<?xml'):
            if not handle_all():
                break
        elif line.startswith('<!DOCTYPE') or line.startswith('<!ENTITY'):
            pass
        else:
            pp.feed(line)
    else:
        pp.feed('</root>\n')
        handle_all()

if write:
    # commit to db and close
    commit_patents()
    cur.close()
    con.close()

print(f'Found {n} patents')
