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

import schema

# parse input arguments
parser = argparse.ArgumentParser(description='patent grant parser')
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
(fdir, fname) = os.path.split(args.path)
if fname.endswith('.dat'):
    gen = 1
elif fname.startswith('pgb'):
    gen = 2
elif fname.startswith('ipgb'):
    gen = 3
else:
    raise Exception('Unknown format')

# default values
skeys = sorted(schema.grant_keys)
nkeys = len(skeys)
default = OrderedDict([(k, None) for k in skeys])
default['gen'] = gen
default['path'] = fname

# database setup
if write:
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    if args.clobber:
        cur.execute('drop table if exists grant')
        cur.execute('drop index if exists idx_patnum')
    sig = ', '.join(['%s text' % k for k in skeys])
    cur.execute('create table if not exists grant (%s)' % sig)
    cur.execute('create unique index if not exists idx_patnum on grant (patnum)')

# storage
pats = []
cmd = 'insert or replace into grant values (%s)' % ','.join(['?' for _ in skeys])
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
            print('pat = %d'%n)
            for (k, v) in p.items():
                print('%s = %s' % (k, v))
            print()

    # break
    if args.limit > 0:
        if n >= args.limit:
            return False

    return True

# tools
def get_text(parent,tag,default=''):
    child = parent.find(tag)
    return (child.text or default) if child is not None else default

def gen1_ipc(ipc):
    if len(ipc) >= 8:
        return ipc[:4] + ipc[4:7].lstrip() + '/' + ipc[7:]
    else:
        return ipc

def gen2_ipc(ipc):
    if len(ipc) >= 8:
        return ipc[:4] + ipc[4:7].lstrip() + '/' + ipc[7:]
    else:
        return ipc

def gen3_ipc(ipcsec):
    for ipc in ipcsec.findall('classification-ipcr'):
        yield get_text(ipc,'section') + get_text(ipc,'class') \
            + get_text(ipc,'subclass') + get_text(ipc,'main-group') + '/' \
            + get_text(ipc,'subgroup')

def raw_text(par,sep=''):
    return sep.join(par.itertext())

def prune_patnum(pn):
    ret = re.match(r'([a-zA-Z]{1,2})?([0-9]+)', pn)
    if ret is None:
        prefix = ''
        patnum = pn
    else:
        prefix, patnum = ret.groups()
        prefix = '' if prefix is None else prefix
    patnum = patnum.lstrip('0')[:7]
    return prefix + patnum

# parse it up
print('Parsing %s, gen %d' % (fname, gen))
if gen == 1:
    pat = None
    sec = None
    tag = None
    ipclist = []
    for nline in chain(open(args.path, encoding='latin1'), ['PATN']):
        # peek at next line
        ntag, nbuf = nline[:4].rstrip(), nline[5:-1]
        if tag is None:
            tag = ntag
            buf = nbuf
            continue
        if ntag == '':
            buf += nbuf
            continue

        # regular tags
        if tag == 'PATN':
            if pat is not None:
                pat['ipc1'] = ipclist.pop(0) if len(ipclist) > 0 else ''
                pat['ipc2'] = ';'.join(ipclist)
                if not add_patent(pat):
                    break
            pat = copy(default)
            sec = 'PATN'
            ipclist = []
        elif tag in ['INVT', 'ASSG', 'PRIR', 'CLAS', 'UREF', 'FREF', 'OREF', 'LREP', 'PCTA', 'ABST']:
            sec = tag
        elif tag in ['PAL', 'PAR', 'PAC', 'PA0', 'PA1']:
            if sec == 'ABST':
                if pat['abstract'] is None:
                    pat['abstract'] = buf
                else:
                    pat['abstract'] += '\n' + buf
        elif tag == 'WKU':
            if sec == 'PATN':
                pat['patnum'] = prune_patnum(buf)
        elif tag == 'ISD':
            if sec == 'PATN':
                pat['pubdate'] = buf
        elif tag == 'APD':
            if sec == 'PATN':
                pat['appdate'] = buf
        elif tag == 'ICL':
            if sec == 'CLAS':
                ipclist.append(gen1_ipc(buf))
        elif tag == 'TTL':
            if sec == 'PATN':
                pat['title'] = buf
        elif tag == 'NCL':
            if sec == 'PATN':
                pat['claims'] = buf
        elif tag == 'NAM':
            if sec == 'ASSG':
                pat['appname'] = buf
        elif tag == 'CTY':
            if sec == 'ASSG':
                pat['city'] = buf.upper()
        elif tag == 'STA':
            if sec == 'ASSG':
                pat['state'] = buf
                pat['country'] = 'US'
        elif tag == 'CNT':
            if sec == 'ASSG':
                pat['country'] = buf[:2]

        # stage next tag and buf
        tag = ntag
        buf = nbuf
elif gen == 2:
    def handle_patent(elem):
        pat = copy(default)

        # top-level section
        bib = elem.find('SDOBI')

        # published patent
        pubref = bib.find('B100')
        pat['patnum'] = prune_patnum(get_text(pubref, 'B110/DNUM/PDAT'))
        pat['pubdate'] = get_text(pubref, 'B140/DATE/PDAT')

        # filing date
        appref = bib.find('B200')
        pat['appdate'] = get_text(appref, 'B220/DATE/PDAT')

        # ipc code
        patref = bib.find('B500')
        ipcsec = patref.find('B510')
        pat['ipc1'] = gen2_ipc(get_text(ipcsec, 'B511/PDAT'))
        pat['ipc2'] = ';'.join(gen2_ipc(get_text(child, 'PDAT')) for child in ipcsec.findall('B512'))

        # title
        pat['title'] = get_text(patref, 'B540/STEXT/PDAT')

        # claims
        pat['claims'] = get_text(patref, 'B570/B577/PDAT')

        # applicant name and address
        ownref = bib.find('B700/B730/B731/PARTY-US')
        if ownref is not None:
            pat['appname'] = get_text(ownref, 'NAM/ONM/STEXT/PDAT')
            address = ownref.find('ADR')
            if address is not None:
                pat['city'] = get_text(address, 'CITY/PDAT').upper()
                pat['state'] = get_text(address, 'STATE/PDAT')
                pat['country'] = get_text(address, 'CTRY/PDAT', default='US')

        # abstract
        abspars = elem.findall('SDOAB/BTEXT/PARA')
        if len(abspars) > 0:
            pat['abstract'] = '\n'.join([raw_text(e) for e in abspars])

        # roll it in
        return add_patent(pat)

    # parse mangled xml
    pp = XMLPullParser(tag='PATDOC', events=['end'], recover=True)
    def handle_all():
        for _, pat in pp.read_events():
            if not handle_patent(pat):
                return False
        return True

    with open(args.path, errors='ignore') as f:
        pp.feed('<root>\n')
        for line in f:
            if line.startswith('<?xml'):
                if not handle_all():
                    break
            elif line.startswith('<!DOCTYPE') or line.startswith('<!ENTITY') or line.startswith(']>'):
                pass
            else:
                pp.feed(line)
        else:
            pp.feed('</root>\n')
            handle_all()
elif gen == 3:
    def handle_patent(elem):
        pat = copy(default)

        # top-level section
        bib = elem.find('us-bibliographic-data-grant')
        pubref = bib.find('publication-reference')
        appref = bib.find('application-reference')

        # published patent
        pubinfo = pubref.find('document-id')
        pat['patnum'] = prune_patnum(get_text(pubinfo, 'doc-number'))
        pat['pubdate'] = get_text(pubinfo, 'date')

        # filing date
        pat['appdate'] = get_text(appref, 'document-id/date')

        # title
        pat['title'] = get_text(bib, 'invention-title')

        # ipc code
        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            ipclist = list(gen3_ipc(ipcsec))
            pat['ipc1'] = ipclist[0]
            pat['ipc2'] = ';'.join(ipclist)

        # claims
        pat['claims'] = get_text(bib, 'number-of-claims')

        # applicant name and address
        assignee = bib.find('assignees/assignee/addressbook')
        if assignee is not None:
            address = assignee.find('address')
            if address is not None:
                pat['appname'] = get_text(assignee, 'orgname')
                pat['state'] = get_text(address, 'state')
                pat['country'] = get_text(address, 'country')

        # abstract
        abspar = elem.find('abstract')
        if abspar is not None:
            pat['abstract'] = raw_text(abspar, sep=' ')

        # roll it in
        return add_patent(pat)

    # parse mangled xml
    pp = XMLPullParser(tag='us-patent-grant', events=['end'], recover=True)
    def handle_all():
        for _, pat in pp.read_events():
            if not handle_patent(pat):
                return False
        return True

    with open(args.path, errors='ignore') as f:
        pp.feed('<root>\n')
        for line in f:
            if line.startswith('<?xml'):
                if not handle_all():
                    break
            elif line.startswith('<!DOCTYPE'):
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

print('Found %d patents' % n)
