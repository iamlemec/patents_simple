# common parsing tools

import re

##
## text tools
##

def get_text(parent, tag, default=''):
    child = parent.find(tag)
    return (child.text or default) if child is not None else default

def raw_text(par, sep=''):
    return sep.join(par.itertext()).strip()

##
## patnum pruners
##

def prune_patnum(pn):
    ret = re.match(r'([a-zA-Z]{1,2}|0)?([0-9]+)', pn)
    if ret is None:
        prefix = ''
        patnum = pn
    else:
        prefix, patnum = ret.groups()
        prefix = '' if prefix is None else prefix
    patnum = patnum[:7].lstrip('0')
    return prefix + patnum

##
## ipc parsers
##

# early grant only (1, 1.5)
def pad_ipc(ipc):
    if len(ipc) >= 8:
        return ipc[:4] + ipc[4:7].replace(' ','0') + '/' + ipc[7:]
    else:
        return ipc

# grant only (1.5)
def gen15_ipc(ipcsec):
    yield pad_ipc(get_text(ipcsec, 'B511/PDAT'))
    for ipc in ipcsec.findall('B512'):
        yield pad_ipc(get_text(ipc, 'PDAT'))

# apply only (2)
def gen2_ipc(ipcsec):
    yield get_text(ipcsec, 'classification-ipc-primary/ipc')
    for ipc in ipcsec.findall('classification-ipc-secondary'):
        yield get_text(ipc, 'ipc')

# apply and grant (3)
def gen3a_ipc(ipcsec):
    yield get_text(ipcsec, 'main-classification')
    for ipc in ipcsec.findall('further-classification'):
        yield ipc.text or ''

def gen3g_ipc(ipcsec):
    yield pad_ipc(get_text(ipcsec, 'main-classification'))
    for ipc in ipcsec.findall('further-classification'):
        yield pad_ipc(ipc.text or '')

# apply and grant (3)
def gen3r_ipc(ipcsec):
    for ipc in ipcsec.findall('classification-ipcr'):
        yield get_text(ipc, 'section') + get_text(ipc, 'class') + get_text(ipc, 'subclass') \
            + get_text(ipc, 'main-group').zfill(3) + '/' + get_text(ipc, 'subgroup')
