import re
import sqlite3
import argparse
import nltk
from mectools.hyper import progress
from nltk.stem import WordNetLemmatizer

nltk.data.path.append('/media/Solid/data/nltk')

# parse input arguments
parser = argparse.ArgumentParser(description='Reduce text to purest form.')
parser.add_argument('--db', type=str, help='patent database')
parser.add_argument('--chunk', type=int, default=100_000, help='chunking size for reading')
args = parser.parse_args()

def tostr(s):
    return s if s is not None else ''

def reduce_text(text):
    text = re.sub(r'[^a-zA-Z\n]', ' ', text) # remove non-text or newlines
    text = re.sub(r'\n', ' ', text) # flatten
    text = re.sub(r' {2,}', ' ', text) # compress spaces
    return text.lower().strip() # to lowercase and trim

wnl = WordNetLemmatizer()
def lemmatize(text):
    return ' '.join([wnl.lemmatize(s) for s in text.split()])

with sqlite3.connect(args.db) as con:
    print('loading patent text')
    cur1, cur2 = con.cursor(), con.cursor()
    cur1.execute(f'drop table if exists apply_reduced')
    cur1.execute(f'create table apply_reduced (appnum text, text text)')
    cur1.execute(f'create unique index if not exists idx_apply_reduced on apply_reduced (appnum)')

    rows = 0
    ret = cur2.execute(f'select appnum,title,abstract from apply')
    while True:
        data = ret.fetchmany(args.chunk)
        if len(data) == 0:
            break
        rows += len(data)
        print(rows)

        pat_idee, pat_title, pat_abstr = map(list, zip(*data))
        pat_text = [tostr(pt)+'\n'+tostr(pa) for pt, pa in zip(pat_title, pat_abstr)]
        pat_redu = [reduce_text(s) for s in pat_text]
        pat_stem = [lemmatize(s) for s in pat_redu]

        cur1.executemany(f'insert or replace into apply_reduced values (?,?)', zip(pat_idee, pat_stem))
