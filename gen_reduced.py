import re
import sqlite3
import argparse
import nltk
from mectools.hyper import progress
from nltk.stem import WordNetLemmatizer

nltk.data.path.append('/media/Solid/data/nltk')

# parse input arguments
parser = argparse.ArgumentParser(description='Reduce text to purest form.')
parser.add_argument('--data', type=str, default=None, help='patent database')
args = parser.parse_args()

def reduce_text(text):
    text = re.sub(r'[^a-zA-Z\n]', ' ', text) # remove non-text or newlines
    text = re.sub(r'\n', ' ', text) # flatten
    text = re.sub(r' {2,}', ' ', text) # compress spaces
    return text.lower().strip() # to lowercase and trim

wnl = WordNetLemmatizer()
def lemmatize(text):
    return ' '.join([wnl.lemmatize(s) for s in text.split()])

with sqlite3.connect(args.data) as con:
    print('loading patent text')
    cur = con.cursor()
    cur.execute('select patnum,abstract from patent where abstract is not null')
    pat_idee, pat_text = map(list, zip(*cur.fetchall()))

    print('reducing patent text')
    pat_redu = [reduce_text(s) for s in progress(pat_text, per=100_000)]
    print('lemmatizing patent text')
    pat_stem = [lemmatize(s) for s in progress(pat_redu, per=100_000)]

    print('storing patent text')
    cur.execute('drop table if exists reduced')
    cur.execute('create table reduced (patnum text, abstract text)')
    cur.execute('create unique index if not exists idx_patnum on patent (patnum)')
    cur.executemany('insert or replace into reduced values (?,?)', zip(pat_idee, pat_stem))
