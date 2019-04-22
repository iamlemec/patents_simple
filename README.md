# Patents

Simple patent parser.

## Data

To load US patent data, which is broken into many smaller files

```bash
ls $US_DATA_DIR/grant_files/*.dat | sort | xargs -n 1 python3 parse_grant.py --db=store/patents_us.db
ls $US_DATA_DIR/grant_files/pgb*.xml | sort | xargs -n 1 python3 parse_grant.py --db=store/patents_us.db
ls $US_DATA_DIR/grant_files/ipgb*.xml | sort | xargs -n 1 python3 parse_grant.py --db=store/patents_us.db
ls $US_DATA_DIR/apply_files/pab*.xml | sort | xargs -n 1 python3 parse_apply.py --db=store/patents_us.db
ls $US_DATA_DIR/apply_files/ipab*.xml | sort | xargs -n 1 python3 parse_apply.py --db=store/patents_us.db
```

## Processing

You can generate an IPC level table with `gen_ipc.py`. To generate reduced and stemmed (requires NLTK) abstract texts, run `gen_text.py`.

## Performance

| routine | time | memory |
|---------|------|--------|
| `cluster.unique_names` | 57s | 2 GB |
| `cluster.filter_pairs` | | 32 GB |
