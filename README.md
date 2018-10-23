# Patents

Multi-country patent parser.

## Data

To load Chinese patent data

```bash
ls CN_DATA_DIR/FM_*.trs | sort | xargs -n 1 python3 ingest_cn.py --db=store/patents_cn.db --output=10000
```

This will store the results in `patents_cn.db` and output every 10000th patent as a progress update.

To load US patent data, which is broken into many smaller files

```bash
ls $US_DATA_DIR/grant_files/*.dat | sort | xargs -n 1 python3 ingest_grant.py --db=store/patents_us.db
ls $US_DATA_DIR/grant_files/pgb*.xml | sort | xargs -n 1 python3 ingest_grant.py --db=store/patents_us.db
ls $US_DATA_DIR/grant_files/ipgb*.xml | sort | xargs -n 1 python3 ingest_grant.py --db=store/patents_us.db
ls $US_DATA_DIR/apply_files/pab*.xml | sort | xargs -n 1 python3 ingest_apply.py --db=store/patents_us.db
ls $US_DATA_DIR/apply_files/ipab*.xml | sort | xargs -n 1 python3 ingest_apply.py --db=store/patents_us.db
```

