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
ls $US_DATA_DIR/*.dat | sort | xargs -n 1 python3 ingest_us.py --db=store/patents_us.db
ls $US_DATA_DIR/pgb*.xml | sort | xargs -n 1 python3 ingest_us.py --db=store/patents_us.db
ls $US_DATA_DIR/ipgb*.xml | sort | xargs -n 1 python3 ingest_us.py --db=store/patents_us.db
```

This takes care of all three data formats historically used by the USPTO. To generate patent-IPC mappings, execute

```bash
python3 star_ipc.py --db=store/patents_us.db --psel="0%" --level=4
python3 star_ipc.py --db=store/patents_cn.db --psel="%" --level=4
```

To generate the list of clean IPC codes, run

```bash
python3 gen_clean.py
```

in the `data` directory.
