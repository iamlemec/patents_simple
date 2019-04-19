#!/usr/bin/env bash

ls $1/grant_files/*.dat | sort | xargs -n 1 python3 parse_grant.py --db $2
ls $1/grant_files/pgb*.xml | sort | xargs -n 1 python3 parse_grant.py --db $2
ls $1/grant_files/ipgb*.xml | sort | xargs -n 1 python3 parse_grant.py --db $2
ls $1/apply_files/pab*.xml | sort | xargs -n 1 python3 parse_apply.py --db $2
ls $1/apply_files/ipab*.xml | sort | xargs -n 1 python3 parse_apply.py --db $2
