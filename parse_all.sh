#!/usr/bin/env bash

ls data/grant/*.dat | sort | xargs -n 1 python3 parse_grant.py
ls data/grant/pgb*.xml | sort | xargs -n 1 python3 parse_grant.py
ls data/grant/ipgb*.xml | sort | xargs -n 1 python3 parse_grant.py
ls data/apply/pab*.xml | sort | xargs -n 1 python3 parse_apply.py
ls data/apply/ipab*.xml | sort | xargs -n 1 python3 parse_apply.py
