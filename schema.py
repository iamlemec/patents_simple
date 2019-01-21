#!/usr/bin/env python3
# coding: UTF-8

# cn fields
cn_trans = {
    'patnum': '公开（公告）号', # Patent number
    'pubdate': '公开（公告）日', # Publication date
    'appnum': '申请号', # Application number
    'appdate': '申请日', # Application date
    'title': '名称', # Title
    'ipc1': '主分类号', # IPC code 1
    'ipc2': '分类号', # IPC code 2
    'appname': '申请（专利权）人', # Application name
    'invname': '发明（设计）人', # Inventor name
    'abstract': '摘要', # Abstract
    'claims': '主权项', # Independent claim
    'province': '国省代码', # Province code
    'address': '地址', # Address
    'agency': '专利代理机构', # Patent Agency
    'agent': '代理人', # Patent Agent
    'path': '发布路径', # Data Path
    'pages': '页数', # No. of Pages
    'country': '申请国代码', # Application Country
    'type': '专利类型', # Type of Patent
    'source': '申请来源', # Source
    'sipoclass': '范畴分类' # Classification by SIPO
}
cn_keys = list(cn_trans.keys())

# us fields
grant_keys = [
    'abstract', # Abstract
    # 'address', # Address
    'appdate', # Application date
    'appname', # Applicant name
    # 'appnum', # Application number
    'claims', # Independent claims
    'country', # Application Country
    'ipc1', # IPC code 1
    'ipc2', # IPC code 2
    'path', # Data Path
    'patnum', # Patent number
    'state', # State
    'city', # City
    'pubdate', # Publication date
    'title', # Title
    'gen', # USPTO data format
]

apply_keys = [
    'appdate', # Application date
    'pubdate', # Publication date
    'appnum', # Application number
    'pubnum', # Publication number
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
