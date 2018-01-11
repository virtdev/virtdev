# history.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import ast
import json
from lib.types import *
from datetime import datetime
from conf.log import LOG_HISTORY
from lib.log import log_debug, log_err
from interface.counterdb import CounterDB

CF_CNT = 'cf0'
CF_KEY = 'cf1'
CF_DATE = 'cf2'
CF_VALUE = 'cf3'
HISTORY = 'history'
COL_KEY = CF_KEY + ':k'
COL_CNT = CF_CNT + ':c'
HEAD_DATE = CF_DATE + ':'
HEAD_VALUE = CF_VALUE + ':'
LEN_HEAD_DATE = len(HEAD_DATE)
LEN_HEAD_VALUE = len(HEAD_VALUE)

RECORD_MAX = 30

class History(object):
    def __init__(self, router):
        self._db = CounterDB(name=HISTORY, router=router, domain=DOMAIN_USR)

    def _log(self, text):
        if LOG_HISTORY:
            log_debug(self, text)

    def _check_counter(self, conn, key):
        cnt = self._db.get_counter(conn, key, COL_CNT)
        if not cnt:
            self._db.set_counter(conn, key, COL_CNT, 1)
            return 0
        else:
            self._db.inc_counter(conn, key, COL_CNT)
            return (cnt % RECORD_MAX) + 1

    def put(self, uid, key, fields):
        if not key or type(key) != str or not fields or type(fields) != dict:
            log_err(self, 'failed to put')
            return

        coll = self._db.collection(uid)
        with self._db.connection(coll) as conn:
            pos = self._check_counter(conn, key)

        if 0 == pos:
            val = {COL_KEY:str(fields.keys())}
            pos = 1
        else:
            val = {}

        col_date = HEAD_DATE + str(pos)
        col_value = HEAD_VALUE + str(pos)
        date = str(datetime.utcnow())
        value = str(fields.values())
        val.update({col_date:date, col_value:value})

        with self._db.connection(coll) as conn:
            self._db.put(conn, key, val)
        self._log('put, key=%s' % key)

    def _extract_row(self, row):
        dates = {}
        values = {}
        empty = (None, None, None)

        if COL_KEY not in row:
            return empty

        keys = ast.literal_eval(row[COL_KEY])
        if not keys or type(keys) != list:
            return empty

        for i in row:
            if i.startswith(HEAD_DATE):
                dates.update({i[LEN_HEAD_DATE:]:row[i]})
            elif i.startswith(HEAD_VALUE):
                v = ast.literal_eval(row[i])
                if not v or type(v) != list:
                    return empty
                values.update({i[LEN_HEAD_VALUE:]:v})
        return (keys, values, dates)

    def get(self, uid, key):
        res = ''
        if not key:
            log_err(self, 'failed to get')
            return res

        coll = self._db.collection(uid)
        with self._db.connection(coll) as conn:
            row = self._db.get(conn, key)

        if row:
            keys, values, dates = self._extract_row(row)
            if keys and len(keys) == len(values) and len(values) == len(dates):
                length = len(keys)
                temp = {i:{} for i in range(length)}
                for i in dates:
                    if i not in values or len(values[i]) != length:
                        return ''
                    for j in range(length):
                        temp[j].update({dates[i]:values[i][j]})
                res = json.dumps({keys[i]:temp[i] for i in temp})
        self._log('get, key=%s' % key)
        return res
