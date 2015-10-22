#      history.py
#      
#      Copyright (C) 2014 Yi-Wei Ci <ciyiwei@hotmail.com>
#      
#      This program is free software; you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation; either version 2 of the License, or
#      (at your option) any later version.
#      
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#      
#      You should have received a copy of the GNU General Public License
#      along with this program; if not, write to the Free Software
#      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#      MA 02110-1301, USA

import ast
import json
from threading import Lock
from datetime import datetime
from conf.virtdev import RECORD_MAX
from happybase import ConnectionPool
from lib.util import USER_DOMAIN, lock
from lib.log import log, log_get, log_err

CF_CNT = 'cf0'
CF_KEY = 'cf1'
CF_DATE = 'cf2'
CF_VALUE = 'cf3'

COL_KEY = CF_KEY + ':k'
COL_CNT = CF_CNT + ':c'

TABLE_HISTORY = 'history'
HEAD_DATE = CF_DATE + ':'
HEAD_VALUE = CF_VALUE + ':'
LEN_HEAD_DATE = len(HEAD_DATE)
LEN_HEAD_VALUE = len(HEAD_VALUE)

POOL_MAX = 1024
POOL_SIZE = 3
PRINT = False

class HistoryDB(object):
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def __init__(self, router):
        self._pools = {}
        self._lock = Lock()
        self._router = router
    
    def _print(self, text):
        if PRINT:
            log(log_get(self, text))
    
    def _close(self, pool):
        pass
    
    def _find(self, table, key):
        return table.row(key)
    
    def _update(self, table, key, val):
        table.put(key, val)
    
    def _get_table(self, conn, name):
        return conn.table(name)
    
    @lock
    def _check_pool(self, addr):
        pool = self._pools.get(addr)
        if not pool:
            if len(self._pools) >= POOL_MAX:
                _, pool = self._pools.popitem()
                self._close(pool)
            pool = ConnectionPool(size=POOL_SIZE, host=addr)
            self._pools.update({addr:pool})
        return pool
    
    def _get_pool(self, uid):
        addr = self._router.get(uid, USER_DOMAIN)
        if not addr:
            log_err(self, 'failed to get pool, no address')
            return
        return self._check_pool(addr)
    
    def _check_counter(self, table, key):
        cnt = table.counter_get(key, COL_CNT)
        if not cnt:
            table.counter_set(key, COL_CNT, 1)
            return 0
        else:
            table.counter_inc(key, COL_CNT)
            return (cnt % RECORD_MAX) + 1
            
    def put(self, uid, key, fields):
        if not key or type(key) != str or not fields or type(fields) != dict:
            log_err(self, 'failed to put, invalid arguments')
            return
        
        pool = self._get_pool(uid)
        if not pool:
            log_err(self, 'failed to put, no pool')
            return
        
        with pool.connection() as conn:
            table = self._get_table(conn, TABLE_HISTORY)
            pos = self._check_counter(table, key)
        
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
        
        with pool.connection() as conn:
            table = self._get_table(conn, TABLE_HISTORY)
            self._update(table, key, val)
        
        self._print('put, key=%s' % key)
    
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
            log_err(self, 'failed to get, no key')
            return res
        
        pool = self._get_pool(uid)
        if not pool:
            log_err(self, 'failed to get, no pool')
            return res
        
        with pool.connection() as conn:
            table = self._get_table(conn, TABLE_HISTORY)
            row = self._find(table, key)
        
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
        
        self._print('get, key=%s' % key)
        return res
