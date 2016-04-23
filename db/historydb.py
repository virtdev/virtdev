#      historydb.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from lib.log import log_err
from datetime import datetime
from interface.hbase import HBase
from lib.domains import DOMAIN_USR
from conf.virtdev import RECORD_MAX

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

class HistoryDB(HBase):
    def __init__(self, router):
        HBase.__init__(self, router, DOMAIN_USR)
    
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
        
        coll = self.get_collection(uid)
        if not coll:
            log_err(self, 'failed to put')
            return
        
        with self.open(coll) as conn:
            table = self.get_table(conn, HISTORY)
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
        
        with self.open(coll) as conn:
            table = self.get_table(conn, HISTORY)
            self.update(table, key, val)
        
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
            log_err(self, 'failed to get, invalid key')
            return res
        
        coll = self.get_collection(uid)
        if not coll:
            log_err(self, 'failed to get, invalid collection')
            return res
        
        with self.open(coll) as conn:
            table = self.get_table(conn, HISTORY)
            row = self.find(table, key)
        
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
