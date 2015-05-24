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
from threading import Lock
from datetime import datetime
from happybase import Connection
from conf.virtdev import STATISTIC
from lib.log import log_get, log_err

HISTORY_TAG_DAY = 'day'
HISTORY_TAG_YEAR = 'year'
HISTORY_TAG_MONTH = 'month'
HISTORY_TAG_TODAY = 'today'
HISTORY_TABLE = 'history'

HISTORY_CF = 'cf1'
HISTORY_CF_HEAD = HISTORY_CF + ':'
HISTORY_CF_HEAD_SIZE = len(HISTORY_CF_HEAD)
HISTORY_TAGS = (HISTORY_TAG_TODAY, HISTORY_TAG_YEAR, HISTORY_TAG_MONTH, HISTORY_TAG_DAY)

HISTORY_ITEM_MAX = 1024

if STATISTIC:
    import unicorndb

class Today(object):
    def scan(self, db, key, num):
        res = []
        t = datetime.utcnow()
        row_prefix = key + '%4d%02d%02d' % (t.year, t.month, t.day)
        try:
            for item in db.scan(row_prefix=row_prefix, limit=num):
                val = {}
                for column in item[1]:
                    val.update({column[HISTORY_CF_HEAD_SIZE:]:item[1][column]})
                res.append(val)
        except:
            log_err(self, 'failed, key=%s, num=%d' % (key, num))
        return str(res)
    
    def average(self, db, key, num):
        res = []
        try:
            if STATISTIC:
                res = unicorndb.stat(db, key, HISTORY_CF, num, "hour", "avg")
        except:
            log_err(self, 'failed, key=%s, num=%d' % (key, num))
        return str(res)

class HistoryDB(object):
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def __init__(self, router):
        self._db = {}
        self._lock = Lock()
        self._router = router
        self._today = Today()
    
    def _get_db(self, key):
        addr = self._router.get(str(self), key)
        db = self._db.get(addr)
        if not db:
            self._lock.acquire()
            try:
                db = self._db.get(addr)
                if not db:
                    conn = Connection(host=addr)
                    db = conn.table(HISTORY_TABLE)
                    self._db.update({addr:db})
            finally:
                self._lock.release()
        if not db:
            log_err(self, 'failed to get db')
            raise Exception(log_get(self, 'failed to get db'))
        return db
    
    def _get_rowkey(self, key):
        t = datetime.utcnow()
        suffix = '%4d%02d%02d%02d%02d%02d%06d' % (t.year, t.month, t.day, t.hour, t.minute, t.second, t.microsecond)
        return key + suffix
    
    def put(self, key, fields):
        if not key or not fields:
            return
        val = {}
        db = self._get_db(key)
        for i in fields:
            val.update({HISTORY_CF_HEAD + str(i):str(fields[i])})
        rowkey = self._get_rowkey(key)
        db.put(rowkey, val)
    
    def _parse(self, args):
        try:
            res = ast.literal_eval(args)
            if type(res) == dict and 1 == len(res):
                return (res.keys()[0], res.values()[0])
        except:
            log_err(self, 'invalid query')
    
    def _scan_today(self, key, limit):
        if limit > HISTORY_ITEM_MAX:
            limit = HISTORY_ITEM_MAX
        db = self._get_db(key)
        if STATISTIC:
            return self._today.average(db, key, limit)
        else:
            return self._today.scan(db, key, limit)
    
    def get(self, key, args):
        if not key or not args:
            return ''
        
        try:
            tag, args = self._parse(args)
        except:
            return ''
        
        if tag == HISTORY_TAG_TODAY:
            ret = self._scan_today(key, **args)
        
        if not ret:
            return ''
        
        return ret
