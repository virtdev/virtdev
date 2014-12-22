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

from threading import Lock
from datetime import datetime
from happybase import Connection
from lib.log import log, log_get, log_err

HISTORY_TABLE = 'history'
HISTORY_TAG_DAY = 'day'
HISTORY_TAG_YEAR = 'year'
HISTORY_TAG_MONTH = 'month'
HISTORY_TAG_TODAY = 'today'

HISTORY_CF = 'cf1:'
HISTORY_CF_LEN = len(HISTORY_CF)
HISTORY_TAGS = (HISTORY_TAG_TODAY, HISTORY_TAG_YEAR, HISTORY_TAG_MONTH, HISTORY_TAG_DAY)

HISTORY_ITEM_MAX = 1024

class HistoryDB(object):
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def __init__(self, router):
        self._db = {}
        self._lock = Lock()
        self._router = router
    
    def _get_db(self, key):
        db = None
        self._lock.acquire()
        try:
            addr = self._router.get(str(self), key)
            if self._db.has_key(addr):
                db = self._db.get(addr)
            else:
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
            val.update({HISTORY_CF + str(i):str(fields[i])})
        rowkey = self._get_rowkey(key)
        db.put(rowkey, val)
    
    def _parse(self, query):
        try:
            fields = query.split('_')
            if len(fields) < 2 or fields[0] not in HISTORY_TAGS:
                log_err(self, 'invalid query')
                return
            tag = fields[0]
            if tag == HISTORY_TAG_TODAY:
                if len(fields) != 2:
                    log_err(self, 'invalid query')
                    return
                num = int(fields[1])
                if num <= 0:
                    log_err(self, 'invalid query')
                    return
                log(log_get(self, 'query, tag=%s, range=%d' % (tag, num)))
                return (tag, num)
            else:
                if len(fields) != 3:
                    log_err(self, 'invalid query')
                    return
                start = fields[1]
                end = fields[2]
                log(log_get(self, 'query, tag=%s, range=[%s, %s]' % (tag, start, end)))
                return (tag, (start, end))
        except:
            log_err(self, 'invalid query')
    
    def _find_today(self, key, num):
        res = []
        if num > HISTORY_ITEM_MAX:
            num = HISTORY_ITEM_MAX
        db = self._get_db(key)
        t = datetime.utcnow()
        row_prefix = key + '%4d%02d%02d' % (t.year, t.month, t.day)
        try:
            for item in db.scan(row_prefix=row_prefix, limit=num):
                val = {}
                for column in item[1]:
                    val.update({column[HISTORY_CF_LEN:]:item[1][column]})
                res.append(val)
        except:
            log_err(self, 'failed, _find_today, key=%s, limit=%d' % (key, num))
        return str(res)
    
    def _find(self, key, tag, start, end):
        pass
    
    def get(self, key, query):
        if not key or not query:
            return ''
        
        try:
            tag, arg = self._parse(query)
        except:
            return ''
        
        if tag == HISTORY_TAG_TODAY:
            ret = self._find_today(key, arg)
        else:
            ret = self._find(key, tag, arg[0], arg[1])
        if not ret:
            return ''
        else:
            return ret
    