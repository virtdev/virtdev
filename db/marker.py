#      marker.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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
#      MA 02110-1301, USA.

from threading import Lock
from datetime import datetime
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from lib.log import log, log_get, log_err
from lib.util import CLS_USER, CLS_DEVICE, lock
from conf.virtdev import META_SERVER_PORT, USER_SERVERS, DEVICE_SERVERS

PRINT = False
COLLECTION_MAX = 1024
DATABASE_NAME = 'test'
USER_MARK = 'usermark'
DEVICE_MARK = 'devicemark'

class Marker(object):
    def __init__(self):
        self._coll_usr = {}
        self._coll_dev = {}
        self._lock = Lock()
    
    def _close(self, coll):
        pass
    
    def _print(self, text):
        if PRINT:
            log(log_get(self, text))
    
    def _get_addr(self, name, cls):
        if cls == CLS_USER:
            servers = USER_SERVERS
        elif cls == CLS_DEVICE:
            servers = DEVICE_SERVERS
        else:
            log_err(self, 'invalid cls')
            raise Exception(log_get(self, 'invalid cls'))
        n = abs(hash(name)) % len(servers)
        return servers[n]
    
    @lock
    def _check_collection(self, addr, cls):
        if cls == CLS_USER:
            coll = self._coll_usr.get(addr)
            if not coll:
                if len(self._coll_usr) >= COLLECTION_MAX:
                    _, coll = self._coll_usr.popitem()
                    self._close(coll)
                db = Database(MongoClient(addr, META_SERVER_PORT), DATABASE_NAME)
                coll = Collection(db, USER_MARK)
                self._coll_usr.update({addr:coll})
        else:
            coll = self._coll_dev.get(addr)
            if not coll:
                if len(self._coll_dev) >= COLLECTION_MAX:
                    _, coll = self._coll_dev.popitem()
                    self._close(coll)
                db = Database(MongoClient(addr, META_SERVER_PORT), DATABASE_NAME)
                coll = Collection(db, DEVICE_MARK)
                self._coll_dev.update({addr:coll})
        return coll
    
    def _get_collection(self, name, cls):
        addr = self._get_addr(name, cls)
        return self._check_collection(addr, cls)
    
    def mark(self, name, cls, area):
        t = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()
        coll = self._get_collection(name, cls)
        if coll:
            coll.update({'k':name}, {'$set':{'v':(t, area)}}, upsert=True)
        else:
            log_err(self, 'failed to mark, name=%s, cls=%s' % (str(name), str(cls)))
        self._print('mark, name=%s, cls=%s' % (str(name), str(cls)))
