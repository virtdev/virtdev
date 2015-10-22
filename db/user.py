#      user.py
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

from lib.util import lock
from threading import Lock
from pymongo import MongoClient
from lib.log import log_err, log_get
from pymongo.database import Database
from pymongo.collection import Collection
from conf.virtdev import META_SERVER_PORT

DATABASE_NAME = 'test'
COLLECTION_MAX = 1024

class UserDB(object):
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def __init__(self, router):
        self._lock = Lock()
        self._router = router
        self._collections = {}
    
    def _close(self, coll):
        pass
    
    @lock
    def _check_collection(self, addr):
        coll = self._collections.get(addr)
        if not coll:
            if len(self._collections) >= COLLECTION_MAX:
                _, coll = self._collections.popitem()
                self._close(coll)
            db = Database(MongoClient(addr, META_SERVER_PORT), DATABASE_NAME)
            coll = Collection(db, str(self))
            self._collections.update({addr:coll})
        return coll
    
    def _get_collection(self, user):
        if not user:
            log_err(self, 'failed to get collection, no user')
            raise Exception(log_get(self, 'failed to get collection'))
        addr = self._router.get(user)
        if not addr:
            log_err(self, 'failed to get collection, no address')
            raise Exception(log_get(self, 'failed to get collection'))
        return self._check_collection(addr)
    
    def get(self, user, *fields):
        coll = self._get_collection(user)
        res = coll.find_one({'user':user})
        if not fields or not res:
            return res
        if 1 == len(fields):
            return res.get(fields[0])
        else:
            ret = []
            for i in fields:
                if not res.has_key(i):
                    return
                ret.append(res.get(i))
            return ret
    
    def put(self, user, **fields):
        coll = self._get_collection(user)
        coll.update({'user':user}, {'$set':fields}, upsert=True)
