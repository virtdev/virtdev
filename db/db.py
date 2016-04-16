#      db.py
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
#      MA 02110-1301, USA.

from cache import Cache
from lib.util import lock
from lib.domains import *
from threading import Lock
from conf.log import LOG_DB
from pymongo import MongoClient
from pymongo.database import Database
from conf.virtdev import META_SERVER_PORT
from pymongo.collection import Collection
from lib.log import log_debug, log_err, log_get

DATABASE_NAME = 'test'
COLLECTION_MAX = 1024

class DB(object):
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def _log(self, text):
        if LOG_DB:
            log_debug(self, text)
    
    def __init__(self, router=None, multi=False, increase=False, cache_port=0, domain=None):
        self._cache = None
        self._lock = Lock()
        self._multi = multi
        self._domain = domain
        self._router = router
        self._collections = {}
        self._increase = increase
    
        if (multi and increase):
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
        
        if cache_port:
            self._cache = Cache(cache_port)
        
        if not router:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
    
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
    
    def _get_collection(self, key):
        if not key:
            log_err(self, 'failed to get collection, no key')
            raise Exception(log_get(self, 'failed to get collection'))
        
        addr = self._router.get(key, self._domain)
        if addr:
            self._log('get_collection, addr=%s, key=%s' % (addr, key))
            return self._check_collection(addr)
    
    def put(self, key, value):
        if self._cache:
            self._cache.remove(key)
        if not self._multi and not self._increase:
            item = {'$set':{'v':value}}
        else:
            if self._multi:
                item = {'$addToSet':{'v':value}}
            else:
                item = {'$inc':{'v':value}}
        coll = self._get_collection(key)
        if coll:
            coll.update({'k':key}, item, upsert=True)
    
    def get(self, key, first=False):     
        value = None
        cache = False
        if self._cache:
            cache = True
            if not self._multi or first:
                value = self._cache.get(key)
            
        if not value:
            coll = self._get_collection(key)
            if not coll:
                return
            
            res = coll.find_one({'k':key})
            if not res:
                return
            
            value = res['v']
            if self._multi:
                if first and type(value) == list:
                    value = value[0]
                else:
                    cache = False
            
            if cache:
                self._cache.put(key, value)
        return value
    
    def remove(self, key, value=None, regex=False):
        if self._cache:
            self._cache.remove(key)
        
        coll = self._get_collection(key)
        if not coll:
            return
        
        if not value:
            coll.remove({'k':key})
        else:
            if not self._multi:
                log_err(self, 'failed to remove')
                raise Exception(log_get(self, 'failed to remove'))
            if not regex:
                coll.update({'k':key}, {'$pull':{'v':value}})
            else:
                coll.update({'k':key}, {'$pull':{'v':{'$regex':value}}})

class TokenDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi=True, domain=DOMAIN_USR)

class GuestDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi=True, domain=DOMAIN_USR)

class NodeDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi=True, domain=DOMAIN_USR)

class MemberDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi=True, domain=DOMAIN_USR)

class DeviceDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, domain=DOMAIN_DEV)

class KeyDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, domain=DOMAIN_DEV)
