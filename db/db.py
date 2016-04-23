#      db.py
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
#      MA 02110-1301, USA.

from cache import Cache
from lib.log import log_err, log_get
from interface.mongodb import MongoDB
from lib.domains import DOMAIN_DEV, DOMAIN_USR

class DB(MongoDB):
    def __init__(self, router=None, multi_value=False, cache_port=0, domain=None):
        MongoDB.__init__(self, router, domain)
        self._multi_value = multi_value
        if cache_port:
            self._cache = Cache(cache_port)
        else:
            self._cache = None
    
    def put(self, key, value):
        if self._cache:
            self._cache.remove(key)
        if not self._multi_value:
            val = {'$set':{'v':value}}
        else:
            val = {'$addToSet':{'v':value}}
        coll = self.get_collection(key)
        if coll:
            self.update(coll, key, val, upsert=True)
    
    def get(self, key, first=False):
        value = None
        cache = False
        if self._cache:
            cache = True
            if not self._multi_value or first:
                value = self._cache.get(key)
        
        if not value:
            coll = self.get_collection(key)
            if not coll:
                return
            
            res = self.find(coll, key)
            if not res:
                return
            
            value = res['v']
            if self._multi_value:
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
        
        coll = self.get_collection(key)
        if not coll:
            return
        
        if not value:
            coll.remove({'k':key})
        else:
            if not self._multi_value:
                log_err(self, 'failed to remove')
                raise Exception(log_get(self, 'failed to remove'))
            if not regex:
                self.update(coll, key, {'$pull':{'v':value}})
            else:
                self.update(coll, key, {'$pull':{'v':{'$regex':value}}})

class TokenDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi_value=True, domain=DOMAIN_USR)

class GuestDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi_value=True, domain=DOMAIN_USR)

class NodeDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi_value=True, domain=DOMAIN_USR)

class MemberDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, multi_value=True, domain=DOMAIN_USR)

class DeviceDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, domain=DOMAIN_DEV)

class KeyDB(DB):
    def __init__(self, router):
        DB.__init__(self, router, domain=DOMAIN_DEV)
