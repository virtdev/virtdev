#      finder.py
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

import zerorpc
from member import Member
from master import get_finders
from pymongo import MongoClient
from threading import Thread, Lock
from lib.log import log_get, log_err
from pymongo.database import Database
from pymongo.collection import Collection
from marker import USER_MARK, DEVICE_MARK
from lib.util import USER_DOMAIN, DEVICE_DOMAIN, zmqaddr, ifaddr, lock
from conf.virtdev import META_SERVER_PORT, USER_SERVERS, DEVICE_SERVERS, USER_FINDER_PORT, DEVICE_FINDER_PORT

CACHE_MAX = 100000
DATABASE_NAME = 'test'
COLLECTION_MAX = 1024

class FinderCache(object):
    def __init__(self, domain):
        self._cache = {}
        self._lock = Lock()
        self._domain = domain
        self._collections = {}
        if self._domain == USER_DOMAIN:
            self._servers = USER_SERVERS
        else:
            self._servers = DEVICE_SERVERS
        if not self._servers:
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
            if self._domain == USER_DOMAIN:
                coll = Collection(db, USER_MARK)
            else:
                coll = Collection(db, DEVICE_MARK)
            self._collections.update({addr:coll})
        return coll
    
    def _get_addr(self, key):
        n = abs(hash(key)) % len(self._servers)
        return self._servers[n]
    
    def _get_collection(self, key):
        addr = self._get_addr(key)
        return self._check_collection(addr)
    
    def _get_val(self, key):
        coll = self._get_collection(key)
        if coll:
            res = coll.find_one({'k':key})
            if res:
                return res.get('v')
    
    def get(self, key):
        ret = self._cache.get(key)
        if ret:
            return ret
        val = self._get_val(key)
        if val:
            if len(self._cache) >= CACHE_MAX:
                self._cache.popitem()
            self._cache.update({key:val})
            return val

class Finder(Member):
    def __init__(self, domain):
        Member.__init__(self)
        finders = get_finders(domain)
        if not finders:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
        self.set_members(finders)
        self._cache = FinderCache(domain)
    
    def get(self, key):
        return self._cache.get(key)

class FinderServer(Thread):
    def __init__(self, domain):
        Thread.__init__(self)
        self._domain = domain
        self._finder = Finder(domain)
    
    def run(self):
        srv = zerorpc.Server(self._finder)
        if self._domain == USER_DOMAIN:
            srv.bind(zmqaddr(ifaddr(), USER_FINDER_PORT))
        elif self._domain == DEVICE_DOMAIN:
            srv.bind(zmqaddr(ifaddr(), DEVICE_FINDER_PORT))
        else:
            log_err(self, 'invalid domain')
            raise Exception(log_get(self, 'invalid domain'))
        srv.run()

class UserFinder(FinderServer):
    def __init__(self):
        FinderServer.__init__(self, USER_DOMAIN)

class DeviceFinder(FinderServer):
    def __init__(self):
        FinderServer.__init__(self, DEVICE_DOMAIN)
