#      finder.py
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

import zerorpc
from lib.domains import *
from router import Router
from member import Member
from info import get_finders
from threading import Thread
from lib.util import zmqaddr, ifaddr
from lib.log import log_get, log_err
from marker import USR_MARK, DEV_MARK
from interface.commondb import CommonDB
from conf.route import USR_SERVERS, DEV_SERVERS, USR_FINDER_PORT, DEV_FINDER_PORT

CACHE_MAX = 100000

class FinderCache(object):
    def __init__(self, domain):
        self._cache = {}
        if domain == DOMAIN_USR:
            self._db = CommonDB(name=USR_MARK, router=Router(servers=USR_SERVERS))
        elif domain == DOMAIN_DEV:
            self._db = CommonDB(name=DEV_MARK, router=Router(servers=DEV_SERVERS))
    
    def _get(self, key):
        coll = self._db.collection(key)
        conn = self._db.connection(coll)
        return self._db.get(conn, key)
    
    def get(self, key):
        ret = self._cache.get(key)
        if ret:
            return ret
        val = self._get(key)
        if val:
            if len(self._cache) >= CACHE_MAX:
                self._cache.popitem()
            self._cache.update({key:val})
            return val

class FinderServer(Member):
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

class Finder(Thread):
    def __init__(self, domain):
        if domain != DOMAIN_USR and domain != DOMAIN_DEV:
            log_err(self, 'invalid domain')
            raise Exception(log_get(self, 'invalid domain'))
        self._finder = FinderServer(domain)
        self._domain = domain
        Thread.__init__(self)
    
    def run(self):
        srv = zerorpc.Server(self._finder)
        if self._domain == DOMAIN_USR:
            srv.bind(zmqaddr(ifaddr(), USR_FINDER_PORT))
        elif self._domain == DOMAIN_DEV:
            srv.bind(zmqaddr(ifaddr(), DEV_FINDER_PORT))
        srv.run()
