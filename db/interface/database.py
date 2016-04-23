#      database.py
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

from lib.util import lock
from threading import Lock
from conf.log import LOG_DB
from lib.log import log_debug, log_err, log_get

COLLECTION_MAX = 1024

class Database(object):
    def __init__(self, router, domain=None):
        self._lock = Lock()
        self._router = router
        self._domain = domain
        self._collections = {}
        if not router:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
    
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def _log(self, text):
        if LOG_DB:
            log_debug(self, text)
    
    @lock
    def _check_collection(self, addr):
        coll = self._collections.get(addr)
        if not coll:
            if len(self._collections) >= COLLECTION_MAX:
                _, coll = self._collections.popitem()
                self.put_collection(coll)
            coll = self.connect(addr)
            self._collections.update({addr:coll})
        return coll
    
    def get_collection(self, name):
        if not name:
            log_err(self, 'failed to get collection')
            raise Exception(log_get(self, 'failed to get collection'))
        
        addr = self._router.get(name, self._domain)
        if addr:
            self._log('collection, addr=%s, name=%s' % (addr, name))
            return self._check_collection(addr)
    
    def put_collection(self, coll):
        pass
    
    def connect(self, addr):
        raise Exception(log_get(self, "connect is not implemented"))
