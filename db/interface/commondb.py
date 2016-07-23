#      commondb.py
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

from remotedb import RemoteDB
from conf.prot import PROT_COMMONDB

if PROT_COMMONDB == 'mongo':
    from module.mongo import Mongo as DB

KEY_NAME = 'k'
VAL_NAME = 'v'

class CommonDB(RemoteDB):
    def __init__(self, name, router, domain=None, key=KEY_NAME):
        RemoteDB.__init__(self, router=router, domain=domain)
        self._db = DB(name)
        self._key = key
    
    def connect(self, addr):
        return self._db.connect(addr)
    
    def get(self, conn, key, all_fields=False):
        res = self._db.get(conn, {self._key:key})
        if res and not all_fields:
            res = res.get(VAL_NAME)
        return res
    
    def put(self, conn, key, val, create=False):
        self._db.put(conn, {self._key:key}, val, create)
    
    def delete(self, conn, key):
        self._db.delete(conn, {self._key:key})
    
    def connection(self, coll):
        return self._db.connection(coll)
