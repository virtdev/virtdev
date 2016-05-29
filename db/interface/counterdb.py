#      counterdb.py
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
from conf.types import TYPE_COUNTERDB

if TYPE_COUNTERDB == 'hbase':
    from module.hbase import HBase as DB

class CounterDB(RemoteDB):
    def __init__(self, name, router, domain):
        RemoteDB.__init__(self, router=router, domain=domain)
        self._db = DB(name)
    
    def connect(self, addr):
        return self._db.connect(addr)
    
    def connection(self, coll):
        return self._db.connection(coll)
    
    def get(self, conn, key):
        return self._db.get(conn, key)
    
    def put(self, conn, key, val, create=False):
        self._db.put(conn, key, val)
    
    def get_counter(self, conn, key, name):
        return self._db.get_counter(conn, key, name)
    
    def set_counter(self, conn, key, name, num):
        self._db.set_counter(conn, key, name, num)
    
    def inc_counter(self, conn, key, name):
        self._db.inc_counter(conn, key, name)
