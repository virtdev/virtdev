# counterdb.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from remotedb import RemoteDB
from conf.prot import PROT_COUNTERDB

if PROT_COUNTERDB == 'hbase':
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
