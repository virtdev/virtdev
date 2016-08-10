# commondb.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

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
