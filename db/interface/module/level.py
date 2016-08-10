# level.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import leveldb

class LevelDB(object):
    def __init__(self, name):
        self._db = leveldb.LevelDB(name)
        
    def bench(self):
        return self._db.WriteBench()
    
    def commit(self, bench):
        self._db.Write(bench, sync=True)
    
    def keys(self):
        return list(self._db.RangeIter())
    
    def get(self, key):
        return self._db.Get(key)
    
    def put(self, key, val):
        return self._db.Put(key, val)
