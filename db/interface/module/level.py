#      level.py
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
