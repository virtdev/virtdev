#      mongo.py
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

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from conf.virtdev import META_SERVER_PORT

DATABASE = 'test'

class Mongo(object):
    def __init__(self, name):
        self._name = name
    
    def connect(self, addr):
        db = pymongo.database.Database(MongoClient(addr, META_SERVER_PORT), DATABASE)
        return Collection(db, self._name)
    
    def get(self, conn, key):
        return conn.find_one(key)
    
    def put(self, conn, key, val, create=False):
        conn.update(key, val, upsert=create)
    
    def delete(self, conn, key):
        conn.remove(key)
    
    def connection(self, coll):
        return coll
