#      mongodb.py
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
from database import Database
from pymongo import MongoClient
from pymongo.collection import Collection
from conf.virtdev import META_SERVER_PORT

KEY_NAME = 'k'
DATABASE_NAME = 'test'

class MongoDB(Database):
    def __init__(self, router, domain=None, key=KEY_NAME):
        Database.__init__(self, router, domain)
        self._key = key
    
    def connect(self, addr):
        db = pymongo.database.Database(MongoClient(addr, META_SERVER_PORT), DATABASE_NAME)
        return Collection(db, str(self))
    
    def find(self, coll, key):
        return coll.find_one({self._key:key})
    
    def update(self, coll, key, val, upsert=False):
        coll.update({self._key:key}, val, upsert=upsert)
