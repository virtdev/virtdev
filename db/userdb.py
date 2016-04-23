#      userdb.py
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
#      MA 02110-1301, USA

from interface.mongodb import MongoDB

class UserDB(MongoDB):
    def __init__(self, router):
        MongoDB.__init__(self, router, key='user')
    
    def get(self, user, *fields):
        coll = self.get_collection(user)
        res = self.find(coll, user)
        if not fields or not res:
            return res
        if 1 == len(fields):
            return res.get(fields[0])
        else:
            ret = []
            for i in fields:
                if not res.has_key(i):
                    return
                ret.append(res.get(i))
            return ret
    
    def put(self, user, **fields):
        coll = self.get_collection(user)
        self.update(coll, user, {'$set':fields}, upsert=True)
