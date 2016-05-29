#      user.py
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

from interface.commondb import CommonDB

class User(object):
    def __init__(self, router):
        self._db = CommonDB(name='user', router=router, key='user')
    
    def get(self, user, *fields):
        coll = self._db.collection(user)
        conn = self._db.connection(coll)
        res = self._db.get(conn, user, all_fields=True)
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
        coll = self._db.collection(user)
        conn = self._db.connection(coll)
        self._db.put(conn, user, {'$set':fields}, create=True)
