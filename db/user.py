# user.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

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
