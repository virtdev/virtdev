#      db.py
#      
#      Copyright (C) 2014 Yi-Wei Ci <ciyiwei@hotmail.com>
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
import datetime
from threading import Lock
from cache import VDevDBCache
from lib.log import log_err, log_get
from conf.virtdev import VDEV_DEFAULT_ADDR, VDEV_DB_PORT

VDEV_ADDR_TTL = 30 # minutes

def strkey(func):
    def _strkey(*args, **kwargs):
        self = args[0]
        key = args[1]
        if type(key) != str and type(key) != unicode:
            log_err(self, 'invalid key')
            raise Exception(log_get(self, 'invalid key'))
        return func(self, str(key), *args[2:], **kwargs)
    return _strkey

def dictkey(func):
    def _dictkey(*args, **kwargs):
        self = args[0]
        key = args[1]
        if type(key) != dict:
            log_err(self, 'invalid key')
            raise Exception(log_get(self, 'invalid key'))
        return func(*args, **kwargs)
    return _dictkey

def single(func):
    def _single(*args, **kwargs):
        self = args[0]
        if self._multi:
            log_err(self, 'invalid mode')
            raise Exception(log_get(self, 'invalid mode'))
        return func(*args, **kwargs)
    return _single

def rdwrmode(func):
    def _rdwrmode(*args, **kwargs):
        self = args[0]
        if self._readOnly:
            log_err(self, 'invalid mode')
            raise Exception(log_get(self, 'invalid mode'))
        return func(*args, **kwargs)
    return _rdwrmode

def ttlmode(func):
    def _ttlmode(*args, **kwargs):
        self = args[0]
        if not self._ttl:
            log_err(self, 'invalid mode')
            raise Exception(log_get(self, 'invalid mode'))
        return func(*args, **kwargs)
    return _ttlmode

class VDevDB(object):
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def __init__(self, router=None, multi=False, readOnly=False, increase=False, cache_port=0, ttl=0, addr=VDEV_DEFAULT_ADDR):
        self._ttl = ttl
        self._cache = None
        self._lock = Lock()
        self._multi = multi
        self._router = router
        self._readOnly = readOnly
        self._increase = increase
        if (multi and increase) or (readOnly and ttl) or (readOnly and increase):
            log_err(self, 'invalid parameters')
            raise Exception(log_get(self, 'invalid parameters'))
        if cache_port:
            if ttl:
                log_err(self, 'invalid parameters')
                raise Exception(log_get(self, 'invalid parameters'))
            self._cache = VDevDBCache(cache_port)
        if self._router:
            self._db = {}
        else:
            conn = pymongo.Connection(addr, VDEV_DB_PORT)
            self._db = eval('conn.test.%s' % str(self))
    
    def _get_db(self, key=''):
        if not self._router:
            return self._db
        elif not key:
            log_err(self, 'failed to get db, invalid key')
            raise Exception(log_get(self, 'failed to get db'))
        db = None
        self._lock.acquire()
        try:
            addr = self._router.get(str(self), key)
            if self._db.has_key(addr):
                db = self._db.get(addr)
            else:
                conn = pymongo.Connection(addr, VDEV_DB_PORT)
                db = eval('conn.test.%s' % str(self))
                self._db.update({addr:db})
        finally:
            self._lock.release()
        if not db:
            log_err(self, 'failed to get db')
            raise Exception(log_get(self, 'failed to get db'))
        return db
    
    @strkey
    @single
    @rdwrmode
    def set(self, key, value):
        db = self._get_db(key)
        db.update({'k':key}, {'$set':{'v':value}}, upsert=True)
    
    @strkey
    @rdwrmode
    def unset(self, key):
        db = self._get_db(key)
        db.update({'k':key}, {'$unset':{'v':''}})
    
    @strkey
    @rdwrmode
    def put(self, key, value):
        if self._cache:
            self._cache.remove(key)
        if not self._multi and not self._increase:
            if not self._ttl:
                item = {'$set':{'v':value}}
            else:
                item = {'$set':{'v':value, 't':datetime.datetime.utcnow()}}
        else:
            if self._multi:
                item = {'$addToSet':{'v':value}}
            else:
                item = {'$inc':{'v':value}}
            if self._ttl:
                item.update({'$set':{'t':datetime.datetime.utcnow()}})
        db = self._get_db(key)
        db.update({'k':key}, item, upsert=True)
    
    @strkey
    def get(self, key, first=False):     
        value = None
        cache = False
        if self._cache:
            cache = True
            value = self._cache.get(key)
            
        if not value:
            db = self._get_db(key)
            res = db.find_one({'k':key})
            if not res:
                return
            value = res['v']
            if self._multi:
                if first and type(value) == list:
                    value = value[0]
                else:
                    cache = False
            if cache:
                self._cache.put(key, value)
        return value
    
    @dictkey
    def find(self, key, *fields):
        db = self._get_db(key.values()[0])
        res = db.find_one(key)
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
    
    @strkey
    @rdwrmode
    def remove(self, key, value=None, regex=False):
        if self._cache:
            self._cache.remove(key)
        if not value:
            self._db.remove({'k':key})
        else:
            if not self._multi:
                log_err(self, 'failed to remove')
                raise Exception(log_get(self, 'failed to remove'))
            db = self._get_db(key)
            if not regex:
                db.update({'k':key}, {'$pull':{'v':value}})
            else:
                db.update({'k':key}, {'$pull':{'v':{'$regex':value}}})
    
    @strkey
    @ttlmode
    def refresh(self, key):
        db = self._get_db(key)
        time = datetime.datetime.utcnow()
        db.update({'k':key}, {'$set':{'t':time}})
    
    @ttlmode
    def recycle(self):
        db = self._get_db()
        now = datetime.datetime.utcnow()
        time = now + datetime.timedelta(minutes=-self._ttl)
        res = db.find_one({'t':{'$lt':time}})
        if res:
            key = res.get('k')
            if key:
                db.update({'k':key}, {'$unset':{'v':''}})
                return key
    
class TokenDB(VDevDB):
    def __init__(self, router):
        VDevDB.__init__(self, router, multi=True)

class DeviceDB(VDevDB):
    def __init__(self, router):
        VDevDB.__init__(self, router)

class UserDB(VDevDB):
    def __init__(self, router):
        VDevDB.__init__(self, router, readOnly=True)

class GuestDB(VDevDB):
    def __init__(self, router):
        VDevDB.__init__(self, router, multi=True)

class NodeDB(VDevDB):
    def __init__(self, router):
        VDevDB.__init__(self, router, multi=True)

class MemberDB(VDevDB):
    def __init__(self, router):
        VDevDB.__init__(self, router, multi=True)

class AddressDB(VDevDB):
    def __init__(self):
        VDevDB.__init__(self, multi=True, ttl=VDEV_ADDR_TTL)

class CounterDB(VDevDB):
    def __init__(self):
        VDevDB.__init__(self, increase=True)
