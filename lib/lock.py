#      lock.py
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

from lib.util import lock
from threading import Lock
from log import log_get, log_err

POS_CNT = 0
POS_LOCK = 1
LOCK_SIZE = 2
LOCK_MAX = 65536

class NamedLock(object):
    def __init__(self):
        self._count = 0
        self._locks = {}
        self._lock = Lock()
    
    def _alloc(self):
        l = [None] * LOCK_SIZE
        l[POS_CNT] = 1
        l[POS_LOCK] = Lock()
        return l
    
    @lock
    def _get(self, name):
        l = self._locks.get(name)
        if l:
            l[POS_CNT] += 1
        else:
            if self._count >= LOCK_MAX:
                log_err(self, 'too much locks')
                raise Exception(log_get(self, 'too much locks'))
            l = self._alloc()
            self._locks.update({name:l})
            self._count += 1
        return l[POS_LOCK]
    
    @lock
    def _put(self, name):
        l = self._locks.get(name)
        if not l:
            log_err(self, 'cannot find lock')
            raise Exception(log_get(self, 'cannot find lock'))
        if l[POS_CNT] > 1:
            l[POS_CNT] -= 1
        else:
            del self._locks[name]
            self._count -= 1
        return l[POS_LOCK]
    
    def acquire(self, name):
        l = self._get(name)
        l.acquire()
    
    def release(self, name):
        l = self._put(name)
        l.release()
