# lock.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.util import lock
from threading import Lock
from lib.log import log_get, log_err

POS_CNT = 0
POS_LOCK = 1
LOCK_MAX = 65536
LOCK_SIZE = 2

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
        if 0 == l[POS_CNT]:
            del self._locks[name]
            self._count -= 1
        return l[POS_LOCK]

    def acquire(self, name):
        l = self._get(name)
        l.acquire()

    def release(self, name):
        l = self._put(name)
        l.release()
