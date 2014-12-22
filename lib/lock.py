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

from threading import Lock

VDEV_LOCK_MAX = 256

class VDevLock(object):
    def __init__(self):
        self._locks = []
        for _ in range(VDEV_LOCK_MAX):
            self._locks.append(Lock())
    
    def _get_lock(self, name):
        n = 0
        length = len(name)
        for i in range(length):
            n ^= ord(name[i])
        return self._locks[n]
    
    def acquire(self, name):
        lock = self._get_lock(name)
        lock.acquire()
        return lock
    