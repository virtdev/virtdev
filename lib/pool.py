#      pool.py
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

from lib.util import lock
from random import randint
from threading import Event, Lock

TIMEOUT = 0.1 # seconds

class PoolEvent(object):
    def __init__(self):
        self._lock = Lock()
        self._event = Event()
    
    @lock
    def _clear(self):
        if self._event.is_set():
            self._event.clear()
    
    @lock
    def set(self):
        self._event.set()
    
    def wait(self, timeout):
        self._event.wait(timeout)
        self._clear()

class Pool(object):
    def __init__(self):
        self.__count = 0
        self.__queues = []
        self.__lock = Lock()
        self.__event = PoolEvent()
    
    def __find(self):
        self.__lock.acquire()
        try:
            if 0 == self.__count:
                return
            n = 0
            length = None
            i = randint(0, self.__count - 1)
            for _ in range(self.__count):
                l = self.__queues[i].length
                if l >= self.__queues[i].capacity:
                    continue
                if l == 0:
                    length = 0
                    n = i
                    break
                elif length == None or l < length:
                    length = l
                    n = i
                i += 1
                if i == self.__count:
                    i = 0
            if length != None:
                return self.__queues[n]
        finally:
            self.__lock.release()
    
    def wait(self, timeout=TIMEOUT):
        self.__event.wait(timeout)
    
    def wakeup(self):
        self.__event.set()
    
    def get(self, index):
        if index >= self.__count:
            return
        else:
            return self.__queues[index]
    
    def add(self, queue):
        queue.set_parent(self)
        queue.set_index(self.__count)
        self.__queues.append(queue)
        self.__count += 1
    
    def push(self, buf):
        while True:
            que = self.__find()
            if not que:
                self.wait()
            else:
                if que.push(buf):
                    return
