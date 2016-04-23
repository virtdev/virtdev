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

from random import randint
from util import hash_name
from log import log_err, log_get
from threading import Event, Lock

class Pool(object):
    def __init__(self):
        self._count = 0
        self._queues = []
        self._capacity = 0
        self._lock = Lock()
        self._event = Event()
    
    def _find_queue(self):
        if 0 == self._count:
            return
        n = 0
        length = self._capacity
        i = randint(0, self._count - 1)
        for _ in range(self._count):
            l = self._queues[i].length
            if l == 0:
                length = 0
                n = i
                break
            elif l < length:
                length = l
                n = i
            i += 1
            if i == self._count:
                i = 0
        if length < self._capacity:
            return self._queues[n]
        
    def _get(self):
        self._lock.acquire()
        try:
            queue = self._find_queue()
            if not queue:
                if self._event.is_set():
                    self._event.clear()
            return queue
        finally:
            self._lock.release()
    
    def _wait(self):
        self._event.wait()
    
    def add(self, queue):
        if 0 == self._capacity:
            self._capacity = queue.capacity
            if not self._capacity:
                log_err(self, 'failed to initialize')
                raise Exception(log_get(self, 'failed to initialize'))
        else:
            if self._capacity != queue.capacity:
                log_err(self, 'invalid queue')
                raise Exception(log_get(self, 'invalid queue'))
        self._queues.append(queue)
        self._count += 1
        queue.set_parent(self)
    
    def select(self, name):
        if 0 == self._count:
            return
        n = hash_name(name) % self._count
        return self._queues[n]
    
    def wakeup(self):
        self._lock.acquire()
        self._event.set()
        self._lock.release()
    
    def push(self, buf):
        while True:
            queue = self._get()
            if not queue:
                self._wait()
            else:
                if queue.push(buf):
                    return
