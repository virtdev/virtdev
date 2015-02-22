#      pool.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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

import time
from random import randint
from util import hash_name
from log import log_err, log_get

WAIT_TIME = 0.01

class VDevPool(object):
    def __init__(self):
        self._count = 0
        self._queues = []
        self._queue_len = 0
    
    def add(self, queue):
        if 0 == self._queue_len:
            self._queue_len = queue.get_capacity()
            if not self._queue_len:
                log_err(self, 'failed to initialize')
                raise Exception(log_get(self, 'failed to initialize'))
        else:
            if self._queue_len != queue.get_capacity():
                log_err(self, 'invalid queue')
                raise Exception(log_get(self, 'invalid queue'))
        self._queues.append(queue)
        self._count += 1
    
    def get(self):
        if 0 == self._count:
            return
        n = 0
        length = self._queue_len
        i = randint(0, self._count - 1)
        for _ in range(self._count):
            l = self._queues[i].get_length()
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
        if length < self._queue_len:
            return self._queues[n]
    
    def select(self, name):
        if 0 == self._count:
            return
        n = hash_name(name) % self._count
        return self._queues[n]
    
    def push(self, buf):
        while True:
            queue = self.get()
            if not queue:
                time.sleep(WAIT_TIME)
            else:
                if queue.push(buf):
                    return
    