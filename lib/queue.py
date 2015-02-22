#      queue.py
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

from util import lock
from log import log_err
from threading import Thread, Event, Lock

class VDevQueue(Thread):
    def __init__(self, queue_len):
        Thread.__init__(self)
        self._queue_len = queue_len
        self._event = Event()
        self._lock = Lock()
        self._queue = []
        self.start()
    
    def _insert(self, buf):
        pass
    
    @lock
    def insert(self, buf):
        self._insert(buf)
        self._queue.insert(0, buf)
        self._event.set()
    
    def _push(self, buf):
        pass
    
    @lock
    def push(self, buf):
        if len(self._queue) < self._queue_len:
            self._push(buf)
            self._queue.append(buf)
            self._event.set()
            return True
    
    def get_length(self):
        return len(self._queue)
    
    def get_capcity(self):
        return self._queue_len
    
    @lock
    def pop(self):
        buf = None
        if len(self._queue) > 0:
            buf = self._queue.pop(0)
            if len(self._queue) == 0:
                self._event.clear()
        return buf
    
    def _proc(self, buf):
        pass
    
    def run(self):
        while True:
            self._event.wait()
            buf = self.pop()
            if buf:
                try:
                    self._proc(buf)
                except:
                    log_err(self, 'failed to process')

    