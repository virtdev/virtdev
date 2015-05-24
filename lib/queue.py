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

from log import log_err
from threading import Thread, Event, Lock
from multiprocessing.pool import ThreadPool

TIMEOUT = 60 # seconds

class Queue(object):
    def __init__(self, capacity, timeout=TIMEOUT):
        self.__thread = Thread(target=self.__run)
        self.__capacity = capacity
        self.__timeout = timeout
        self.__event = Event()
        self.__lock = Lock()
        self.__queue = []
        self.__thread.start()
    
    def proc(self, buf):
        pass
    
    def prepush(self, buf):
        pass
    
    def preinsert(self, buf):
        pass
    
    def insert(self, buf):
        self.__lock.acquire()
        try:
            self.preinsert(buf)
            self.__queue.insert(0, buf)
            self.__event.set()
        finally:
            self.__lock.release()
    
    def push(self, buf):
        self.__lock.acquire()
        try:
            if len(self.__queue) < self.__capacity:
                self.prepush(buf)
                self.__queue.append(buf)
                self.__event.set()
                return True
        finally:
            self.__lock.release()
    
    @property
    def length(self):
        return len(self.__queue)
    
    @property
    def capacity(self):
        return self.__capacity
    
    def __pop(self):
        self.__lock.acquire()
        try:
            buf = None
            if len(self.__queue) > 0:
                buf = self.__queue.pop(0)
                if len(self.__queue) == 0:
                    self.__event.clear()
            return buf
        finally:
            self.__lock.release()
    
    def __proc(self, buf):
        pool = ThreadPool(processes=1)
        result = pool.apply_async(self.proc, args=(buf,))
        try:
            result.get(timeout=self.__timeout)
        finally:
            pool.terminate()
    
    def __run(self):
        while True:
            self.__event.wait()
            buf = self.__pop()
            if buf:
                try:
                    self.__proc(buf)
                except:
                    log_err(self, 'failed to process')
