# pool.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.util import lock
from random import randint
from lib.log import log_debug
from conf.log import LOG_POOL
from datetime import datetime
from threading import Event, Lock

TIMEOUT = 0.001  # sec
LOAD_SUPER  = 95  # sec
LOAD_HEAVY  = 45  # sec
LOAD_MEDIUM = 15  # sec
LOAD_LIGHT  = 5   # sec

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
        self.__id = 0
        self.__pos = 0
        self.__count = 0
        self.__tasks = 0
        self.__queues = []
        self.__lock = Lock()
        self.__task_start = {}
        self.__active_tasks = 0
        self.__event = PoolEvent()
        self.__task_time = LOAD_SUPER

    def __log(self, text):
        if LOG_POOL:
            log_debug(self, text)

    @property
    def __super_load_tasks(self):
        tasks = self.__count / 8
        if 0 == tasks:
            return 1
        else:
            return tasks

    @property
    def __heavy_load_tasks(self):
        tasks = self.__count / 4
        if 0 == tasks:
            return 1
        else:
            return tasks

    @property
    def __medium_load_tasks(self):
        tasks = self.__count / 2
        if 0 == tasks:
            return 1
        else:
            return tasks

    @property
    def __light_load_tasks(self):
        return self.__count

    def __push(self, buf, identity=None):
        self.__lock.acquire()
        try:
            if 0 == self.__count:
                return

            if identity != None and identity > self.__pos:
                return

            if 0 == self.__tasks:
                self.__tasks = self.__super_load_tasks

            if self.__active_tasks >= self.__tasks:
                return

            pos = None
            length = None
            for i in range(self.__tasks):
                if self.__queues[i].length < self.__queues[i].max_len:
                    if length == None or length > self.__queues[i].length:
                        length = self.__queues[i].length
                        pos = i
                        if 0 == length:
                            break

            if pos != None:
                que = self.__queues[pos]
                if identity != None:
                    i = que.index
                    if not self.__task_start.has_key(i):
                        self.__task_start[i] = []
                    self.__task_start[i].append(datetime.now())
                    self.__active_tasks += 1
                    self.__pos += 1
                que.push(buf)
                return True
        finally:
            self.__lock.release()

    def wait(self, timeout=TIMEOUT):
        self.__event.wait(timeout)

    def wakeup(self):
        self.__event.set()

    def __update_task_time(self, index):
        if self.__task_start.has_key(index) and len(self.__task_start[index]) > 0:
            start = self.__task_start[index].pop(0)
            t = (datetime.now() - start).total_seconds()
            self.__task_time = t / 4 + self.__task_time * 3 / 4
            return True

    def finish(self, index):
        self.__lock.acquire()
        try:
            if self.__update_task_time(index):
                d_super = abs(self.__task_time - LOAD_SUPER)
                d_heavy = abs(self.__task_time - LOAD_HEAVY)
                d_medium = abs(self.__task_time - LOAD_MEDIUM)
                d_light = abs(self.__task_time - LOAD_LIGHT)
                if d_super < d_heavy:
                    self.__tasks = self.__super_load_tasks
                    self.__log('super load, tasktime=%s, active=%d, total=%d' % (str(self.__task_time), self.__active_tasks, self.__count))
                elif d_heavy < d_medium:
                    self.__tasks = self.__heavy_load_tasks
                    self.__log('heavy load, tasktime=%s, active=%d, total=%d' % (str(self.__task_time), self.__active_tasks, self.__count))
                elif d_medium < d_light:
                    self.__tasks = self.__medium_load_tasks
                    self.__log('medium load, tasktime=%s, active=%d, total=%d' % (str(self.__task_time), self.__active_tasks, self.__count))
                else:
                    self.__tasks = self.__light_load_tasks
                    self.__log('light load, tasktime=%s, active=%d, total=%d' % (str(self.__task_time), self.__active_tasks, self.__count))

            if self.__active_tasks > 0:
                self.__active_tasks -= 1
        finally:
            self.__lock.release()

    def get(self, index):
        if index < self.__count:
            return self.__queues[index]

    def add(self, queue):
        queue.set_index(self.__count)
        self.__queues.append(queue)
        self.__count += 1

    def __alloc(self):
        self.__lock.acquire()
        ret = self.__id
        self.__id += 1
        self.__lock.release()
        return ret

    def push(self, buf, async=False):
        if not async:
            identity = self.__alloc()
            while not self.__push(buf, identity):
                self.wait()
        else:
            return self.__push(buf)
