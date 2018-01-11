# queue.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.tasklet import Tasklet
from conf.defaults import DEBUG
from lib.log import log_debug, log_err
from multiprocessing import TimeoutError
from threading import Thread, Event, Lock
from multiprocessing.pool import ThreadPool

SAFE = True
TASKLET = False
TIMEOUT = 3600 # sec
WAIT_TIME = 1  # sec

class Queue(object):
    def __init__(self, parent, max_len, timeout=TIMEOUT):
        self.__parent = parent
        self.__max_len = max_len
        self.__timeout = timeout
        self.__event = Event()
        self.__lock = Lock()
        self.__index = None
        self.__queue = []
        self.__thread = Thread(target=self.__run)
        self.__thread.start()

    def _log(self, text):
        if self.__parent:
            log_debug(self.__parent, "Queue=>%s" % str(text))
        else:
            log_debug(self, text)

    def set_index(self, index):
        self.__index = index

    def proc(self, buf):
        pass

    def insert(self, buf):
        self.__lock.acquire()
        try:
            self.__queue.insert(0, buf)
            self.__event.set()
            return True
        finally:
            self.__lock.release()

    def push(self, buf):
        self.__lock.acquire()
        try:
            if len(self.__queue) < self.__max_len:
                self.__queue.append(buf)
                self.__event.set()
                return True
        finally:
            self.__lock.release()

    @property
    def index(self):
        return self.__index

    @property
    def length(self):
        return len(self.__queue)

    @property
    def max_len(self):
        return self.__max_len

    def __pop(self):
        self.__lock.acquire()
        try:
            if len(self.__queue) > 0:
                buf = self.__queue.pop(0)
                if len(self.__queue) == 0:
                    self.__event.clear()
                if buf:
                    return buf
        finally:
            self.__lock.release()

    def __proc_tasklet(self, buf):
        t = Tasklet(target=self.proc, args=(buf,), parent=self.__parent)
        t.wait(self.__timeout)

    def __proc_unsafe(self, buf):
        pool = ThreadPool(processes=1)
        result = pool.apply_async(self.proc, args=(buf,))
        result.get(timeout=self.__timeout)
        pool.terminate()

    def __do_proc(self, buf):
        pool = ThreadPool(processes=1)
        result = pool.apply_async(self.proc, args=(buf,))
        try:
            result.get(timeout=self.__timeout)
        except TimeoutError:
            self._log('timeout (%ss)' % str(self.__timeout))
        finally:
            pool.terminate()

    def __proc_safe(self, buf):
        try:
            self.__do_proc(buf)
        except:
            self._log('failed to process')

    def __wait(self):
        self.__event.wait(WAIT_TIME)

    def __run(self):
        while True:
            buf = self.__pop()
            if buf:
                self.__parent.wakeup()
                try:
                    if TASKLET:
                        self.__proc_tasklet(buf)
                    else:
                        if DEBUG and not SAFE:
                            self.__proc_unsafe(buf)
                        else:
                            self.__proc_safe(buf)
                finally:
                    self.__parent.finish(self.__index)
            else:
                self.__wait()
