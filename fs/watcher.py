# watcher.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import select
import inotify
from errno import EINTR
from threading import Thread, Event
from inotify.watcher import AutoWatcher

class Watcher(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._event = Event()
        self._watcher = AutoWatcher()
        self._results = {}
    
    def register(self, path):
        n = self._watcher.num_watches()
        self._watcher.add(path, inotify.IN_MODIFY)
        if n == 0:
            self._event.set()
    
    def push(self, path):
        self._results[path] = True
    
    def pop(self, path):
        try:
            return self._results.pop(path)
        except:
            pass
    
    def run(self):
        while True:
            try:
                if 0 == self._watcher.num_watches():
                    self._event.wait()
                    self._event.clear()
                for e in self._watcher.read():
                    path = e.fullpath
                    if self._watcher.path(path):
                        self._results[path] = True
                        self._watcher.remove_path(path)
            except(OSError, select.error) as EINTR:
                continue
