#      watcher.py
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

import select
import inotify
from errno import EINTR
from lib.util import hash_name
from threading import Thread, Event
from inotify.watcher import AutoWatcher

VDEV_WATCHER_MAX = 4 # VDEV_WATCHER_MAX < 65536

class VDevWatcher(Thread):
    def __init__(self):
        self._results = {}
        Thread.__init__(self)
        self._event = Event()
        self._watcher = AutoWatcher()
    
    def add(self, path):
        n = self._watcher.num_watches()
        self._watcher.add(path, inotify.IN_MODIFY)
        if n == 0:
            self._event.set()
    
    def push(self, path):
        self._results[path] = True
    
    def pop(self, path):
        ret = self._results.get(path)
        if ret:
            self._results.pop(path)
        return ret
    
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

class VDevWatcherPool(object):
    def __init__(self):
        self._watchers = []
        for _ in range(VDEV_WATCHER_MAX):
            w = VDevWatcher()
            self._watchers.append(w)
            w.start()
    
    def _hash(self, path):
        return hash_name(path) % VDEV_WATCHER_MAX
    
    def add(self, path):
        n = self._hash(path)
        self._watchers[n].add(path)
    
    def push(self, path):
        n = self._hash(path)
        self._watchers[n].push(path)
    
    def pop(self, path):
        n = self._hash(path)
        return self._watchers[n].pop(path)
    