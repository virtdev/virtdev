#      watcher.py
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
