# data.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from temp import Temp
from entry import Entry

class Data(Entry):
    def __init__(self, vrtx, edge, attr, router=None, core=None, rdonly=True):
        Entry.__init__(self, router, core)
        self._temp = Temp(self, rdonly)
        self._vrtx = vrtx
        self._edge = edge
        self._attr = attr
    
    def can_load(self):
        return True
    
    def can_scan(self):
        return True
    
    def can_enable(self):
        return True
    
    def can_disable(self):
        return True
    
    def truncate(self, uid, name, length):
        path = self.get_path(uid, name)
        self._fs.truncate(uid, path, length)
        self._temp.truncate(uid, name, length)
    
    def getattr(self, uid, name):
        return self.lsattr(uid, name)
    
    def create(self, uid, name):
        self.check_path(uid, name)
        self._vrtx.check_path(uid, parent=name)
        self._edge.check_path(uid, parent=name)
        self._attr.check_path(uid, parent=name)
        return self._temp.create(uid, name)
    
    def open(self, uid, name, flags):
        return self._temp.open(uid, name, flags)
    
    def release(self, uid, name, fh):
        return self._temp.release(uid, name, fh)
    
    def _unlink(self, uid, name):
        if self._core:
            self._core.remove(name)
    
    def unlink(self, uid, name):
        self._vrtx.remove(uid, name)
        self._edge.unlink(uid, name)
        self._attr.unlink(uid, name)
        self._temp.remove(uid, name)
        self._unlink(uid, name)
        self.remove(uid, name)
    
    def readdir(self, uid, name):
        if not name:
            return self.lsdir(uid, name)
        else:
            return []
    
    def initialize(self, uid, name):
        f = self.create(uid, name)
        self.release(uid, name, f)
    
    def discard(self, uid, name):
        return self._temp.discard(uid, name)
    
    def commit(self, uid, name):
        return self._temp.commit(uid, name)
