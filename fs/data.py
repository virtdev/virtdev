#      data.py
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

from temp import Temp
from path import VDevPath

class Data(VDevPath):
    def __init__(self, vertex, edge, attr, watcher=None, router=None, manager=None):
        VDevPath.__init__(self, router, manager)
        self._temp = Temp(self, watcher)
        self._vertex = vertex
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
        self.file.truncate(uid, path, length)
        self._temp.truncate(uid, name, length)
    
    def getattr(self, uid, name):
        return self.lsattr(uid, name)
    
    def create(self, uid, name):
        self.check_path(uid, name)
        self._vertex.check_path(uid, parent=name)
        self._edge.check_path(uid, parent=name)
        self._attr.check_path(uid, parent=name)
        return self._temp.create(uid, name)
    
    def open(self, uid, name, flags):
        return self._temp.open(uid, name, flags)
    
    def release(self, uid, name, fh):
        return self._temp.close(uid, name, fh)
    
    def _unlink(self, uid, name):
        if self.manager:
            self.manager.synchronizer.remove(name)
    
    def unlink(self, uid, name):
        self._vertex.remove(uid, name)
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
    