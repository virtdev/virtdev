#      edge.py
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

import os
from path import Path
from  errno import EINVAL
from lib.log import log_err
from fuse import FuseOSError

class Edge(Path):
    def can_touch(self):
        return True
    
    def can_unlink(self):
        return True
    
    def initialize(self, uid, edge, hidden=False):
        if type(edge) != tuple:
            log_err(self, 'failed to initialize')
            raise FuseOSError(EINVAL)
        
        if not hidden:
            name = os.path.join(edge[0], edge[1])
        else:
            name = os.path.join(edge[0], '.' + edge[1])
        self.create(uid, name)
    
    def _new_edge(self, src, dest):
        if self._core:
            if dest.startswith('.'):
                edge = (src, dest[1:])
            else:
                edge = (src, dest)
            self._core.add_edge(edge)
    
    def _create(self, uid, name):
        if self._core:
            parent = self.parent(name)
            child = self.child(name)
            if parent != child:
                self._new_edge(parent, child)
    
    def create(self, uid, name):
        self.symlink(uid, name)
        self._create(uid, name)
        return 0
    
    def open(self, uid, name, flags):
        return self.create(uid, name)
    
    def _unlink(self, uid, name):
        if not self._core:
            return
        parent = self.parent(name)
        child = self.child(name)
        if parent != child:
            edge = (parent, child)
            self._core.remove_edge(edge)
    
    def unlink(self, uid, name):
        self._unlink(uid, name)
        self.remove(uid, name)
    
    def readdir(self, uid, name):
        return self.lsdir(uid, name)
    
    def readlink(self, uid, name):
        return self.lslink(uid, name)
    
    def getattr(self, uid, name):
        return self.lsattr(uid, name, symlink=True)
    
    def release(self, uid, name, fh):
        pass
