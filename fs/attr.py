#      attr.py
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
import librsync
from temp import Temp
from path import VDevPath
from StringIO import StringIO
from lib.log import log_err, log_get
from base64 import encodestring, decodestring

ATTR_MODE = 'mode'
ATTR_FREQ = 'freq'
ATTR_FILTER = 'filter'
ATTR_HANDLER = 'handler'
ATTR_PROFILE = 'profile'
ATTR_DISPATCHER = 'dispatcher'
ATTRIBUTES = [ATTR_MODE, ATTR_FREQ, ATTR_FILTER, ATTR_HANDLER, ATTR_PROFILE, ATTR_DISPATCHER]

class Attr(VDevPath):
    def __init__(self, watcher=None, router=None, manager=None):
        VDevPath.__init__(self, router, manager)
        self._temp = Temp(self, watcher)
    
    def can_invalidate(self):
        return True
    
    def truncate(self, uid, name, length):
        if not self.manager:
            path = self.get_path(uid, name)
            self.file.truncate(uid, path, length)
            self._temp.truncate(uid, name, length)
    
    def may_update(self, flags):
        return flags & os.O_APPEND or flags & os.O_RDWR or flags & os.O_TRUNC or flags & os.O_WRONLY
    
    def is_expired(self, uid, name):
        path = self.path2temp(self.get_path(uid, name))
        return self.file.exists(uid, path)
    
    def getattr(self, uid, name):
        return self.lsattr(uid, name)
    
    def create(self, uid, name):
        self.check_path(uid, name)
        return self._temp.create(uid, name)
    
    def open(self, uid, name, flags):
        if self.manager:
            flags = 0
        return self._temp.open(uid, name, flags)
    
    def _release(self, uid, name, fh, force=False):
        self._temp.close(uid, name, fh, force)
    
    def release(self, uid, name, fh):
        self._release(uid, name, fh)
        
    def _unlink(self, uid, name):
        if not self.manager:
            return
        child = self.child(name)
        parent = self.parent(name)
        if parent != child:
            if child == ATTR_HANDLER:
                self.manager.synchronizer.remove_handler(parent)
            elif child == ATTR_FILTER:
                self.manager.synchronizer.remove_filter(parent)
            elif child == ATTR_MODE:
                self.manager.synchronizer.remove_mode(parent)
    
    def unlink(self, uid, name):
        self.remove(uid, name)
        self._unlink(uid, name)
    
    def invalidate(self, uid, name):
        path = self.get_path(uid, name)
        temp = self.path2temp(path)
        if self.file.exists(uid, path):
            self.file.rename(uid, path, temp)
            self._unlink(uid, name)
        else:
            self.file.touch(uid, temp)
    
    def signature(self, uid, name):
        path = self.path2temp(self.get_path(uid, name))
        with open(path, 'rb') as f:
            sig = librsync.signature(f)
        return encodestring(sig.read())
    
    def patch(self, uid, name, buf):
        delta = StringIO(decodestring(buf))
        dest = self.get_path(uid, name)
        src = self.path2temp(dest)
        with open(dest, 'wb') as f_dest:
            with open(src, 'rb') as f_src:
                try:
                    librsync.patch(f_src, delta, f_dest)
                except:
                    # warning
                    self.file.rename(uid, src, dest)
                    return
        self.file.remove(uid, src)
        self.file.save(uid, dest, self._temp.get_path(uid, name))
    
    def readdir(self, uid, name):
        return self.lsdir(uid, name)
    
    def _create_filter(self, uid, name, val):
        name = os.path.join(name, ATTR_FILTER)
        f = self.create(uid, name)
        try:
            os.write(f, str(val))
        finally:
            self._release(uid, name, f, force=True)
    
    def _create_mode(self, uid, name, val):
        name = os.path.join(name, ATTR_MODE)
        f = self.create(uid, name)
        try:
            os.write(f, str(val))
        finally:
            self._release(uid, name, f, force=True)
    
    def _create_freq(self, uid, name, val):
        name = os.path.join(name, ATTR_FREQ)
        f = self.create(uid, name)
        try:
            os.write(f, str(val))
        finally:
            self._release(uid, name, f, force=True)
    
    def _create_handler(self, uid, name, val):
        name = os.path.join(name, ATTR_HANDLER)
        f = self.create(uid, name)
        try:
            os.write(f, str(val))
        finally:
            self._release(uid, name, f, force=True)
    
    def _create_profile(self, uid, name, val):
        name = os.path.join(name, ATTR_PROFILE)
        f = self.create(uid, name)
        try:
            for i in val:
                os.write(f, '%s=%s\n' % (str(i), str(val[i])))
        finally:
            self._release(uid, name, f, force=True)
    
    def _create_dispatcher(self, uid, name, val):
        name = os.path.join(name, ATTR_DISPATCHER)
        f = self.create(uid, name)
        try:
            os.write(f, str(val))
        finally:
            self._release(uid, name, f, force=True)
    
    def initialize(self, uid, name, attr):
        if type(attr) != dict or len(attr) != 1:
            log_err(self, 'failed to initialize, invalid attr')
            raise Exception(log_get(self, 'failed to initialize, invalid attr'))
        key = attr.keys()[0]
        val = attr[key]
        if key == ATTR_FILTER:
            self._create_filter(uid, name, val)
        elif key == ATTR_MODE:
            self._create_mode(uid, name, val)
        elif key == ATTR_FREQ:
            self._create_freq(uid, name, val)
        elif key == ATTR_HANDLER:
            self._create_handler(uid, name, val)
        elif key == ATTR_PROFILE:
            self._create_profile(uid, name, val)
        elif key == ATTR_DISPATCHER:
            self._create_dispatcher(uid, name, val)
    