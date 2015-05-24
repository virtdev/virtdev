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
from path import Path
from StringIO import StringIO
from lib.util import check_profile
from lib.log import log_err, log_get
from base64 import encodestring, decodestring

ATTR_MODE = 'mode'
ATTR_FREQ = 'freq'
ATTR_FILTER = 'filter'
ATTR_PARENT = 'parent'
ATTR_HANDLER = 'handler'
ATTR_PROFILE = 'profile'
ATTR_TIMEOUT = 'timeout'
ATTR_DISPATCHER = 'dispatcher'

ATTRIBUTES = [ATTR_MODE, ATTR_FREQ, ATTR_FILTER, ATTR_HANDLER, ATTR_PARENT, ATTR_PROFILE, ATTR_TIMEOUT, ATTR_DISPATCHER]

class Attr(Path):
    def __init__(self, watcher=None, router=None, core=None):
        Path.__init__(self, router, core)
        self._temp = Temp(self, watcher)
    
    def can_invalidate(self):
        return True
    
    def truncate(self, uid, name, length):
        if not self._core:
            path = self.get_path(uid, name)
            self._file.truncate(uid, path, length)
            self._temp.truncate(uid, name, length)
    
    def may_update(self, flags):
        return flags & os.O_APPEND or flags & os.O_RDWR or flags & os.O_TRUNC or flags & os.O_WRONLY
    
    def is_expired(self, uid, name):
        path = self.path2temp(self.get_path(uid, name))
        return self._file.exists(uid, path)
    
    def getattr(self, uid, name):
        return self.lsattr(uid, name)
    
    def create(self, uid, name):
        self.check_path(uid, name)
        return self._temp.create(uid, name)
    
    def open(self, uid, name, flags):
        if self._core:
            flags = 0
        return self._temp.open(uid, name, flags)
    
    def _release(self, uid, name, fh, force=False):
        self._temp.close(uid, name, fh, force)
    
    def release(self, uid, name, fh):
        self._release(uid, name, fh)
    
    def _unlink(self, uid, name):
        if not self._core:
            return
        child = self.child(name)
        parent = self.parent(name)
        if parent != child:
            if child == ATTR_HANDLER:
                self._core.remove_handler(parent)
            elif child == ATTR_FILTER:
                self._core.remove_filter(parent)
            elif child == ATTR_MODE:
                self._core.remove_mode(parent)
            elif child == ATTR_TIMEOUT:
                self._core.remove_timeout(parent)
    
    def unlink(self, uid, name):
        self.remove(uid, name)
        self._unlink(uid, name)
    
    def invalidate(self, uid, name):
        path = self.get_path(uid, name)
        temp = self.path2temp(path)
        if self._file.exists(uid, path):
            self._file.rename(uid, path, temp)
            self._unlink(uid, name)
        else:
            self._file.touch(uid, temp)
    
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
                    self._file.rename(uid, src, dest)
                    return
        self._file.remove(uid, src)
        self._file.save(uid, dest, self._temp.get_path(uid, name))
    
    def readdir(self, uid, name):
        return self.lsdir(uid, name)
    
    def _init_attr(self, uid, name, attr, val):
        name = os.path.join(name, attr)
        f = self.create(uid, name)
        try:
            os.write(f, str(val))
        finally:
            self._release(uid, name, f, force=True)
    
    def initialize(self, uid, name, attr, val):
        if attr not in ATTRIBUTES:
            log_err(self, 'invalid attribute %s' % str(attr))
            raise Exception(log_get(self,' invalid attribute'))
        if attr == ATTR_PROFILE:
            tmp = ''
            for i in val:
                tmp += '%s=%s\n' % (str(i), str(val[i]))
            val = tmp
        self._init_attr(uid, name, attr, val)
    
    def get_profile(self, uid, name):
        path = os.path.join(name, ATTR_PROFILE)
        fd = self.open(uid, path, 0)
        if not fd:
            log_err(self, 'failed to get profile')
            raise Exception(log_get(self, 'failed to get profile'))
        try:
            st = os.fstat(fd)
            buf = os.read(fd, st.st_size)
            if buf:
                return check_profile(buf.strip().split('\n')) 
        finally:
            self.release(uid, path, fd)
