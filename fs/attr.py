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
import json
import librsync
from temp import Temp
from path import Path
from StringIO import StringIO
from lib.util import path2temp
from base64 import b64encode, b64decode
from lib.log import log_err, log_get, log

PRINT = False
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
    def __init__(self, router=None, core=None, rdonly=True):
        Path.__init__(self, router, core)
        self._temp = Temp(self, rdonly)
    
    def _print(self, text):
        if PRINT:
            log(log_get(self, text))
    
    def can_invalidate(self):
        return True
    
    def truncate(self, uid, name, length):
        if not self._core:
            path = self.get_path(uid, name)
            self._file.truncate(uid, path, length)
            self._temp.truncate(uid, name, length)
    
    def is_expired(self, uid, name):
        temp = path2temp(self.get_path(uid, name))
        return self._file.exists(uid, temp)
    
    def getattr(self, uid, name):
        return self.lsattr(uid, name)
    
    def create(self, uid, name):
        self.check_path(uid, name)
        return self._temp.create(uid, name)
    
    def open(self, uid, name, flags):
        if self._core:
            flags = 0
        return self._temp.open(uid, name, flags)
    
    def release(self, uid, name, fh):
        return self._temp.release(uid, name, fh)
    
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
            elif child == ATTR_DISPATCHER:
                self._core.remove_dispatcher(parent)
            elif child == ATTR_MODE:
                self._core.remove_mode(parent)
            elif child == ATTR_FREQ:
                self._core.remove_freq(parent)
            elif child == ATTR_TIMEOUT:
                self._core.remove_timeout(parent)
    
    def unlink(self, uid, name):
        self.remove(uid, name)
        self._unlink(uid, name)
    
    def invalidate(self, uid, name):
        self._print('invalidate, name=%s' % name)
        path = self.get_path(uid, name)
        temp = path2temp(path)
        if self._file.exists(uid, path):
            self._file.rename(uid, path, temp)
            self._unlink(uid, name)
        else:
            self._file.touch(uid, temp)
    
    def signature(self, uid, name):
        temp = path2temp(self.get_path(uid, name))
        with open(temp, 'rb') as f:
            sig = librsync.signature(f)
        return b64encode(sig.read())
    
    def patch(self, uid, name, buf):
        if not buf:
            log_err(self, 'failed to patch')
            raise Exception(log_get(self, 'failed to patch'))
        delta = StringIO(b64decode(buf))
        dest = self.get_path(uid, name)
        src = path2temp(dest)
        with open(dest, 'wb') as f_dest:
            with open(src, 'rb') as f_src:
                try:
                    librsync.patch(f_src, delta, f_dest)
                except:
                    # FIXME:
                    self._file.rename(uid, src, dest)
                    return
        self._file.remove(uid, src)
    
    def readdir(self, uid, name):
        return self.lsdir(uid, name)
    
    def _create_attr(self, uid, name, attr, val):
        drop = False
        name = os.path.join(name, attr)
        f = self.create(uid, name)
        try:
            os.write(f, str(val))
        except:
            drop = True
        finally:
            os.close(f)
        if drop:
            self.drop(uid, name)
            return
        self.update(uid, name)
    
    def initialize(self, uid, name, attr, val):
        if attr not in ATTRIBUTES:
            log_err(self, 'failed to initialize, invalid attribute %s' % str(attr))
            raise Exception(log_get(self, 'failed to initialize'))
        if attr == ATTR_PROFILE:
            val = json.dumps(val)
        self._create_attr(uid, name, attr, val)
    
    def drop(self, uid, name):
        return self._temp.drop(uid, name)
    
    def update(self, uid, name):
        return self._temp.update(uid, name)
