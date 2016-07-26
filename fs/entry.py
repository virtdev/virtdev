#      entry.py
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

import os
import stat
from lib.log import log_err
from fuse import FuseOSError
from lib.util import get_temp
from errno import EINVAL, ENOENT
from conf.env import PATH_VAR, PATH_MNT
from lib.fields import FIELDS, FIELD_DATA, FIELD_ATTR

class Entry(object):
    def __init__(self, router=None, core=None):
        if not os.path.exists(PATH_VAR):
            os.mkdir(PATH_VAR)
        name = self._get_name()
        if name in FIELDS:
            self._field = name
        else:
            log_err(self, 'invalid entry')
            raise Exception('Error: invalid entry')
        if not router:
            from interface.localfs import LocalFS
            self._fs = LocalFS()
        else:
            from interface.remotefs import RemoteFS
            self._fs = RemoteFS(router)
        self._core = core
    
    def _get_name(self):
        return self.__class__.__name__.lower()
    
    @property
    def field(self):
        return self._field
    
    def can_load(self):
        return False
    
    def can_scan(self):
        return False
    
    def can_touch(self):
        return False
    
    def can_unlink(self):
        return False
    
    def can_invalidate(self):
        return False
    
    def can_enable(self):
        return False
    
    def can_disable(self):
        return False
    
    def is_expired(self, uid, name):
        return False
    
    def release(self, uid, name, fh):
        pass
    
    def truncate(self, uid, name, length):
        pass
    
    def discard(self, uid, name):
        pass
    
    def commit(self, uid, name):
        pass
    
    def patch(self, uid, name, buf):
        pass
    
    def signature(self, uid, name):
        return ''
    
    def get_mtime(self, uid, path):
        return self._fs.mtime(uid, path)
    
    def load(self, uid, src, dest):
        return self._fs.load(uid, src, dest)
    
    def save(self, uid, src, dest):
        return self._fs.save(uid, src, dest)
    
    def parent(self, name):
        tmp = name.split('/')
        if len(tmp) >= 2:
            return tmp[-2]
        else:
            return tmp[0]
    
    def child(self, name):
        return str(name).split('/')[-1] 
    
    def real(self, name):
        if self._field == FIELD_DATA:
            return name
        elif self._field == FIELD_ATTR:
            return os.path.join(self._field, name)
        else:
            tmp = name.split('/')
            if len(tmp) >= 2:
                name = tmp[-2:]
            else:
                name = tmp[0]
            return os.path.join(self._field, *name)
    
    def get_path(self, uid, name='', parent=''):
        return str(os.path.join(PATH_VAR, uid, FIELDS[self._field], parent, name))
    
    def check_path(self, uid, name='', parent=''):
        path = self.get_path(uid, name, parent)
        parent = os.path.dirname(path)
        if not self._fs.exists(uid, parent):
            self._fs.mkdir(uid, parent)
        if not self._fs.exists(uid, path):
            self._fs.touch(uid, path)
    
    def symlink(self, uid, name):
        child = self.child(name)
        parent = self.parent(name)
        if child == parent:
            log_err(self, 'failed to create symlink')
            raise FuseOSError(EINVAL) 
        path = os.path.join(self.get_path(uid, parent), child)
        self._fs.touch(uid, path)   
    
    def remove(self, uid, name):
        path = self.get_path(uid, name)
        self._fs.remove(uid, path)
    
    def invalidate(self, uid, name):
        path = self.get_path(uid, name)
        self._fs.remove(uid, path)
    
    def lsdir(self, uid, name):
        path = self.get_path(uid, name)
        return self._fs.lsdir(uid, path)
    
    def lslink(self, uid, name):
        child = self.child(name)
        return os.path.join(PATH_MNT, uid, self._field, child)
    
    def lsattr(self, uid, name, symlink=False):
        st = None
        path = self.get_path(uid, name)
        
        if self._fs.exists(uid, path):
            st = self._fs.stat(uid, path)
        elif self.can_invalidate():
            path = get_temp(path)
            try:
                st = self._fs.stat(uid, path)
            except:
                raise FuseOSError(ENOENT)
        
        if not st:
            raise FuseOSError(ENOENT)
        
        if symlink:
            parent = self.parent(name)
            child = self.child(name)
            if parent != child:
                path = self.get_path(uid, child)
                if os.path.exists(path):
                    mode = st['st_mode']
                    mode = mode & (~stat.S_IFDIR) & (~stat.S_IFREG)
                    mode = mode | stat.S_IFLNK
                    st['st_mode'] = mode
                    st['st_nlink'] = 2
        
        return st
