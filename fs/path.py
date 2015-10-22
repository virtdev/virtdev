#      path.py
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
import stat
from lib.log import log_err
from fuse import FuseOSError
from lib.util import path2temp
from errno import EINVAL, ENOENT
from conf.path import PATH_FS, PATH_MOUNTPOINT

DOMAIN = {'vertex':'vertex', 'edge':'edge', 'data':'data', 'attr':'attr', 'temp':'temp'}

def is_local(uid, name):
    path = os.path.join(PATH_FS, uid, DOMAIN['attr'], name)
    return os.path.exists(path)

def load(uid, name='', domain='', sort=False, passthrough=False):
    if not passthrough:
        root = PATH_MOUNTPOINT
    else:
        root = PATH_FS
        if not domain:
            domain = DOMAIN['data']
    if not name and not domain:
        path = os.path.join(root, uid)
    else:
        if domain not in DOMAIN.keys():
            return
        path = os.path.join(root, uid, DOMAIN[domain], name)
    if not os.path.exists(path):
        return
    if not sort:
        return os.listdir(path)
    else:
        key = lambda f: os.stat(os.path.join(path, f)).st_mtime
        return sorted(os.listdir(path), key=key)

class Path(object):
    def __init__(self, router=None, core=None):
        if not os.path.exists(PATH_FS):
            os.mkdir(PATH_FS)
        name = self.__class__.__name__.lower()
        self._label = DOMAIN.get(name, '')
        if not router:
            from local import LocalFile
            self._file = LocalFile()
        else:
            from remote import RemoteFile
            self._file = RemoteFile(router)
        self._core = core
    
    def _undefined_op(self):
        log_err(self, 'undefined operation')
        raise FuseOSError(EINVAL)
    
    @property
    def label(self):
        return self._label
    
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
    
    def signature(self, uid, name):
        self._undefined_op()
    
    def patch(self, uid, name, buf):
        self._undefined_op()
    
    def is_expired(self, uid, name):
        return False
    
    def release(self, uid, name, fh):
        os.close(fh)
    
    def get_mtime(self, uid, path):
        return self._file.mtime(uid, path)
    
    def load_file(self, uid, src, dest):
        return self._file.load(uid, src, dest)
    
    def save_file(self, uid, src, dest):
        return self._file.save(uid, src, dest)
    
    def parent(self, name):
        tmp = name.split('/')
        if len(tmp) >= 2:
            return tmp[-2]
        else:
            return tmp[0]
    
    def child(self, name):
        return str(name).split('/')[-1] 
    
    def real(self, name):
        if self._label == DOMAIN['data']:
            return name
        elif self._label == DOMAIN['attr']:
            return os.path.join(self._label, name)
        else:
            tmp = name.split('/')
            if len(tmp) >= 2:
                name = tmp[-2:]
            else:
                name = tmp[0]
            return os.path.join(self._label, *name)
    
    def get_path(self, uid, name='', parent=''):
        return str(os.path.join(PATH_FS, uid, self._label, parent, name))
    
    def check_path(self, uid, name='', parent=''):
        path = self.get_path(uid, name, parent)
        parent = os.path.dirname(path)
        if not self._file.exists(uid, parent):
            self._file.mkdir(uid, parent)
        if not self._file.exists(uid, path):
            self._file.touch(uid, path)
    
    def symlink(self, uid, name):
        child = self.child(name)
        parent = self.parent(name)
        if child == parent:
            log_err(self, 'failed to create symlink')
            raise FuseOSError(EINVAL) 
        path = os.path.join(self.get_path(uid, parent), child)
        self._file.touch(uid, path)   
    
    def remove(self, uid, name):
        path = self.get_path(uid, name)
        self._file.remove(uid, path)
    
    def invalidate(self, uid, name):
        path = self.get_path(uid, name)
        self._file.remove(uid, path)
    
    def truncate(self, uid, name, length):
        pass
    
    def lsdir(self, uid, name):
        path = self.get_path(uid, name)
        return self._file.lsdir(uid, path)
    
    def lslink(self, uid, name):
        child = self.child(name)
        return os.path.join(PATH_MOUNTPOINT, uid, self._label, child)
    
    def lsattr(self, uid, name, symlink=False):
        st = None
        path = self.get_path(uid, name)
        
        if self._file.exists(uid, path):
            st = self._file.stat(uid, path)
        elif self.can_invalidate():
            path = path2temp(path)
            try:
                st = self._file.stat(uid, path)
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
    
    def drop(self, uid, name):
        pass
    
    def update(self, uid, name):
        pass
