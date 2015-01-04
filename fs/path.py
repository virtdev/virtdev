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
from errno import EINVAL, ENOENT
from conf.virtdev import VDEV_FS_PATH, VDEV_FS_MOUNTPOINT

VDEV_FS_UPDATE = 0x00000001
VDEV_FS_LABELS = {'vertex':'vertex', 'edge':'edge', 'data':'data', 'attr':'attr', 'temp':'temp'}
VDEV_FS_REFLEXIVITY = True

def undefined_opration():
    log_err(self, 'failed to truncate')
    raise FuseOSError(EINVAL)

def is_local(uid, name):
    path = os.path.join(VDEV_FS_MOUNTPOINT, uid, name)
    return os.path.exists(path)

def load(label, uid, name):
    if label not in VDEV_FS_LABELS.keys():
        return
    path = os.path.join(VDEV_FS_MOUNTPOINT, uid, VDEV_FS_LABELS[label], name)
    if not os.path.exists(path):
        return
    return os.listdir(path)

class VDevPath(object):
    def __init__(self, router=None, manager=None):
        if not os.path.exists(VDEV_FS_PATH):
            os.mkdir(VDEV_FS_PATH)
        name = self.__class__.__name__.lower()
        self._label = VDEV_FS_LABELS.get(name, '')
        if not router:
            from fi.local import VDevLocalFS
            self.fs = VDevLocalFS()
        else:
            from fi.remote import VDevRemoteFS
            self.fs = VDevRemoteFS(router)
        self.router = router
        self.manager = manager
    
    @property
    def label(self):
        return self._label
    
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
    
    def may_update(self, flags):
        return False
    
    def signature(self, uid, name):
        undefined_opration()
    
    def patch(self, uid, name, buf):
        undefined_opration()
    
    def is_expired(self, uid, name):
        return False
    
    def release(self, uid, name, fh):
        pass
    
    def parent(self, name):
        tmp = name.split('/')
        if len(tmp) >= 2:
            return tmp[-2]
        else:
            return tmp[0]
    
    def child(self, name):
        return str(name).split('/')[-1] 
    
    def real(self, name):
        if self._label == VDEV_FS_LABELS['data']:
            return name
        elif self._label == VDEV_FS_LABELS['attr']:
            return os.path.join(self._label, name)
        else:
            tmp = name.split('/')
            if len(tmp) >= 2:
                name = tmp[-2:]
            else:
                name = tmp[0]
            return os.path.join(self._label, *name)
    
    def path2temp(self, path):
        return path + '~'
    
    def get_path(self, uid, name='', parent=''):
        return str(os.path.join(VDEV_FS_PATH, uid, self._label, parent, name))
    
    def check_path(self, uid, name='', parent=''):
        path = self.get_path(uid, name, parent)
        parent = os.path.dirname(path)
        if not self.fs.exists(uid, parent):
            self.fs.mkdir(uid, parent)
        if not self.fs.exists(uid, path):
            self.fs.touch(uid, path)
    
    def symlink(self, uid, name):
        child = self.child(name)
        parent = self.parent(name)
        if not VDEV_FS_REFLEXIVITY and child == parent:
            log_err(self, 'failed to create symlink')
            raise FuseOSError(EINVAL) 
        path = os.path.join(self.get_path(uid, parent), child)
        self.fs.touch(uid, path)   
    
    def remove(self, uid, name):
        path = self.get_path(uid, name)
        self.fs.remove(uid, path)
    
    def invalidate(self, uid, name):
        path = self.get_path(uid, name)
        self.fs.remove(uid, path)
    
    def truncate(self, uid, name, length):
        pass
    
    def lsdir(self, uid, name):
        path = self.get_path(uid, name)
        return self.fs.lsdir(uid, path)
    
    def lslink(self, uid, name):
        child = self.child(name)
        return os.path.join(VDEV_FS_MOUNTPOINT, uid, self._label, child)
    
    def lsattr(self, uid, name, symlink=False):
        st = None
        path = self.get_path(uid, name)
        
        if self.fs.exists(uid, path):
            st = self.fs.stat(uid, path)
        elif self.can_invalidate():
            path = self.path2temp(path)
            try:
                st = self.fs.stat(uid, path)
            except:
                raise FuseOSError(ENOENT)
        
        if not st:
            raise FuseOSError(ENOENT)
        
        if symlink:
            parent = self.parent(name)
            child = self.child(name)
            if VDEV_FS_REFLEXIVITY or parent != child:
                path = self.get_path(uid, child)
                if os.path.exists(path):
                    mode = st['st_mode']
                    mode = mode & (~stat.S_IFDIR) & (~stat.S_IFREG)
                    mode = mode | stat.S_IFLNK
                    st['st_mode'] = mode
                    st['st_nlink'] = 2
        return st
    