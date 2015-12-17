#      temp.py
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
from conf.path import PATH_FS
from lib.domains import TEMP, DOMAINS

class Temp(object):
    def __init__(self, parent, rdonly):
        self._parent = parent
        self._rdonly = rdonly
        self._watcher = None
        if not self._rdonly:
            from watcher import Watcher
            self._watcher = Watcher()
    
    def _get_name(self, name):
        ret = ''
        length = len(name)
        for i in range(length):
            if name[i] == '/':
                ret += '_'
            else:
                ret += name[i]
        return ret
    
    def _get_dir(self, uid, domain):
        return os.path.join(PATH_FS, uid, TEMP, DOMAINS[domain])
    
    def _check_dir(self, uid, domain):
        path = self._get_dir(uid, domain)
        if not os.path.exists(path):
            os.makedirs(path, 0644)
        return path
    
    def _check_path(self, uid, name):
        name = self._get_name(name)
        dirname = self._check_dir(uid, self._parent.domain)
        return self._get_path(dirname, name)
    
    def _get_path(self, dirname, name, suffix=''):
        path = os.path.join(dirname, name)
        if suffix:
            path += '.' + suffix
        return path
    
    def get_path(self, uid, name, suffix=''):
        name = self._get_name(name)
        dirname = self._get_dir(uid, self._parent.domain)
        return self._get_path(dirname, name, suffix)
    
    def mtime(self, uid, name):
        path = self.get_path(uid, name, 'mtime')
        try:
            with open(path, 'r') as f:
                t = long(f.read().strip())
            return t
        except:
            return 0
    
    def set_mtime(self, uid, name, t):
        path = self.get_path(uid, name, 'mtime')
        with open(path, 'w') as f:
            f.write(str(t))
    
    def create(self, uid, name):
        path = self._check_path(uid, name)
        ret = os.open(path, os.O_RDWR | os.O_CREAT, 0644)
        if ret >= 0 and self._watcher:
            self._watcher.push(path)
        return ret
    
    def truncate(self, uid, name, length):
        path = self.get_path(uid, name)
        with open(path, 'r+') as f:
            f.truncate(length)
    
    def open(self, uid, name, flags):
        path = self._check_path(uid, name)
        parent = self._parent.get_path(uid, name)
        t = self._parent.get_mtime(uid, parent)
        if not t or t != self.mtime(uid, name):
            self._parent.load_file(uid, parent, path)
            if t:
                self.set_mtime(uid, name, t)
        mode = os.O_RDWR
        if flags & os.O_TRUNC:
            mode |= os.O_TRUNC
        ret = os.open(path, mode, 0644)
        if ret >= 0 and self._watcher:
            self._watcher.register(path)
        return ret
    
    def release(self, uid, name, fh):
        os.close(fh)
        if not self._rdonly:
            path = self.get_path(uid, name)
            if self._watcher and self._watcher.pop(path):
                return True
    
    def update(self, uid, name):
        path = self.get_path(uid, name)
        parent = self._parent.get_path(uid, name)
        self._parent.save_file(uid, path, parent)
    
    def drop(self, uid, name):
        path = self.get_path(uid, name)
        if os.path.exists(path):
            os.remove(path)
