#      file.py
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

import errno
from lib.log import log_err
from fuse import FuseOSError

class File(object):
    def _undefined_op(self):
        log_err(self, 'undefined operation')
        raise FuseOSError(errno.EINVAL)
    
    def load(self, uid, src, dest):
        self._undefined_op()
    
    def save(self, uid, src, dest):
        self._undefined_op()
    
    def remove(self, uid, path):
        self._undefined_op()
    
    def mkdir(self, uid, path):
        self._undefined_op()
    
    def lsdir(self, uid, path):
        self._undefined_op()
    
    def exists(self, uid, path):
        self._undefined_op()
    
    def touch(self, uid, path):
        self._undefined_op()
    
    def rename(self, uid, src, dest):
        self._undefined_op()
    
    def stat(self, uid, path):
        self._undefined_op()
    
    def truncate(self, uid, path, length):
        self._undefined_op()
    
    def mtime(self, uid, path):
        self._undefined_op()
