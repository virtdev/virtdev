#      oper.py
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
import xattr
from subprocess import call
from lib.util import DEVNULL, cat
from conf.virtdev import MOUNTPOINT
from lib.op import OP_MOUNT, OP_INVALIDATE

RETRY_MAX = 2

class Operation(object):
    def __init__(self, manager):
        self._manager = manager
        self.uid = manager.uid
    
    def _get_path(self, path=''):
        if path:
            return os.path.join(MOUNTPOINT, self.uid, path)
        else:
            return os.path.join(MOUNTPOINT, self.uid)
    
    def _touch(self, path):
        call(['touch', path], stderr=DEVNULL, stdout=DEVNULL)
    
    def mount(self, attr):
        path = self._get_path()
        xattr.setxattr(path, OP_MOUNT, str(attr))
        return True
    
    def invalidate(self, path):
        path = self._get_path(path)
        if not os.path.exists(path):
            self._touch(path)
        xattr.setxattr(path, OP_INVALIDATE, "", symlink=True)
        return True
    
    def touch(self, path):
        path = self._get_path(path)
        self._touch(path)
        return True
    
    def put(self, dest, src, buf, flags):
        for _ in range(RETRY_MAX):
            if self._manager.core.put(dest, src, buf, flags):
                return True
    
    def enable(self, path):
        self._manager.device.open(path)
        return True
    
    def disable(self, path):
        self._manager.device.close(path)
        return True
    
    def join(self, dest, src):
        self._manager.notify('wait', json.dumps({'dest':dest, 'src':src}))
        return True
    
    def accept(self, dest, src):
        uid = str(src['uid'])
        name = str(src['name'])
        user = str(src['user'])
        node = str(src['node'])
        self._manager.member.update({name:{'uid':uid, 'user':user, 'node':node, 'state':'join'}})
        self._manager.notify('list', cat(name, 'join'))
        return True
