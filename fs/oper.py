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
from lib.util import cat
from lib.log import log_err
from conf.virtdev import VDEV_FS_MOUNTPOINT

OP_ADD  = 'add'
OP_LOAD = 'load'
OP_JOIN = 'join'
OP_POLL = 'poll'
OP_SYNC = 'sync'
OP_DIFF = 'diff'
OP_FORK = 'fork'
OP_MOUNT = 'mount'
OP_TOUCH = 'touch'
OP_ACCEPT = 'accept'
OP_ENABLE = 'enable'
OP_CREATE = 'create'
OP_DISABLE = 'disable'
OP_COMBINE = 'combine'
OP_INVALIDATE = 'invalidate'

RETRY_MAX = 2

class VDevFSOperation(object):
    def __init__(self, manager):
        self.manager = manager
        self.uid = manager.uid
    
    def _get_path(self, path=''):
        if path:
            return os.path.join(VDEV_FS_MOUNTPOINT, self.uid, path)
        else:
            return os.path.join(VDEV_FS_MOUNTPOINT, self.uid)
    
    def mount(self, attr):
        try:
            path = self._get_path()
            xattr.setxattr(path, OP_MOUNT, str(attr))
            return True
        except:
            log_err(self, 'failed to mount, path=%s' % path)
    
    def invalidate(self, path):
        try:
            path = self._get_path(path)
            if not os.path.exists(path):
                with open(path, 'w') as _:
                    pass
            xattr.setxattr(path, OP_INVALIDATE, "", symlink=True)
            return True
        except:
            log_err(self, 'failed to invalidate, path=%s' % path)
    
    def touch(self, path):
        try:
            path = self._get_path(path)
            with open(path, 'w') as _:
                pass
            return True
        except:
            log_err(self, 'failed to touch, path=%s' % path)
    
    def put(self, dest, src, buf, flags):
        i = 0
        try:
            while i < RETRY_MAX:
                if self.manager.synchronizer.put(dest, src, buf, flags):
                    return True
                i += 1
        except:
            log_err(self, 'failed to put, dest=%s, src=%s' % (dest, src))
    
    def enable(self, path):
        try:
            self.manager.device.open(path)
            return True
        except:
            log_err(self, 'failed to enable, path=%s' % path)
    
    def disable(self, path):
        try:
            self.manager.device.close(path)
            return True
        except:
            log_err(self, 'failed to disable, path=%s' % path)
    
    def join(self, dest, src):
        try:
            self.manager.notify('wait', json.dumps({'dest':dest, 'src':src}))
            return True
        except:
            log_err(self, 'failed to join, dest=%s, src=%s' % (str(dest), str(src)))
    
    def accept(self, dest, src):
        try:
            uid = str(src['uid'])
            name = str(src['name'])
            user = str(src['user'])
            node = str(src['node'])
            self.manager.member.update({name:{'uid':uid, 'user':user, 'node':node, 'state':'join'}})
            self.manager.notify('list', cat(name, 'join'))
            return True
        except:
            log_err(self, 'failed to accept, dest=%s, src=%s' % (str(dest), str(src)))
    