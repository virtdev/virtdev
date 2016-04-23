#      oper.py
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
import json
from subprocess import call
from conf.virtdev import PATH_MNT
from lib.util import DEVNULL, invalidate, mount

RETRY_MAX = 2

class Operation(object):
    def __init__(self, manager):
        self._manager = manager
        self._uid = manager.uid
    
    def _get_path(self, path):
        return os.path.join(PATH_MNT, self._uid, path)
    
    def mount(self, attr):
        mount(self._uid, attr)
    
    def invalidate(self, path):
        path = self._get_path(path)
        if not os.path.exists(path):
            self._touch(path)
        invalidate(path)
    
    def touch(self, path):
        path = self._get_path(path)
        call(['touch', path], stderr=DEVNULL, stdout=DEVNULL)
    
    def put(self, dest, src, buf, flags):
        for _ in range(RETRY_MAX):
            if self._manager.core.put(dest, src, buf, flags):
                return
    
    def enable(self, path):
        self._manager.device.open(path)
    
    def disable(self, path):
        self._manager.device.close(path)
    
    def join(self, req):
        self._manager.notify('wait', json.dumps(req))
    
    def accept(self, req):
        user = str(req['user'])
        node = str(req['node'])
        name = str(req['name'])
        self._manager.member.update({name:{'user':user, 'node':node, 'state':'join'}})
        self._manager.notify('list', json.dumps({'state':'join', 'name':name}))
