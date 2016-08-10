# operation.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import json
from conf.env import PATH_MNT
from lib.util import invalidate, mount, touch

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
            touch(path)
        invalidate(path)
    
    def touch(self, path):
        path = self._get_path(path)
        touch(path)
    
    def put(self, dest, src, buf, flags):
        self._manager.core.put(dest, src, buf, flags)
    
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
