# operation.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import json
from lib.api import api_invalidate, api_exists, api_mount, api_touch

class Operation(object):
    def __init__(self, manager):
        self._manager = manager
        self._uid = manager.uid
    
    def mount(self, attr):
        attr = eval(attr)
        api_mount(self._uid, **attr)
    
    def invalidate(self, path):
        if not api_exists(self._uid, path):
            api_touch(self._uid, path)
        api_invalidate(self._uid, path)
    
    def touch(self, path):
        api_touch(self._uid, path)
    
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
