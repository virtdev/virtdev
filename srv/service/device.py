# device.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import ast
import json
from service import Service
from db.marker import Marker
from StringIO import StringIO
from lib.api import api_mount
from lib.domains import DOMAIN_DEV
from conf.route import ROUTE, AREA
from conf.virtdev import RSYNC, HISTORY
from base64 import b64encode, b64decode
from lib.log import log_err, log_warnning
from lib.util import update_device, get_mnt_path

MAX_LEN = 1 << 24
RECORD_LEN = 1 << 24

if RSYNC:
    import librsync

class Device(Service):
    def __init__(self, query):
        Service.__init__(self, query)
        if ROUTE:
            self._marker = Marker()
    
    def _mark(self, name):
        if ROUTE:
            self._marker.mark(name, DOMAIN_DEV, AREA)
    
    def find(self, uid, name):
        device = self._query.device.get(name)
        if device:
            if uid != device['uid']:
                guests = self._query.guest.get(uid)
                if not guests or name not in guests:
                    return 
            return device
    
    def add(self, uid, node, addr, name, mode, freq, prof):
        if mode != None and prof != None:
            api_mount(uid, name=name, mode=mode, freq=freq, prof=prof)
            self._mark(name)
        update_device(self._query, uid, node, addr, name)
        self._query.event.put(uid, name)
        return True
    
    def delete(self, uid, node, name):
        self._query.device.delete(name)
        self._query.member.delete(uid, (name, node))
        self._query.event.put(uid, name)
        return True
    
    def put(self, uid, name, buf):
        try:
            fields = ast.literal_eval(buf)
            if type(fields) != dict:
                log_err(self, 'failed to put, name=%s' % str(name))
                return
        except:
            log_err(self, 'failed to put, name=%s' % str(name))
            return
        record = json.dumps(fields)
        if len(record) > RECORD_LEN:
            log_err(self, 'failed to put, name=%s' % str(name))
            return
        path = get_mnt_path(uid, name)
        with open(path, 'w') as f:
            f.write(record)
        if HISTORY:
            self._query.history.put(uid, name, **fields)
        self._query.event.put(uid, name)
        return True
    
    def get(self, uid, name, field, item, buf):
        if RSYNC:
            sig = StringIO(b64decode(buf))
        mnt = get_mnt_path(uid)
        path = os.path.join(mnt, field, name, item)
        fd = os.open(path, os.O_RDONLY)
        try:
            res = os.read(fd, MAX_LEN)
        except:
            log_err(self, 'failed to get, cannot read, name=%s' % str(name))
            return
        finally:
            os.close(fd)
        if not res:
            log_warnning(self, 'failed to get, no content, name=%s' % str(name))
            return
        if RSYNC:
            tmp = StringIO(res)
            delta = librsync.delta(tmp, sig)
            res = delta.read()
        return b64encode(res)
