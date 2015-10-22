#      device.py
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
#      MA 02110-1301, USA

import os
import ast
import json
import librsync
from lib.log import log_err
from db.marker import Marker
from StringIO import StringIO
from lib.mode import MODE_LINK
from srv.service import Service
from conf.path import PATH_MOUNTPOINT
from base64 import b64encode, b64decode
from conf.virtdev import EXTEND, AREA_CODE
from lib.util import DEVICE_DOMAIN, mount_device, update_device

ATTR_LEN = 1024
RECORD_LEN = 1 << 26

class Device(Service):
    def __init__(self, query):
        Service.__init__(self, query)
        if EXTEND:
            self._marker = Marker()
    
    def _mark(self, name):
        if EXTEND:
            self._marker.mark(name, DEVICE_DOMAIN, AREA_CODE)
    
    def get(self, uid, name):
        device = self._query.device.get(name)
        if device:
            if uid != device['uid']:
                guests = self._query.guest.get(uid)
                if not guests or name not in guests:
                    return 
            return device
    
    def add(self, uid, node, addr, name, mode, freq, prof):
        if mode != None and prof != None:
            mount_device(uid, name, mode | MODE_LINK, freq, prof)
            self._mark(name)
        update_device(self._query, uid, node, addr, name)
        self._query.event.put(uid, name)
        return True
    
    def remove(self, uid, node, name):
        self._query.device.remove(name)
        self._query.member.remove(uid, (name, node))
        self._query.event.put(uid, name)
        return True
    
    def update(self, uid, name, buf):
        try:
            fields = ast.literal_eval(buf)
            if type(fields) != dict:
                log_err(self, 'failed to update, name=%s' % str(name))
                return
        except:
            log_err(self, 'failed to update, name=%s' % str(name))
            return
        record = json.dumps(fields)
        if len(record) > RECORD_LEN:
            log_err(self, 'failed to update, name=%s' % str(name))
            return
        path = os.path.join(PATH_MOUNTPOINT, uid, name)
        with open(path, 'w') as f:
            f.write(record)
        self._query.history.put(uid, name, **fields)
        self._query.event.put(uid, name)
        return True
    
    def diff(self, uid, name, label, item, buf):
        sig = StringIO(b64decode(buf))
        path = os.path.join(PATH_MOUNTPOINT, uid, label, name, item)
        fd = os.open(path, os.O_RDONLY)
        if fd < 0:
            log_err(self, 'failed to diff, name=%s' % str(name))
            return
        try:
            dest = StringIO(os.read(fd, ATTR_LEN))
        finally:
            os.close(fd)
        delta = librsync.delta(dest, sig)
        return b64encode(delta.read())
