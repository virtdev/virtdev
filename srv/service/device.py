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
from service import Service
from db.marker import Marker
from StringIO import StringIO
from lib.modes import MODE_LINK
from lib.domains import DOMAIN_DEV
from base64 import b64encode, b64decode
from lib.log import log_err, log_warnning
from lib.util import mount_device, update_device
from conf.virtdev import EXTEND, RSYNC, AREA_CODE, PATH_MNT

MAX_LEN = 1 << 24
RECORD_LEN = 1 << 24

class Device(Service):
    def __init__(self, query):
        Service.__init__(self, query)
        if EXTEND:
            self._marker = Marker()
    
    def _mark(self, name):
        if EXTEND:
            self._marker.mark(name, DOMAIN_DEV, AREA_CODE)
    
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
        path = os.path.join(PATH_MNT, uid, name)
        with open(path, 'w') as f:
            f.write(record)
        self._query.history.put(uid, name, **fields)
        self._query.event.put(uid, name)
        return True
    
    def diff(self, uid, name, field, item, buf):
        if RSYNC:
            sig = StringIO(b64decode(buf))
        path = os.path.join(PATH_MNT, uid, field, name, item)
        fd = os.open(path, os.O_RDONLY)
        try:
            res = os.read(fd, MAX_LEN)
        except:
            log_err(self, 'failed to read, name=%s' % str(name))
            return
        finally:
            os.close(fd)
        if not res:
            log_warnning(self, 'no content, name=%s' % str(name))
            return
        if RSYNC:
            tmp = StringIO(res)
            delta = librsync.delta(tmp, sig)
            res = delta.read()
        return b64encode(res)
