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
import librsync
from lib.log import log_err
from StringIO import StringIO
from lib.mode import MODE_LINK
from srv.service import Service
from conf.virtdev import MOUNTPOINT
from base64 import encodestring, decodestring
from lib.util import mount_device, update_device

class Device(Service):
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
                log_err(self, 'failed to update, invalid type')
                return
        except:
            log_err(self, 'failed to update')
            return
        path = os.path.join(MOUNTPOINT, uid, name)
        with open(path, 'w') as f:
            f.write(buf)
        self._query.history.put(name, **fields)
        self._query.event.put(uid, name)
        return True
    
    def diff(self, uid, name, label, item, buf):
        sig = StringIO(decodestring(buf))
        path = os.path.join(MOUNTPOINT, uid, label, name, item)
        with open(path, 'r') as f:
            delta = librsync.delta(f, sig)
        return encodestring(delta.read())
    