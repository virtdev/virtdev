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
from task import VDevAuthTask
from StringIO import StringIO
from conf.virtdev import VDEV_FS_MOUNTPOINT
from base64 import encodestring, decodestring
from dev.vdev import mount_device, update_device

class Device(VDevAuthTask):    
    def get(self, uid, name):
        device = self.query.device_get(name)
        if device:
            if uid != device['uid']:
                guests = self.query.guest_get(uid)
                if not guests or name not in guests:
                    return 
            return device
    
    def add(self, uid, node, addr, name, mode, freq, profile):
        if mode != None and profile != None:
            mount_device(uid, name, mode, freq, profile)
        update_device(self.query, uid, node, addr, name)
        self.query.event_put(uid, name)
        return True
    
    def remove(self, uid, node, name):
        self.query.device_remove(name)
        self.query.member_remove(uid, (name, node))
        self.query.event_put(uid, name)
        return True
    
    def sync(self, uid, name, buf):
        try:
            fields = ast.literal_eval(buf)
            if type(fields) != dict:
                log_err(self, 'failed to sync, invalid type')
                return
        except:
            log_err(self, 'failed to sync')
            return
        path = os.path.join(VDEV_FS_MOUNTPOINT, uid, name)
        with open(path, 'w') as f:
            f.write(buf)
        self.query.history_put(name, **fields)
        self.query.event_put(uid, name)
        return True
    
    def diff(self, uid, name, label, item, buf):
        sig = StringIO(decodestring(buf))
        path = os.path.join(VDEV_FS_MOUNTPOINT, uid, label, name, item)
        with open(path, 'r') as f:
            delta = librsync.delta(f, sig)
        return encodestring(delta.read())
    