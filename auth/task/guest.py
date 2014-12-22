#      guest.py
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

from lib.log import log_err
from task import VDevAuthTask
from lib.util import DEFAULT_NAME
from fs.oper import OP_JOIN, OP_ACCEPT

class Guest(VDevAuthTask):
    def join(self, uid, dest, src):
        device = self.query.device_get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to join, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return  False
        user = self.query.user_get({'uid':device['uid']}, 'user')
        if not user:
            log_err(self, 'failed to join, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return  False
        node = device['node']
        device = self.query.device_get(dest)
        if not device or device['uid'] == uid:
            log_err(self, 'failed to join, invalid dest, guest=%s, host=%s' % (str(dest), str(src)))
            return  False
        self.query.link.put(name=DEFAULT_NAME, op=OP_JOIN, addr=device['addr'], uid=device['uid'], dest=dest, src={'uid':uid, 'user':user, 'node':node, 'name':src})
        return True
    
    def accept(self, uid, dest, src):
        device = self.query.device_get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to accept, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        user = self.query.user_get({'uid':device['uid']}, 'user')
        if not user:
            log_err(self, 'failed to accept, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return  False
        node = device['node']
        device = self.query.device_get(dest)
        if not device or device.get('uid') == uid:
            log_err(self, 'failed to accept, invalid dest, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        self.query.link.put(name=DEFAULT_NAME, op=OP_ACCEPT, addr=device['addr'], uid=device['uid'], dest=dest, src={'uid':uid, 'user':user, 'node':node, 'name':src})
        self.query.guest_put(device['uid'], src)
        return True
    
    def drop(self, uid, dest, src):
        device = self.query.device_get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to drop, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        device = self.query.device_get(dest)
        if not device or device.get('uid') == uid:
            log_err(self, 'failed to drop, invalid dest, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        self.query.guest_remove(device['uid'], src)
        return True
    