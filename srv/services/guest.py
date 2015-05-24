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
from srv.service import Service
from lib.op import OP_JOIN, OP_ACCEPT

class Guest(Service):
    def join(self, uid, dest, src):
        device = self._query.device.get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to join, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return  False
        user = self._query.user.get({'uid':device['uid']}, 'user')
        if not user:
            log_err(self, 'failed to join, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return  False
        node = device['node']
        device = self._query.device.get(dest)
        if not device or device['uid'] == uid:
            log_err(self, 'failed to join, invalid dest, guest=%s, host=%s' % (str(dest), str(src)))
            return  False
        self._query.link.put(name='', op=OP_JOIN, uid=device['uid'], node=device['node'], addr=device['addr'], dest=dest, src={'uid':uid, 'user':user, 'node':node, 'name':src})
        return True
    
    def accept(self, uid, dest, src):
        device = self._query.device.get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to accept, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        user = self._query.user.get({'uid':device['uid']}, 'user')
        if not user:
            log_err(self, 'failed to accept, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return  False
        node = device['node']
        device = self._query.device.get(dest)
        if not device or device.get('uid') == uid:
            log_err(self, 'failed to accept, invalid dest, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        self._query.link.put(name='', op=OP_ACCEPT, uid=device['uid'], node=device['node'], addr=device['addr'], dest=dest, src={'uid':uid, 'user':user, 'node':node, 'name':src})
        self._query.guest.put(device['uid'], src)
        return True
    
    def drop(self, uid, dest, src):
        device = self._query.device.get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to drop, invalid src, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        device = self._query.device.get(dest)
        if not device or device.get('uid') == uid:
            log_err(self, 'failed to drop, invalid dest, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        self._query.guest.remove(device['uid'], src)
        return True
