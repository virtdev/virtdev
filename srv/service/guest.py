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

from service import Service
from lib.log import log_err
from lib.operations import OP_JOIN, OP_ACCEPT

class Guest(Service):
    def join(self, uid, user, dest, src):
        if not user or not dest or not src or dest == src:
            log_err(self, 'failed to join, user=%s, dest=%s, src=%s' % (str(user), str(dest), str(src)))
            return  False
        device = self._query.device.get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to join, invalid device %s' % str(src))
            return  False
        node = device['node']
        device = self._query.device.get(dest)
        if not device:
            log_err(self, 'failed to join, cannot find target device %s' % str(dest))
            return  False
        self._query.link.put(name='', op=OP_JOIN, uid=device['uid'], node=device['node'], addr=device['addr'], req={'user':user, 'node':node, 'name':src})
        return True
    
    def accept(self, uid, user, dest, src):
        if not user or not dest or not src or dest == src:
            log_err(self, 'failed to accept, user=%s, dest=%s, src=%s' % (str(user), str(dest), str(src)))
            return False
        device = self._query.device.get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to accept, invalid device %s' % str(src))
            return False
        node = device['node']
        device = self._query.device.get(dest)
        if not device:
            log_err(self, 'failed to accept, cannot find target device %s' % str(dest))
            return False
        self._query.link.put(name='', op=OP_ACCEPT, uid=device['uid'], node=device['node'], addr=device['addr'], req={'user':user, 'node':node, 'name':src})
        self._query.guest.put(device['uid'], src)
        return True
    
    def drop(self, uid, dest, src):
        if not dest or not src or dest == src:
            log_err(self, 'failed to drop, dest=%s, src=%s' % (str(dest), str(src)))
            return False
        device = self._query.device.get(src)
        if not device or device.get('uid') != uid:
            log_err(self, 'failed to drop, invalid device %s' % str(src))
            return False
        device = self._query.device.get(dest)
        if not device:
            log_err(self, 'failed to drop, cannot find target device %s' % str(dest))
            return False
        self._query.guest.remove(device['uid'], src)
        return True
