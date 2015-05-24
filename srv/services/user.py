#      user.py
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

import uuid
from lib.dhcp import DHCP
from srv.service import Service
from lib.log import log_err, log_get

dhcp = DHCP()

class User(Service):
    def _gen_uuid(self):
        return uuid.uuid4().hex
    
    def login(self, uid, node, networks, mode):
        key = self._gen_uuid()
        token = self._query.token.get(uid)
        if not token:
            self._query.token.put(uid, self._gen_uuid())
            token = self._query.token.get(uid)
            if not token:
                log_err(self, 'no token')
                raise Exception(log_get(self, 'no token'))
        addr = dhcp.allocate(uid, node, networks)
        self._query.node.remove(uid, (node,))
        self._query.node.put(uid, (node, addr, str(mode)))
        self._query.key.put(uid + node, key)
        return {'uid':uid, 'addr':addr, 'token':token, 'key':key}
    
    def logout(self, uid, node, addr):
        self._query.node.remove(uid, (node,))
        self._query.member.remove(uid, ('', node))
        dhcp.free(addr)
        return True