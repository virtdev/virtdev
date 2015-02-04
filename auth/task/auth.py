#      auth.py
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
from db.dhcp import VDevDHCP
from task import VDevAuthTask
from lib.log import log_err, log_get

dhcp = VDevDHCP()

class Auth(VDevAuthTask):
    def _gen_token(self):
        return uuid.uuid4().hex
    
    def login(self, uid, node, networks, flags):
        token = self.query.token.get(uid)
        if not token:
            self.query.token.put(uid, self._gen_token())
            token = self.query.token.get(uid)
            if not token:
                log_err(self, 'no token')
                raise Exception(log_get(self, 'no token'))
        addr = dhcp.allocate(uid, node, networks)
        self.query.node.remove(uid, (node,))
        self.query.node.put(uid, (node, addr, str(flags)))
        return {'uid':uid, 'token':token, 'addr':addr}
    
    def logout(self, uid, node, addr):
        self.query.node.remove(uid, (node,))
        self.query.member.remove(uid, ('', node))
        dhcp.free(addr)
        return True
    