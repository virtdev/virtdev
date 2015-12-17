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

from lib import resolv
from service import Service
from db.marker import Marker
from lib.log import log_err, log_get
from conf.virtdev import EXTEND, AREA_CODE, PROTOCOL
from lib.protocols import PROTOCOL_N2N, PROTOCOL_WRTC
from lib.util import CLS_DEVICE, get_name, update_device, gen_key, gen_token

class User(Service):
    def __init__(self, query):
        Service.__init__(self, query)
        if EXTEND:
            self._marker = Marker()
    
    def _check_node(self, uid, node, host, mode, key):
        name = get_name(uid, node)
        if self._query.key.get(name):
            self._query.node.remove(uid, (node,))
        else:
            if EXTEND:
                self._marker.mark(name, CLS_DEVICE, AREA_CODE)
        self._query.key.put(name, key)
        self._query.node.put(uid, (node, host, str(mode)))
        update_device(self._query, uid, node, host, name)
    
    def login(self, uid, node, networks, mode):
        key = gen_key()
        token = self._query.token.get(uid)
        if not token:
            self._query.token.put(uid, gen_token())
            token = self._query.token.get(uid)
            if not token:
                log_err(self, 'no token')
                raise Exception(log_get(self, 'no token'))
        host = resolv.get_addr(uid, node, networks, protocol=PROTOCOL_N2N)
        if not host:
            log_err(self, 'invalid host address')
            raise Exception(log_get(self, 'invalid host address'))
        self._check_node(uid, node, host, mode, key)
        if PROTOCOL == PROTOCOL_WRTC:
            addr = resolv.get_addr(uid, node, networks, protocol=PROTOCOL_WRTC)
            if not addr:
                log_err(self, 'invalid address')
                raise Exception(log_get(self, 'invalid address'))
        else:
            addr = host
        return {'uid':uid, 'host':host, 'addr':addr, 'token':token, 'key':key}
    
    def logout(self, uid, node, addr):
        self._query.node.remove(uid, (node,))
        self._query.member.remove(uid, ('', node))
        return True
