#      node.py
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

from random import randint
from lib.util import str2tuple
from lib.mode import MODE_VISI
from srv.service import Service

NODE_MAX = 256

class Node(Service):
    def _get_uid(self, user):
        return self._query.user.get(user, 'uid')
    
    def find(self, uid, user, node):
        uid = self._get_uid(user)
        if not uid:
            return
        nodes = self._query.node.get(uid)
        for item in nodes:
            n, _, mode = str2tuple(item)
            if mode and int(mode) & MODE_VISI and n == node:
                return {'uid':uid}
    
    def search(self, uid, user, random, limit):
        res = []
        uid = self._get_uid(user)
        if not uid:
            return
        if random:
            limit = NODE_MAX
        elif limit > NODE_MAX:
            limit = NODE_MAX
        node_list = []
        nodes = self._query.node.get(uid)
        for item in nodes:
            node, _, mode = str2tuple(item)
            if mode and int(mode) & MODE_VISI:
                node_list.append(node)
                if len(node_list) >= limit:
                    break
        if node_list:
            if random:
                res.append(node_list[randint(0, len(node_list) - 1)])
            else:
                res = node_list
        if res:
            return {'node':res, 'uid':uid}
