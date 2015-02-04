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
from task import VDevAuthTask
from lib.util import VDEV_FLAG_SPECIAL, str2tuple

NODE_MAX = 256

class Node(VDevAuthTask):
    def _get_uid(self, user):
        return self.query.user_get({'user':user}, 'uid')
    
    def find(self, uid, user, node):
        uid = self._get_uid(user)
        if not uid:
            return
        nodes = self.query.node_get(uid)
        for item in nodes:
            n, _, flags = str2tuple(item)
            if flags and int(flags) & VDEV_FLAG_SPECIAL and n == node:
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
        nodes = self.query.node_get(uid)
        for item in nodes:
            node, _, flags = str2tuple(item)
            if flags and int(flags) & VDEV_FLAG_SPECIAL:
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
    