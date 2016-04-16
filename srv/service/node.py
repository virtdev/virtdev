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

from service import Service
from lib.util import str2tuple
from lib.modes import MODE_VISI

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
