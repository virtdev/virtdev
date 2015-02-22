#      router.py
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

from lib.util import lock
from threading import Lock

class VDevRouter(object):
    def __init__(self):
        self._servers = {}
        self._lock = Lock()
    
    @lock
    def add_server(self, namespace, key, val=None, compare=None):
        if self._servers.has_key(namespace):
            if compare:
                cnt = 0
                for i in self._servers[namespace]:
                    if compare(i[1], val) > 0:
                        self._servers.insert(cnt, (key, val))
                        return
                    else:
                        cnt += 1
            self._servers[namespace].append((key, val))
        else:
            self._servers.update({namespace:[(key, val)]})
    
    def _map(self, namespace, identity, path):
        return abs(hash(identity + path)) % len(self._servers[namespace])
    
    def get(self, namespace, identity, path=''):
        n = self._map(namespace, identity, path)
        return self._servers[namespace][n][0]
    