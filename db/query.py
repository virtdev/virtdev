#      query.py
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

from history import HistoryDB
from lib.util import tuple2str
from event.event import VDevEvent
from lib.router import VDevRouter
from db import MemberDB, TokenDB, GuestDB, DeviceDB, UserDB, NodeDB
from conf.virtdev import VDEV_DB_SERVERS, VDEV_EVENT_SERVERS, VDEV_DFS_SERVERS

def tuplevalue(func):
    def _tuplevalue(*args, **kwargs):
        if len(args) < 3:
            raise Exception('invalid arguments')
        return func(args[0], args[1], tuple2str(args[2]), *args[3:], **kwargs)
    return _tuplevalue

class VDevDBQueryMember(object):
    def __init__(self, router):
        self._member = MemberDB(router)
        for i in VDEV_DB_SERVERS:
            router.add_server(str(self._member), i)
    
    def get(self, key):
        return self._member.get(key)
    
    @tuplevalue
    def put(self, key, value):
        self._member.put(key, value)
    
    @tuplevalue
    def remove(self, key, value):
        self._member.remove(key, value, regex=True)

class VDevDBQueryNode(object):
    def __init__(self, router):
        self._node = NodeDB(router)
        for i in VDEV_DB_SERVERS:
            router.add_server(str(self._node), i)
    
    def get(self, key):
        return self._node.get(key)
    
    @tuplevalue
    def put(self, key, value):
        self._node.put(key, value)
    
    @tuplevalue
    def remove(self, key, value):
        self._node.remove(key, value, regex=True)

class VDevDBQueryUser(object):
    def __init__(self, router):
        self._user = UserDB(router)
        for i in VDEV_DB_SERVERS:
            router.add_server(str(self._user), i)
    
    def get(self, key, *fields):
        return self._user.find(key, *fields)

class VDevDBQueryToken(object):
    def __init__(self, router):
        self._token = TokenDB(router)
        for i in VDEV_DB_SERVERS:
            router.add_server(str(self._token), i)
    
    def get(self, key):
        return self._token.get(key, first=True)
    
    def put(self, key, value):
        return self._token.put(key, value)
    
    def remove(self, key):
        return self._token.remove(key)

class VDevDBQueryDevice(object):
    def __init__(self, router):
        self._device = DeviceDB(router)
        for i in VDEV_DB_SERVERS:
            router.add_server(str(self._device), i)
    
    def get(self, key):
        return self._device.get(key)
    
    def put(self, key, value):
        self._device.put(key, value)
    
    def remove(self, key):
        self._device.remove(key)

class VDevDBQueryGuest(object):
    def __init__(self, router):
        self._guest = GuestDB(router)
        for i in VDEV_DB_SERVERS:
            router.add_server(str(self._guest), i)
    
    def get(self, key):
        return self._guest.get(key)
    
    def put(self, key, value):
        self._guest.put(key, value)
    
    def remove(self, key, value):
        self._guest.remove(key, value)

class VDevDBQueryHistory(object):
    def __init__(self, router):
        self._history = HistoryDB(router)
        for i in VDEV_DFS_SERVERS:
            router.add_server(str(self._history), i)
    
    def get(self, key, query):
        return self._history.get(key, query)
    
    def put(self, key, **fields):
        self._history.put(key, fields)

class VDevDBQueryEvent(object):
    def __init__(self, router):
        self._event = VDevEvent(router)
        for i in VDEV_EVENT_SERVERS:
            router.add_server('event', i)
    
    def get(self, key):
        return self._event.get(key)
    
    def put(self, key, value):
        return self._event.put(key, value)

class VDevDBQuery(object):
    def __init__(self):
        self.link = None
        router = VDevRouter()
        self.user = VDevDBQueryUser(router)
        self.node = VDevDBQueryNode(router)
        self.token = VDevDBQueryToken(router)
        self.guest = VDevDBQueryGuest(router)
        self.member = VDevDBQueryMember(router)
        self.device = VDevDBQueryDevice(router)
        self.history = VDevDBQueryHistory(router)
