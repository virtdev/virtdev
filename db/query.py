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
from lib.router import Router
from lib.util import tuple2str
from event.event import DeviceEvent
from conf.virtdev import META_SERVERS, EVENT_SERVERS, DATA_SERVERS
from db import MemberDB, TokenDB, GuestDB, DeviceDB, UserDB, NodeDB, KeyDB

def tuplevalue(func):
    def _tuplevalue(*args, **kwargs):
        if len(args) < 3:
            raise Exception('invalid arguments')
        return func(args[0], args[1], tuple2str(args[2]), *args[3:], **kwargs)
    return _tuplevalue

class MemberQuery(object):
    def __init__(self, router):
        self._member = MemberDB(router)
        for i in META_SERVERS:
            router.add_server(str(self._member), i)
    
    def get(self, key):
        return self._member.get(key)
    
    @tuplevalue
    def put(self, key, value):
        self._member.put(key, value)
    
    @tuplevalue
    def remove(self, key, value):
        self._member.remove(key, value, regex=True)

class NodeQuery(object):
    def __init__(self, router):
        self._node = NodeDB(router)
        for i in META_SERVERS:
            router.add_server(str(self._node), i)
    
    def get(self, key):
        return self._node.get(key)
    
    @tuplevalue
    def put(self, key, value):
        self._node.put(key, value)
    
    @tuplevalue
    def remove(self, key, value):
        self._node.remove(key, value, regex=True)

class UserQuery(object):
    def __init__(self, router):
        self._user = UserDB(router)
        for i in META_SERVERS:
            router.add_server(str(self._user), i)
    
    def get(self, key, *fields):
        return self._user.find(key, *fields)

class TokenQuery(object):
    def __init__(self, router):
        self._token = TokenDB(router)
        for i in META_SERVERS:
            router.add_server(str(self._token), i)
    
    def get(self, key):
        return self._token.get(key, first=True)
    
    def put(self, key, value):
        return self._token.put(key, value)
    
    def remove(self, key):
        return self._token.remove(key)

class DeviceQuery(object):
    def __init__(self, router):
        self._device = DeviceDB(router)
        for i in META_SERVERS:
            router.add_server(str(self._device), i)
    
    def get(self, key):
        return self._device.get(key)
    
    def put(self, key, value):
        self._device.put(key, value)
    
    def remove(self, key):
        self._device.remove(key)

class GuestQuery(object):
    def __init__(self, router):
        self._guest = GuestDB(router)
        for i in META_SERVERS:
            router.add_server(str(self._guest), i)
    
    def get(self, key):
        return self._guest.get(key)
    
    def put(self, key, value):
        self._guest.put(key, value)
    
    def remove(self, key, value):
        self._guest.remove(key, value)

class HistoryQuery(object):
    def __init__(self, router):
        self._history = HistoryDB(router)
        for i in DATA_SERVERS:
            router.add_server(str(self._history), i)
    
    def get(self, key, query):
        return self._history.get(key, query)
    
    def put(self, key, **fields):
        self._history.put(key, fields)

class EventQuery(object):
    def __init__(self, router):
        self._event = DeviceEvent(router)
        for i in EVENT_SERVERS:
            router.add_server('event', i)
    
    def get(self, key):
        return self._event.get(key)
    
    def put(self, key, value):
        return self._event.put(key, value)

class KeyQuery(object):
    def __init__(self, router):
        self._key = KeyDB(router)
        for i in META_SERVERS:
            router.add_server(str(self._key), i)
    
    def get(self, key):
        return self._key.get(key)
    
    def put(self, key, value):
        self._key.put(key, value)
    
    def remove(self, key):
        self._key.remove(key)

class Query(object):
    def __init__(self):
        self.link = None
        router = Router()
        self.router = router
        self.key = KeyQuery(router)
        self.user = UserQuery(router)
        self.node = NodeQuery(router)
        self.event = EventQuery(router)
        self.token = TokenQuery(router)
        self.guest = GuestQuery(router)
        self.member = MemberQuery(router)
        self.device = DeviceQuery(router)
        self.history = HistoryQuery(router)
