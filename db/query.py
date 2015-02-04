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

from lib import util
from history import HistoryDB
from event.event import VDevEvent
from lib.router import VDevRouter
from db import MemberDB, TokenDB, GuestDB, DeviceDB, UserDB, NodeDB
from conf.virtdev import VDEV_DB_SERVERS, VDEV_EVENT_SERVERS, VDEV_DFS_SERVERS

def tuplevalue(func):
    def _tuplevalue(*args, **kwargs):
        if len(args) < 3:
            raise Exception('invalid arguments')
        return func(args[0], args[1], util.tuple2str(args[2]), *args[3:], **kwargs)
    return _tuplevalue

class VDevDBQuery(object):
    def _init_db(self, router):
        self._node = NodeDB(router)
        self._user = UserDB(router)
        self._token = TokenDB(router)
        self._guest = GuestDB(router)
        self._event = VDevEvent(router)
        self._device = DeviceDB(router)
        self._member = MemberDB(router)
        self._history = HistoryDB(router)
    
    def _init_router(self, router):
        for i in VDEV_EVENT_SERVERS:
            router.add_server('event', i)
        
        for i in VDEV_DB_SERVERS:
            router.add_server(str(self._node), i)
            router.add_server(str(self._user), i)
            router.add_server(str(self._token), i)
            router.add_server(str(self._guest), i)
            router.add_server(str(self._device), i)
            router.add_server(str(self._member), i)
        
        for i in VDEV_DFS_SERVERS:
            router.add_server(str(self._history), i)
        
        self.router = router
    
    def __init__(self):
        self.link = None 
        router = VDevRouter()
        self._init_db(router)
        self._init_router(router)
    
    def set_link(self, link):
        self.link = link
    
    def user_get(self, key, *fields):
        return self._user.find(key, *fields)
    
    def member_get(self, key):
        return self._member.get(key)
    
    @tuplevalue
    def member_put(self, key, value):
        self._member.put(key, value)
    
    @tuplevalue
    def member_remove(self, key, value):
        self._member.remove(key, value, regex=True)
    
    def node_get(self, key):
        return self._node.get(key)
    
    @tuplevalue
    def node_put(self, key, value):
        self._node.put(key, value)
    
    @tuplevalue
    def node_remove(self, key, value):
        self._node.remove(key, value, regex=True)
    
    def token_get(self, key):
        return self._token.get(key, first=True)
    
    def token_put(self, key, value):
        return self._token.put(key, value)
    
    def token_remove(self, key):
        return self._token.remove(key)
    
    def guest_get(self, key):
        return self._guest.get(key)
    
    def guest_put(self, key, value):
        self._guest.put(key, value)
    
    def guest_remove(self, key, value):
        self._guest.remove(key, value)
    
    def device_get(self, key):
        return self._device.get(key)
    
    def device_put(self, key, value):
        self._device.put(key, value)
    
    def device_remove(self, key):
        self._device.remove(key)
    
    def history_put(self, key, **fields):
        self._history.put(key, fields)
    
    def history_get(self, key, query):
        return self._history.get(key, query)
    
    def event_get(self, key):
        return self._event.get(key)
    
    def event_put(self, key, value):
        return self._event.put(key, value)
    