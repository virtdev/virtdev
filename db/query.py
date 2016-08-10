# query.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from user import User
from history import History
from event.event import Event
from lib.util import tuple2str
from database import Member, Token, Guest, Device, Node, Key

def tuplevalue(func):
    def _tuplevalue(*args, **kwargs):
        if len(args) < 3:
            raise Exception('Error: invalid tuple')
        return func(args[0], args[1], tuple2str(args[2]), *args[3:], **kwargs)
    return _tuplevalue

class MemberQuery(object):
    def __init__(self, router):
        self._member = Member(router)
    
    def get(self, key):
        return self._member.get(key)
    
    @tuplevalue
    def put(self, key, value):
        self._member.put(key, value)
    
    @tuplevalue
    def delete(self, key, value):
        self._member.delete(key, value, regex=True)

class NodeQuery(object):
    def __init__(self, router):
        self._node = Node(router)
    
    def get(self, key):
        return self._node.get(key)
    
    @tuplevalue
    def put(self, key, value):
        self._node.put(key, value)
    
    @tuplevalue
    def delete(self, key, value):
        self._node.delete(key, value, regex=True)

class UserQuery(object):
    def __init__(self, router):
        self._user = User(router)
    
    def get(self, key, *fields):
        return self._user.get(key, *fields)

class TokenQuery(object):
    def __init__(self, router):
        self._token = Token(router)
    
    def get(self, key):
        return self._token.get(key, first=True)
    
    def put(self, key, value):
        return self._token.put(key, value)
    
    def delete(self, key):
        return self._token.delete(key)

class DeviceQuery(object):
    def __init__(self, router):
        self._device = Device(router)
    
    def get(self, key):
        return self._device.get(key)
    
    def put(self, key, value):
        self._device.put(key, value)
    
    def delete(self, key):
        self._device.delete(key)

class GuestQuery(object):
    def __init__(self, router):
        self._guest = Guest(router)
    
    def get(self, key):
        return self._guest.get(key)
    
    def put(self, key, value):
        self._guest.put(key, value)
    
    def delete(self, key, value):
        self._guest.delete(key, value)

class HistoryQuery(object):
    def __init__(self, router):
        self._history = History(router)
    
    def get(self, uid, key):
        return self._history.get(uid, key)
    
    def put(self, uid, key, **fields):
        self._history.put(uid, key, fields)

class EventQuery(object):
    def __init__(self, router):
        self._event = Event(router)
    
    def get(self, key):
        return self._event.get(key)
    
    def put(self, key, value):
        return self._event.put(key, value)

class KeyQuery(object):
    def __init__(self, router):
        self._key = Key(router)
    
    def get(self, key):
        return self._key.get(key)
    
    def put(self, key, value):
        self._key.put(key, value)
    
    def delete(self, key):
        self._key.delete(key)

class Query(object):
    def __init__(self, meta, data):
        self.link = None
        self.key = KeyQuery(meta)
        self.user = UserQuery(meta)
        self.node = NodeQuery(meta)
        self.event = EventQuery(data)
        self.token = TokenQuery(meta)
        self.guest = GuestQuery(meta)
        self.member = MemberQuery(meta)
        self.device = DeviceQuery(meta)
        self.history = HistoryQuery(data)
