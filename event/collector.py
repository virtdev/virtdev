# collector.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zerorpc
from threading import Thread
from lib.lock import NamedLock
from lib.util import zmqaddr, named_lock
from conf.meta import EVENT_COLLECTOR_PORT, EVENT_MONITOR_PORT

class Collector(object):
    def __init__(self):
        self._queue = {}
        self._events = {}
        self._lock = NamedLock()
    
    def _push(self, uid, addr):
        if self._queue.get(uid) == None:
            self._queue[uid] = []
        if addr not in self._queue[uid]:
            self._queue[uid].append(addr)
    
    def _pop(self, uid):
        if not self._queue.has_key(uid):
            return
        length = len(self._queue[uid])
        if 0 == length:
            del self._queue[uid]
            return
        addr = self._queue[uid].pop(0)
        events = self._events[uid].keys()
        self._reply(uid, addr, events)
        self._events[uid] = {}
    
    def _reply(self, uid, addr, events):
        if not events:
            return
        cli = zerorpc.Client()
        cli.connect(zmqaddr(addr, EVENT_MONITOR_PORT))
        cli.put(uid, events)
    
    @named_lock
    def put(self, uid, name):
        events = self._events.get(uid)
        if events == None:
            self._events[uid] = {}
        self._events[uid].update({name:None})
        self._pop(uid)
    
    @named_lock
    def get(self, uid, addr):
        if not self._events.has_key(uid) or not self._events[uid]:
            self._push(uid, addr)
        else:
            events = self._events[uid].keys()
            self._events[uid] = {}
            if events:
                return events

class EventCollector(Thread):
    def __init__(self, addr):
        Thread.__init__(self)
        self._addr = addr
    
    def run(self):
        srv = zerorpc.Server(Collector())
        srv.bind(zmqaddr(self._addr, EVENT_COLLECTOR_PORT))
        srv.run()
