#      collector.py
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

import zerorpc
from threading import Thread
from lib.lock import NamedLock
from lib.util import zmqaddr, ifaddr, named_lock
from conf.virtdev import EVENT_COLLECTOR_PORT, EVENT_RECEIVER_PORT

class Collector(object):
    def __init__(self, collector):
        self._collector = collector
    
    def put(self, uid, name):
        return self._collector.put(uid, name)
    
    def get(self, uid, addr):
        return self._collector.get(uid, addr)

class EventCollector(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._queue = {}
        self._events = {}
        self._lock = NamedLock()
        self._collector = Collector(self)
        self.start()
    
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
        cli.connect(zmqaddr(addr, EVENT_RECEIVER_PORT))
        cli.put(uid, events)
    
    @named_lock
    def put(self, uid, name):
        events = self._events.get(uid)
        if None == events:
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
    
    def run(self):
        srv = zerorpc.Server(self._collector)
        srv.bind(zmqaddr(ifaddr(), EVENT_COLLECTOR_PORT))
        srv.run()
    