# event.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.util import ifaddr
from conf.virtdev import IFBACK
from emitter import EventEmitter
from monitor import EventMonitor

class Event(object):
    def __init__(self, router):
        addr = ifaddr(ifname=IFBACK)
        self._emitter = EventEmitter(router)
        self._monitor = EventMonitor(addr, router)
        self._monitor.start()
    
    def put(self, uid, name):
        return self._emitter.put(uid, name)
    
    def get(self, uid):
        return self._monitor.get(uid)
