#      event.py
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
