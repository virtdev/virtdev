#      queue.py
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
#
#      This work originally started from the example of Paranoid Pirate Pattern,
#      which is provided by Daniel Lundin <dln(at)eintr(dot)org>

import time
from ppp import *
from collections import OrderedDict

class VDevAuthItem(object):
    def __init__(self, identity):
        self._identity = identity
        self._time = time.time() + PPP_HEARTBEAT_INTERVAL * PPP_HEARTBEAT_LIVENESS
    
    @property
    def identity(self):
        return self._identity
    
    @property
    def time(self):
        return self._time

class VDevAuthQueue(object):
    def __init__(self):
        self._queue = OrderedDict()
    
    def add(self, item):
        self._queue.pop(item.identity, None)
        self._queue[item.identity] = item
    
    def pop(self):
        return self._queue.popitem(False)[0]
    
    def purge(self):
        t = time.time()
        expired = []
        for identity, item in self._queue.iteritems():
            if t < item.time:
                break
            expired.append(identity)
        for identity in expired:
            self._queue.pop(identity, None)
    
    @property
    def queue(self):
        return self._queue
