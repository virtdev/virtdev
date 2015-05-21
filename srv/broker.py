#      broker.py
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
from lib import util
from lib.ppp import *
from lib.log import log_err
from threading import Thread
from collections import OrderedDict
from zmq import Poller, Context, ROUTER, POLLIN
from conf.virtdev import VDEV_PORT, BROKER_PORT, IFBACK

class BrokerItem(object):
    def __init__(self, identity):
        self._identity = identity
        self._time = time.time() + PPP_HEARTBEAT_INTERVAL * PPP_HEARTBEAT_LIVENESS
    
    @property
    def identity(self):
        return self._identity
    
    @property
    def time(self):
        return self._time

class BrokerQueue(object):
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

class Broker(Thread):
    def _init_frontend(self):
        addr = util.ifaddr()
        self._frontend = self._context.socket(ROUTER)
        self._frontend.bind(util.zmqaddr(addr, VDEV_PORT))
    
    def _init_backend(self):
        addr = util.ifaddr(IFBACK)
        self._backend = self._context.socket(ROUTER)
        self._backend.bind(util.zmqaddr(addr, BROKER_PORT))
    
    def _init_pollers(self):
        self._poller1 = Poller()
        self._poller2 = Poller()
        self._poller1.register(self._backend, POLLIN)
        self._poller2.register(self._backend, POLLIN)
        self._poller2.register(self._frontend, POLLIN)
        
    def _init_context(self):
        self._context = Context(1)
        self._init_frontend()
        self._init_backend()
        self._init_pollers()
    
    def __init__(self):
        Thread.__init__(self)
        self._queue = BrokerQueue()
        self._init_context()
        self._active = True
    
    def run(self):
        timeout = time.time() + PPP_HEARTBEAT_INTERVAL
        while self._active:
            if len(self._queue.queue) > 0:
                poller = self._poller2
            else:
                poller = self._poller1
            socks = dict(poller.poll(PPP_HEARTBEAT_INTERVAL * 1000))
            if socks.get(self._backend) == POLLIN:
                frames = self._backend.recv_multipart()
                if not frames:
                    break
                identity = frames[0]
                self._queue.add(BrokerItem(identity))
                msg = frames[1:]
                if len(msg) == 1:
                    if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
                        log_err(self, "invalid message, %s" % msg)
                else:
                    self._frontend.send_multipart(msg)
                
                if time.time() >= timeout:
                    for identity in self._queue.queue:
                        msg = [identity, PPP_HEARTBEAT]
                        self._backend.send_multipart(msg)
                    timeout = time.time() + PPP_HEARTBEAT_INTERVAL
            
            if socks.get(self._frontend) == POLLIN:
                frames = self._frontend.recv_multipart()
                if not frames:
                    break
                frames.insert(0, self._queue.pop())
                self._backend.send_multipart(frames)
            
            self._queue.purge()
    
    def stop(self):
        self._active = False
    
