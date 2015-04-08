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
from ppp import *
from lib import util
from lib.log import log_err
from threading import Thread
from queue import VDevAuthItem
from queue import VDevAuthQueue
from zmq import Poller, Context, ROUTER, POLLIN
from conf.virtdev import AUTH_PORT, BROKER_PORT, IFBACK

class VDevAuthBroker(Thread):
    def _init_frontend(self):
        addr = util.ifaddr()
        self._frontend = self._context.socket(ROUTER)
        self._frontend.bind(util.zmqaddr(addr, AUTH_PORT))
    
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
        self._queue = VDevAuthQueue()
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
                self._queue.add(VDevAuthItem(identity))
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
    
