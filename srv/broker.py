# broker.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#
# This work originally started from the example of Paranoid Pirate Pattern,
# which is provided by Daniel Lundin <dln(at)eintr(dot)org>
#

import time
from lib.ppp import *
from lib.util import zmqaddr
from threading import Thread
from conf.log import LOG_BROKER
from conf.meta import BROKER_PORT
from collections import OrderedDict
from conf.virtdev import GATEWAY_PORT
from lib.log import log_debug, log_err
from zmq import Poller, Context, ROUTER, POLLIN

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
    def __init__(self, faddr, baddr):
        Thread.__init__(self)
        self._faddr = faddr
        self._baddr = baddr
        self._init()
    
    def _log(self, text):
        if LOG_BROKER:
            log_debug(self, text)
    
    def _init(self):
        self._context = Context(1)
        self._queue = BrokerQueue()
        self._init_frontend()
        self._init_backend()
        self._init_pollers()
        self._active = True
    
    def _init_frontend(self):
        self._frontend = self._context.socket(ROUTER)
        self._frontend.bind(zmqaddr(self._faddr, GATEWAY_PORT))
    
    def _init_backend(self):
        self._backend = self._context.socket(ROUTER)
        self._backend.bind(zmqaddr(self._baddr, BROKER_PORT))
    
    def _init_pollers(self):
        self._poller1 = Poller()
        self._poller2 = Poller()
        self._poller1.register(self._backend, POLLIN)
        self._poller2.register(self._backend, POLLIN)
        self._poller2.register(self._frontend, POLLIN)
    
    def run(self):
        self._log('start ...')
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
                        log_err(self, "invalid message")
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
