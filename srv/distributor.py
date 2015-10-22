#      distributor.py
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
import uuid
import random
import zerorpc
from lib.ppp import *
from random import randint
from threading import Thread
from lib.log import log_err, log_get
from multiprocessing.pool import ThreadPool
from lib.util import UID_SIZE, USERNAME_SIZE, zmqaddr, hash_name
from zmq import DEALER, POLLIN, LINGER, IDENTITY, Context, Poller
from conf.virtdev import BROKER_SERVERS, BROKER_PORT, PROCESSOR_SERVERS, PROCESSOR_PORT

POOL_SIZE = 64
SLEEP_TIME = 10 # seconds

class Distributor(Thread):        
    def __init__(self, query):
        Thread.__init__(self)
        self._pool = ThreadPool(processes=POOL_SIZE)
        self._identity = bytes(uuid.uuid4())
        self._init_services(query)
        self._init_sock()
    
    def _init_sock(self):
        self._context = Context(1)
        self._poller = Poller()
        self._set_sock()
    
    def _set_sock(self):
        self._sock = self._context.socket(DEALER)
        self._sock.setsockopt(IDENTITY, self._identity)
        self._poller.register(self._sock, POLLIN)
        self._sock.connect(zmqaddr(self._get_broker(), BROKER_PORT))
        self._sock.send(PPP_READY)
    
    def _reset_sock(self):
        self._poller.unregister(self._sock)
        self._sock.setsockopt(LINGER, 0)
        self._sock.close()
        self._set_sock()
    
    def _get_broker(self):
        length = len(BROKER_SERVERS)
        return BROKER_SERVERS[randint(0, length - 1)]
    
    def _get_processor(self, uid):
        length = len(PROCESSOR_SERVERS)
        return PROCESSOR_SERVERS[hash_name(uid) % length]
    
    def _get_user(self, buf):
        if len(buf) < USERNAME_SIZE:
            log_err(self, 'failed to get user, invalid length')
            raise Exception(log_get(self, 'failed to get user'))
        name = filter(lambda x:x != '*', buf[:USERNAME_SIZE])
        if not name:
            log_err(self, 'failed to get user')
            raise Exception(log_get(self, 'failed to get user'))
        return name
    
    def _get_token(self, buf):
        if len(buf) < UID_SIZE:
            log_err(self, 'failed to get token, invalid length')
            raise Exception(log_get(self, 'failed to get token'))
        uid = None
        token = None
        if buf[UID_SIZE - 1] == '*':
            user = self._get_user(buf)
            uid, token = self._query.user.get(user, 'uid', 'password')
        else:
            uid = buf[0:UID_SIZE]
            token = self._query.token.get(uid)
        if uid and token:
            return (uid, token)
        else:
            log_err(self, 'failed to get token')
            raise Exception(log_get(self, 'failed to get token'))
    
    def _reply(self, identity, seq, buf):
        msg = [identity, '', seq, buf]
        self._sock.send_multipart(msg)
    
    def _proc(self, identity, seq, buf):
        try:
            uid, token = self._get_token(buf)
            if not uid or not token:
                log_err(self, 'failed to process, invalid token')
                return
            c = zerorpc.Client()
            addr = self._get_processor(uid)
            c.connect(zmqaddr(addr, PROCESSOR_PORT))
            ret = c.proc(uid, token, buf)
            self._reply(identity, seq, ret)
        except:
            log_err(self, 'failed to process')
    
    def run(self):
        liveness = PPP_HEARTBEAT_LIVENESS
        timeout = time.time() + PPP_HEARTBEAT_INTERVAL
        while True:
            socks = dict(self._poller.poll(PPP_HEARTBEAT_INTERVAL * 1000))
            if socks.get(self._sock) == POLLIN:
                frames = self._sock.recv_multipart()
                if not frames:
                    log_err(self, 'invalid request')
                    break
                if len(frames) == PPP_NR_FRAMES:
                    self._pool.apply_async(self._proc, args=(frames[PPP_FRAME_ID], frames[PPP_FRAME_SEQ], frames[PPP_FRAME_BUF]))
                elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                    liveness = PPP_HEARTBEAT_LIVENESS
                else:
                    log_err(self, "invalid request")
            else:
                liveness -= 1
                if liveness == 0:
                    time.sleep(random.randint(SLEEP_TIME / 2, SLEEP_TIME))
                    self._reset_sock()
                    liveness = PPP_HEARTBEAT_LIVENESS
            
            if time.time() > timeout:
                timeout = time.time() + PPP_HEARTBEAT_INTERVAL
                self._sock.send(PPP_HEARTBEAT)
