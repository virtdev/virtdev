#      request.py
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

import zmq
import uuid
import crypto
from log import log_err
from util import zmqaddr
from random import randint
from threading import Lock
from conf.virtdev import VDEV_SERVERS, VDEV_PORT
from zmq import REQ, IDENTITY, POLLIN, SNDMORE, LINGER

TIMEOUT = 30 # seconds
RETRY_MAX = 3

class Packet(object):
    def _get_addr(self):
        length = len(VDEV_SERVERS)
        n = randint(0, length - 1)
        return zmqaddr(VDEV_SERVERS[n], VDEV_PORT)
    
    def _set_sock(self):
        self._sock = self._context.socket(REQ)
        self._sock.setsockopt(IDENTITY, self._id)
        self._sock.connect(self._get_addr())
        self._poller.register(self._sock, POLLIN)
    
    def _close_sock(self):
        self._poller.unregister(self._sock)
        self._sock.setsockopt(LINGER, 0)
        self._sock.close()
    
    def _reset_sock(self):
        self._close_sock()
        self._set_sock()
    
    def __init__(self):
        self._seq = 0
        self._id = bytes(uuid.uuid4())
        self._context = zmq.Context(1)
        self._poller = zmq.Poller()
        self._lock = Lock()
        self._set_sock()
    
    def _send(self, buf):
        self._sock.send(str(self._seq), SNDMORE)
        self._sock.send(str(buf))
    
    def send(self, buf):
        cnt = 0
        res = None
        self._lock.acquire()
        try:
            self._seq += 1
            while cnt < RETRY_MAX:
                cnt += 1
                self._send(buf)
                while True:
                    socks = dict(self._poller.poll(TIMEOUT * 1000))
                    if socks.get(self._sock):
                        reply = self._sock.recv_multipart()
                        if len(reply) != 2:
                            break
                        if int(reply[0]) == self._seq:
                            cnt = RETRY_MAX
                            res = reply[1]
                            break
                    else:
                        if cnt < RETRY_MAX:
                            self._reset_sock()
                        break
        finally:
            self._lock.release()
            return res
    
    def __del__(self):
        self._close_sock()
        self._context.term()

class Client(object):
    def __init__(self, task, uid, token, timeout):
        self._uid = uid
        self._task = task
        self._token = token
        self._timeout = timeout
    
    def __getattr__(self, op):
        self._op = op
        return self._request
    
    def _request(self, **kwargs):
        ret = None
        try:
            cmd = {'task':self._task, 'op':self._op, 'args':kwargs, 'timeout':self._timeout}
            buf = crypto.pack(self._uid, cmd, self._token)
            result = Packet().send(buf)
            if result:
                ret = crypto.unpack(self._uid, result, self._token)
            return ret
        except:
            log_err(self, "failed to request")

class Request(object):
    def __init__(self, uid, token, timeout=None):
        self._uid = uid
        self._token = token
        self._timeout = timeout
    
    def __getattr__(self, task):
        return Client(task, self._uid, self._token, self._timeout)
    