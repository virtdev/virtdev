# request.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zmq
import uuid
from lib import codec
from random import randint
from threading import Lock
from lib.log import log_err
from lib.util import zmqaddr
from zmq import REQ, IDENTITY, POLLIN, SNDMORE, LINGER
from conf.virtdev import GATEWAY_SERVERS, GATEWAY_PORT

TIMEOUT = 60  # sec
RETRY_MAX = 2

class RequestSender(object):
    def _get_addr(self):
        length = len(GATEWAY_SERVERS)
        n = randint(0, length - 1)
        return zmqaddr(GATEWAY_SERVERS[n], GATEWAY_PORT)

    def _set(self):
        self._socket = self._context.socket(REQ)
        self._socket.setsockopt(IDENTITY, self._id)
        self._socket.connect(self._get_addr())
        self._poller.register(self._socket, POLLIN)

    def _close(self):
        self._poller.unregister(self._socket)
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()

    def _reset(self):
        self._close()
        self._set()

    def __init__(self):
        self._seq = 0
        self._id = bytes(uuid.uuid4())
        self._context = zmq.Context(1)
        self._poller = zmq.Poller()
        self._lock = Lock()
        self._set()

    def _send(self, buf):
        self._socket.send(str(self._seq), SNDMORE)
        self._socket.send(str(buf))

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
                    sockets = dict(self._poller.poll(TIMEOUT * 1000))
                    if sockets.get(self._socket):
                        reply = self._socket.recv_multipart()
                        if len(reply) != 2:
                            break
                        if int(reply[0]) == self._seq:
                            cnt = RETRY_MAX
                            res = reply[1]
                            break
                    else:
                        if cnt < RETRY_MAX:
                            self._reset()
                        break
        finally:
            self._lock.release()
            return res

    def __del__(self):
        self._close()
        self._context.term()

class RequestHandler(object):
    def __init__(self, srv, uid, token):
        self._srv = srv
        self._uid = uid
        self._token = token

    def __getattr__(self, op):
        self._op = op
        return self._request

    def _request(self, **kwargs):
        ret = None
        try:
            cmd = {'srv':self._srv, 'op':self._op, 'args':kwargs}
            buf = codec.encode(cmd, self._token, self._uid)
            result = RequestSender().send(buf)
            if result:
                ret = codec.decode(result, self._token, self._uid)
            return ret
        except:
            log_err(self, "failed to request")

class Request(object):
    def __init__(self, uid, token):
        self._uid = uid
        self._token = token

    def __getattr__(self, srv):
        return RequestHandler(srv, self._uid, self._token)
