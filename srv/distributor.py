# distributor.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#
# This work originally started from the example of Paranoid Pirate Pattern,
# which is provided by Daniel Lundin <dln(at)eintr(dot)org>
#

import time
import uuid
import random
from lib import io
from lib import bson
from lib.ppp import *
from conf.virtdev import *
from random import randint
from threading import Thread
from hash_ring import HashRing
from conf.defaults import DEBUG
from conf.log import LOG_DISTRIBUTOR
from multiprocessing.pool import ThreadPool
from lib.log import log_debug, log_err, log_get
from zmq import DEALER, POLLIN, LINGER, IDENTITY, Context, Poller
from lib.util import UID_SIZE, USERNAME_SIZE, zmqaddr, unicode2str

SAFE = True
SLEEP_TIME = 10 # seconds

class Distributor(Thread):
    def __init__(self, query):
        Thread.__init__(self)
        self._pool = ThreadPool(processes=4)
        self._workers = HashRing(WORKER_SERVERS)
        self._identity = bytes(uuid.uuid4())
        self._query = query
        self._init_sock()

    def _log(self, text):
        if LOG_DISTRIBUTOR:
            log_debug(self, text)

    def _init_sock(self):
        self._context = Context(1)
        self._poller = Poller()
        self._set_sock()

    def _set_sock(self):
        self._socket = self._context.socket(DEALER)
        self._socket.setsockopt(IDENTITY, self._identity)
        self._poller.register(self._socket, POLLIN)
        self._socket.connect(zmqaddr(self._get_broker(), BROKER_PORT))
        self._socket.send(PPP_READY)

    def _reset_sock(self):
        self._poller.unregister(self._socket)
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()
        self._set_sock()

    def _get_broker(self):
        length = len(BROKER_SERVERS)
        return BROKER_SERVERS[randint(0, length - 1)]

    def _get_worker(self, uid):
        return self._workers.get_node(uid)

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
        self._socket.send_multipart(msg)

    def _request(self, addr, **args):
        sock = io.connect(addr, WORKER_PORT)
        try:
            buf = bson.dumps(args)
            io.send_pkt(sock, buf)
            res = io.recv_pkt(sock)
            return unicode2str(bson.loads(res)['result'])
        finally:
            io.close(sock)

    def _proc(self, identity, seq, buf):
        uid, token = self._get_token(buf)
        if not uid or not token:
            log_err(self, 'failed to process, cannot get token')
            return
        addr = self._get_worker(uid)
        ret = self._request(addr, uid=uid, token=token, buf=buf)
        self._reply(identity, seq, ret)

    def _proc_safe(self, identity, seq, buf):
        try:
            self._proc(identity, seq, buf)
        except:
            log_err(self, 'failed to process')

    def _handler(self, identity, seq, buf):
        if DEBUG and not SAFE:
            self._proc(identity, seq, buf)
        else:
            self._proc_safe(identity, seq, buf)

    def run(self):
        self._log('start ...')
        liveness = PPP_HEARTBEAT_LIVENESS
        timeout = time.time() + PPP_HEARTBEAT_INTERVAL
        while True:
            sockets = dict(self._poller.poll(PPP_HEARTBEAT_INTERVAL * 1000))
            if sockets.get(self._socket) == POLLIN:
                frames = self._socket.recv_multipart()
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
                self._socket.send(PPP_HEARTBEAT)
