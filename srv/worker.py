# worker.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib import io
from lib import bson
from lib import codec
from conf.virtdev import *
from service.key import Key
from threading import Thread
from service.user import User
from service.node import Node
from conf.log import LOG_WORKER
from service.token import Token
from service.guest import Guest
from conf.defaults import DEBUG
from lib.tasklet import Tasklet
from service.device import Device
from lib.log import log_debug, log_err
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
from SocketServer import BaseRequestHandler
from lib.util import UID_SIZE, unicode2str, create_server

SAFE = True
TASKLET = False
TIMEOUT = 120 # seconds

_services = {}

class RequestHandler(BaseRequestHandler):
    def _do_proc(self, tasklet=False):
        pkt = io.recv_pkt(self.request)
        if pkt:
            reqest = unicode2str(bson.loads(pkt))
            uid = reqest['uid']
            buf = reqest['buf']
            token = reqest['token']
            req = codec.decode(buf, token)
            if not req:
                log_err(self, 'failed to handle, invalid request')
                return
        else:
            log_err(self, 'failed to handle')
            return

        op = req.get('op')
        srv = req.get('srv')
        args = req.get('args')
        if not op or not srv:
            log_err(self, 'failed to handle, invalid arguments')
            return

        args.update({'uid':uid})
        if not _services.has_key(srv):
            log_err(self, 'failed to handle, invalid service %s' % str(srv))
            return

        res = None
        if not tasklet:
            pool = ThreadPool(processes=1)
            result = pool.apply_async(_services[srv].proc, args=(op, args))
            try:
                res = result.get(timeout=TIMEOUT)
            except TimeoutError:
                log_debug(self, 'timeout')
            finally:
                pool.terminate()
        else:
            t = Tasklet(target=_services[srv].proc, args=(op, args), parent=self)
            res = t.wait(TIMEOUT)

        if res == None:
            res = ''

        res = codec.encode(res, token, buf[:UID_SIZE])
        io.send_pkt(self.request, bson.dumps({'result':res}))

    def _proc_safe(self):
        try:
            self._do_proc()
        except:
            log_debug(self, "failed to process")

    def _proc_unsafe(self):
        self._do_proc()

    def _proc_tasklet(self):
        try:
            self._do_proc(tasklet=True)
        except:
            log_debug(self, "failed to process through tasklet")

    def handle(self):
        if TASKLET:
            self._proc_tasklet()
        else:
            if DEBUG and not SAFE:
                self._proc_unsafe()
            else:
                self._proc_safe()

class Worker(object):
    def __init__(self, addr, query):
        self._init_services(query)
        self._addr = addr

    def _log(self, text):
        if LOG_WORKER:
            log_debug(self, text)

    def _add_service(self, srv):
        name = str(srv)
        if name not in _services:
            _services.update({str(srv):srv})

    def _init_services(self, query):
        self._add_service(Key(query))
        self._add_service(User(query))
        self._add_service(Node(query))
        self._add_service(Guest(query))
        self._add_service(Token(query))
        self._add_service(Device(query))

    def start(self):
        self._log('start ...')
        Thread(target=create_server, args=(self._addr, WORKER_PORT, RequestHandler)).start()
