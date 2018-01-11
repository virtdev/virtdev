# conductor.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.pool import Pool
from threading import Event
from lib.queue import Queue
from lib.operations import *
from lib import channel, codec
from lib.request import Request
from operation import Operation
from conf.defaults import DEBUG
from conf.log import LOG_CONDUCTOR
from lib.ws import WSHandler, ws_start
from conf.defaults import CONDUCTOR_PORT
from lib.log import log_debug, log_err, log_get
from lib.signaling import SignalingClient, reply
from lib.util import UID_SIZE, get_name, close_port

SAFE = True
ASYNC = True
TIMEOUT = 1200 # seconds
QUEUE_LEN = 1
POOL_SIZE = 32

def chkstat(func):
    def _chkstat(*args, **kwargs):
        self = args[0]
        if self._ready:
            return func(*args, **kwargs)
    return _chkstat

class ConductorQueue(Queue):
    def __init__(self, parent):
        Queue.__init__(self, parent, QUEUE_LEN, TIMEOUT)
        self._parent = parent

    def proc(self, buf):
        self._parent.proc(buf)

class ConductorPool(Pool):
    def __init__(self):
        Pool.__init__(self)
        self._ready = False

    def initialize(self, manager):
        key = manager.key
        uid = manager.uid
        addr = manager.addr
        token = manager.token

        if not key or not uid or not addr or not token:
            log_err(self, 'faild to initialize')
            raise Exception(log_get(self, 'failed to initialize'))

        self._keys = {}
        self._tokens = {}
        self._devices = {}
        self._event = Event()
        self._op = Operation(manager)

        if ASYNC:
            for _ in range(POOL_SIZE):
                self.add(ConductorQueue(self))

        self.uid = uid
        self.addr = addr
        self.token = token
        self.user = manager.user
        self.devices = manager.devices
        self.request = Request(uid, token)

        channel.create(uid, addr, key)
        self._ready = True

    def _log(self, text):
        if LOG_CONDUCTOR:
            log_debug(self, text)

    @chkstat
    def get_token(self, head, update=False):
        if head == self.uid:
            return self.token

        token = None
        if not update:
            token = self._tokens.get(head)

        if not token:
            token = self.request.token.get(name=head)
            if token:
                self._tokens.update({head:token})
            else:
                log_err(self, 'failed to get token, head=%s' % str(head))
                raise Exception(log_get(self, 'failed to get token'))

        return token

    @chkstat
    def get_device(self, name):
        res = self._devices.get(name)
        if not res:
            res = self.request.device.find(name=name)
            if res:
                res = (res['uid'], res['node'], res['addr'])
                self._devices.update({name:res})
        if not res:
            log_err(self, 'failed to get device, name=%s' % name)
            raise Exception(log_get(self, 'failed to get device, name=%s' % name))
        return res

    @chkstat
    def remove_device(self, name):
        if self._devices.has_key(name):
            del self._devices[name]

    @chkstat
    def get_key(self, uid, node):
        name = get_name(uid, node)
        key = self._keys.get(name)
        if not key:
            key = self.request.key.get(name=name)
            if key:
                self._keys.update({name:key})
        if not key:
            log_err(self, 'failed to get key')
            raise Exception(log_get(self, 'failed to get key'))
        return key

    @chkstat
    def remove_key(self, uid, node):
        name = uid + node
        if self._keys.get(name):
            del self._keys[name]

    def parse(self, buf):
        if len(buf) <= UID_SIZE:
            return

        head = buf[0:UID_SIZE]
        token = self.get_token(head)
        if not token:
            log_err(self, 'failed to handle, no token')
            return

        try:
            req = codec.decode(buf, token)
        except:
            token = self.get_token(head, update=True)
            if not token:
                log_err(self, 'failed to handle, no token')
                return
            req = codec.decode(buf, token)

        if req:
            op = req.get('op')
            if not op or op[0] == '_' or type(req.get('args')) != dict:
                log_err(self, 'failed to handle, invalid request')
                return
            return req

    def _proc(self, req):
        op = req.get('op')
        args = req.get('args')
        func = getattr(self._op, op)
        if func:
            self._log('op=%s, args=%s' % (str(op), str(args)))
            if args:
                func(**args)
            else:
                func()
        else:
            log_err(self, 'failed to process, op=%s' % str(op))

    def _proc_safe(self, req):
        try:
            self._proc(req)
        except:
            log_err(self, 'failed to process')

    def proc(self, req):
        if DEBUG and not SAFE:
            self._proc(req)
        else:
            self._proc_safe(req)

    def _is_async(self, req):
        if ASYNC:
            op = req.get('op')
            if op == OP_PUT or op == OP_GET:
                return True
        return False

    def put(self, req):
        if self._is_async(req):
            self.push(req)
        else:
            self.proc(req)

    def _handle(self, req):
        op = req.get('op')
        if op == OP_EXIST:
            args = req.get('args')
            if args.get('dest') == self.addr:
                addr = args.get('src')
                broker = args.get('broker')
                if addr and broker:
                    reply(addr, broker)
                    return True
        return False

    def handle(self, buf):
        req = self.parse(buf)
        if req:
            if not self._handle(req):
                self.proc(req)

conductor = ConductorPool()

class ConductorHandler(WSHandler):
    def _do_proc(self, buf):
        req = conductor.parse(buf)
        if req:
            conductor.put(req)

    def _proc_safe(self, buf):
        try:
            self._do_proc(buf)
        except:
            log_debug(self, 'failed')

    def _proc_unsafe(self, buf):
        self._do_proc(buf)

    def _proc(self, buf):
        if SAFE:
            self._proc_safe(buf)
        else:
            self._proc_unsafe(buf)

    def on_message(self, buf):
        try:
            self._proc(buf)
        finally:
            self.close()

class Conductor(object):
    def __init__(self):
        self._signal = None
        self._create = False

    def _clear(self):
        close_port(CONDUCTOR_PORT)

    def create(self, manager):
        if not self._create:
            self._clear()
            self._create = True
            conductor.initialize(manager)
            self._signal = SignalingClient(manager.addr, conductor.handle)
            ws_start(ConductorHandler, CONDUCTOR_PORT)
