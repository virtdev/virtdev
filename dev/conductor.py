# conductor.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from threading import Event
from lib import channel, codec
from lib.request import Request
from operation import Operation
from conf.defaults import DEBUG
from conf.log import LOG_CONDUCTOR
from lib.ws import WSHandler, ws_start
from lib.util import UID_SIZE, get_name
from conf.defaults import CONDUCTOR_PORT
from lib.log import log_debug, log_err, log_get

def chkstat(func):
    def _chkstat(*args, **kwargs):
        self = args[0]
        if self._ready:
            return func(*args, **kwargs)
    return _chkstat

class ConductorServer(object):
    def __init__(self):
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
            log_err(self, 'failed to get device')
            raise Exception(log_get(self, 'failed to get device'))
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
    
    def _proc(self, req):
        op = req.get('op')
        args = req.get('args')
        if not op or op[0] == '_' or type(args) != dict:
            log_err(self, 'failed to process, invalid request, op=%s' % str(op))
            return
        func = getattr(self._op, op)
        if func:
            if args:
                func(**args)
            else:
                func()
            self._log('op=%s, args=%s' % (str(op), str(args)))
        else:
            log_err(self, 'failed to process, op=%s' % str(op))
    
    def _proc_safe(self, req):
        try:
            self._proc(req)
        except:
            log_err(self, 'failed to process')
    
    def proc(self, req):
        if DEBUG:
            self._proc(req)
        else:
            self._proc_safe(req)

conductor = ConductorServer()

class ConductorHandler(WSHandler):
    def _get_head(self, buf):
        return buf[0:UID_SIZE]
    
    def on_message(self, buf):
        try:
            if len(buf) <= UID_SIZE:
                return
            head = self._get_head(buf)
            token = conductor.get_token(head)
            if not token:
                log_err(self, 'failed, no token')
                return
            try:
                req = codec.decode(buf, token)
            except:
                token = conductor.get_token(head, update=True)
                if not token:
                    log_err(self, 'failed, no token')
                    return
                req = codec.decode(buf, token)
            
            if req:
                op = req.get('op')
                args = req.get('args')
                if not op or op[0] == '_' or type(args) != dict:
                    log_err(self, 'failed, op=%s' % str(op))
                    return
                conductor.proc(req)
        finally:
            self.close()

class Conductor(object):
    def __init__(self):
        self._create = False
    
    def create(self, manager):
        if not self._create:
            self._create = True
            conductor.initialize(manager)
            ws_start(ConductorHandler, CONDUCTOR_PORT)
