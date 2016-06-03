#      conductor.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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

import tornado.web
import tornado.ioloop
import tornado.websocket
import tornado.httpserver
from lib.pool import Pool
from lib.queue import Queue
from lib import channel, codec
from lib.request import Request
from operation import Operation
from conf.log import LOG_CONDUCTOR
from threading import Thread, Event
from multiprocessing import cpu_count
from lib.util import UID_SIZE, get_name
from conf.virtdev import DEBUG, CONDUCTOR_PORT
from lib.log import log_debug, log_err, log_get

CACHE = True
POOL_SIZE = cpu_count() * 2
QUEUE_LEN = 2

def chkstat(func):
    def _chkstat(*args, **kwargs):
        self = args[0]
        if self._ready:
            return func(*args, **kwargs)
    return _chkstat

class ConductorQueue(Queue):
    def __init__(self, srv):
        Queue.__init__(self, QUEUE_LEN)
        self._srv = srv
    
    def proc(self, buf):
        self._srv.proc(buf)

class ConductorServer(object):
    def __init__(self):
        self._ready = False
    
    def initialize(self, manager):
        key = manager.key
        uid = manager.uid
        addr = manager.addr
        token = manager.token
        channel.create(uid, addr, key)
        
        self._keys = {}
        self._devices = {}
        self._pool = Pool()
        self._event = Event()
        self._tokens = {uid:token}
        self._op = Operation(manager)
        for _ in range(POOL_SIZE):
            self._pool.add(ConductorQueue(self))
        
        self.uid = uid
        self.addr = addr
        self.token = token
        self.user = manager.user
        self.devices = manager.devices
        self.request = Request(uid, token)
        self._ready = True
    
    def _get_token(self, uid, update=False):
        token = None
        if not update:
            token = self._tokens.get(uid)
        if not token:
            if uid == self.uid:
                log_err(self, 'failed to get token, invalid uid')
                raise Exception(log_get(self, 'failed to get token'))
            token = self.request.token.get(name=uid)
            if token:
                self._tokens.update({uid:token})
        if not token:
            log_err(self, 'failed to get token')
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
    
    def _proc(self, buf):
        if len(buf) <= UID_SIZE:
            return
        uid = buf[0:UID_SIZE]
        token = self._get_token(uid)
        if not token:
            log_err(self, 'failed to process, no token')
            return
        try:
            req = codec.decode(uid, buf, token)
        except:
            token = self._get_token(uid, update=True)
            if not token:
                log_err(self, 'failed to process, no token')
                return
            req = codec.decode(uid, buf, token)
        if req:
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
            else:
                log_err(self, 'failed to process, invalid operation %s' % str(op))
    
    def _proc_safe(self, buf):
        try:
            self._proc(buf)
        except:
            log_err(self, 'failed to process')
    
    def proc(self, buf):
        if DEBUG:
            self._proc(buf)
        else:
            self._proc_safe(buf)
    
    @chkstat
    def put(self, buf):
        if not CACHE:
            self.proc(buf)
        else:
            self._pool.push(buf)

conductor = ConductorServer()

class ConductorHandler(tornado.websocket.WebSocketHandler):
    def on_message(self, buf):
        conductor.put(buf)    

class Conductor(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._create = False
    
    def _log(self, text):
        if LOG_CONDUCTOR:
            log_debug(self, text)
    
    def create(self, manager):
        if not self._create:
            self._create = True
            conductor.initialize(manager)
            self.start()
    
    def run(self):
        self._log('start ...')
        if not self._create:
            log_err(self, 'conductor is not initialized')
            return
        app = tornado.web.Application([(r'/', ConductorHandler)], debug=False)
        server = tornado.httpserver.HTTPServer(app)
        server.listen(CONDUCTOR_PORT)
        tornado.ioloop.IOLoop.instance().start()
