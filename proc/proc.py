# proc.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib import io
from lib import bson
from lib.log import log_err
from threading import Thread
from SocketServer import BaseRequestHandler
from RestrictedPython import compile_restricted
from lib.util import unicode2str, create_server

_manager = None

def put(addr, port, **args):
    sock = io.connect(addr, port)
    try:
        buf = bson.dumps(args)
        io.send_pkt(sock, buf)
        res = io.recv_pkt(sock)
        return unicode2str(bson.loads(res)[''])
    finally:
        io.close(sock)

def _getattr_(obj, attr):
    t = type(obj)
    if t in [dict, list, tuple, int, float, str, unicode]:
        return getattr(obj, attr)
    else:
        raise RuntimeError('invalid object')

def _getitem_(obj, item):
    t = type(obj)
    if t == dict:
        return dict.__getitem__(obj, item)
    elif t == list:
        return list.__getitem__(obj, item)
    elif t == tuple:
        return tuple.__getitem__(obj, item)
    else:
        raise RuntimeError('invalid object')

def _getiter_(obj):
    t = type(obj)
    if t in [dict, list, tuple]:
        return iter(obj)
    else:
        raise RuntimeError('invalid object')

def _exec(device, code, args):
    try:
        if device:
            return device.execute(code, args)
        else:
            func = None
            res = compile_restricted(code, '<string>', 'exec')
            exec(res)
            if func:
                return func(args)
    except:
        log_err(None, 'failed to execute')


class ProcServer(BaseRequestHandler):
    def handle(self):
        try:
            res = ''
            buf = io.recv_pkt(self.request)
            if buf:
                req = unicode2str(bson.loads(buf))
                if type(req) == dict:
                    device = _manager.compute_unit
                    ret = _exec(device, req['code'], req['args'])
                    if ret:
                        res = ret
            io.send_pkt(self.request, bson.dumps({'':res}))
        except:
            pass

class Proc(object):
    def __init__(self, manager, addr, port):
        global _manager
        if not _manager:
            _manager = manager
        self._addr = addr
        self._port = port
    
    def start(self):
        Thread(target=create_server, args=(self._addr, self._port, ProcServer)).start()
