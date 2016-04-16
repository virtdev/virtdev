#      proc.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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

import socket
from lib import bson
from lib.pool import Pool
from conf.virtdev import HA
from lib.queue import Queue
from lib.log import log, log_err
from threading import Lock, Thread
from RestrictedPython import compile_restricted
from lib.util import send_pkt, recv_pkt, unicode2str

QUEUE_LEN = 2
POOL_SIZE = 32

def put(addr, **args):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(addr)
    try:
        buf = bson.dumps(args)
        send_pkt(sock, buf)
        res = recv_pkt(sock)
        return unicode2str(bson.loads(res)[''])
    finally:
        sock.close()

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
        log('failed to evaluate')

class ProcQueue(Queue):
    def __init__(self, srv):
        Queue.__init__(self, QUEUE_LEN)
        self._srv = srv
    
    def proc(self, sock):
        self._srv.proc(sock)

class Proc(Thread):
    def _init_sock(self, addr):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(addr)
        self._sock.listen(5)
    
    def __init__(self, manager, addr):
        self._lock = Lock()
        Thread.__init__(self)
        self._init_sock(addr)
        self._manager = manager
        self._pool = Pool()
        for _ in range(POOL_SIZE):
            self._pool.add(ProcQueue(self))
    
    def proc(self, sock):
        try:
            res = ''
            buf = recv_pkt(sock)
            if buf:
                req = unicode2str(bson.loads(buf))
                if type(req) == dict:
                    device = None
                    if HA:
                        device = self._manager.get_passive_device()
                    ret = _exec(device, req['code'], req['args'])
                    if ret:
                        res = ret
            send_pkt(sock, bson.dumps({'':res}))
        finally:
            sock.close()
    
    def run(self):
        while True:
            try:
                sock, _ = self._sock.accept()
                self._pool.push(sock)
            except:
                log_err(self, 'failed to process')
