#      sandbox.py
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

import json
import socket
from base64 import decodestring
from lib.log import log, log_err
from threading import Lock, Thread
from conf.virtdev import SANDBOX_ADDR
from lib.util import send_pkt, recv_pkt
from multiprocessing.pool import ThreadPool
from RestrictedPython import compile_restricted

TIMEOUT = 20 # seconds
SANDBOX_PUT = 'put'
SANDBOX_OP = [SANDBOX_PUT]

def request(port, op, **args):
    if op not in SANDBOX_OP:
        log('This operation is not supported by VDevSandbox')
        return
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((SANDBOX_ADDR, port))
    try:
        buf = json.dumps({'op':op, 'args':args})
        send_pkt(sock, buf)
        return recv_pkt(sock)
    finally:
        sock.close()

def _getattr_(obj, attr):
    t = type(obj)
    if t in [dict, list, tuple, int, float]:
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

def _sandbox(code, args):
    try:
        func = None
        code = compile_restricted(code, '<string>', 'exec')
        exec(code)
        if func:
            return func(args)
    except:
        log('failed to launch')

class VDevSandbox(Thread):
    def _init_sock(self, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((SANDBOX_ADDR, port))
        self._sock.listen(5)
    
    def __init__(self, port):
        self._lock = Lock()
        Thread.__init__(self)
        self._init_sock(port)
    
    def _put(self, code, args):
        ret = None
        pool = ThreadPool(processes=1)
        result = pool.apply_async(_sandbox, args=(decodestring(code), args))
        try:
            ret = result.get(timeout=TIMEOUT)
        finally:
            pool.terminate()
            return ret
    
    def _proc(self, sock):
        try:
            res = ''
            buf = recv_pkt(sock)
            if buf:
                req = json.loads(buf)
                if type(req) == dict and req.has_key('op') and req.has_key('args'):
                    ret = None
                    op = req['op']
                    args = req['args']
                    if op == SANDBOX_PUT:
                        ret = self._put(code=args['code'], args=args['args'])
                    if ret:
                        res = str(ret)
            send_pkt(sock, res)
        finally:
            sock.close()
    
    def run(self):
        while True:
            try:
                sock = self._sock.accept()[0]
                if sock:
                    Thread(target=self._proc, args=(sock,)).start()
            except:
                if sock:
                    log_err(self, 'failed to process')
                    sock.close()
    