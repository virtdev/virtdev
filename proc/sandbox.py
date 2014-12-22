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
from lib.util import send_pkt, recv_pkt
from conf.virtdev import VDEV_SANDBOX_ADDR
from multiprocessing.pool import ThreadPool
from RestrictedPython import compile_restricted
from RestrictedPython.Guards import safe_builtins

VDEV_SANDBOX_TIMEOUT = 20 # seconds

VDEV_SANDBOX_PUT = 'put'
VDEV_SANDBOX_OP = [VDEV_SANDBOX_PUT]

restricted_globals = dict(__builtins__ = safe_builtins)

def request(port, op, **args):
    if op not in VDEV_SANDBOX_OP:
        log('This operation is not supported by VDevSandbox')
        return
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((VDEV_SANDBOX_ADDR, port))
    try:
        buf = json.dumps({'op':op, 'args':args})
        send_pkt(sock, buf)
        return recv_pkt(sock)
    finally:
        sock.close()

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
        self._sock.bind((VDEV_SANDBOX_ADDR, port))
        self._sock.listen(5)
    
    def __init__(self, port):
        self._lock = Lock()
        Thread.__init__(self)
        self._init_sock(port)
    
    def _put(self, code, args):
        pool = ThreadPool(processes=1)
        result = pool.apply_async(_sandbox, args=(decodestring(code), args))
        try:
            return result.get(timeout=VDEV_SANDBOX_TIMEOUT)
        finally:
            pool.terminate()
            return
    
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
                    if op == VDEV_SANDBOX_PUT:
                        ret = self._put(**args)
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
    