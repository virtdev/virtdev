#      server.py
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

import socket
from lib import tunnel, crypto
from oper import VDevFSOperation
from threading import Thread, Event
from lib.log import log_err, log_get
from conf.virtdev import VDEV_FS_PORT
from lib.request import VDevAuthRequest
from lib.util import DEFAULT_UID, DEFAULT_TOKEN, UID_SIZE

TIMEOUT = 10

class VDevFSServer(object):
    def _init_sock(self, addr):
        addr = tunnel.addr2ip(addr)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((addr, VDEV_FS_PORT))
        self._sock.listen(5)
    
    def __init__(self, manager):
        uid = manager.uid
        addr = manager.addr
        token = manager.token
        tunnel.create(addr, token)
        
        self._srv = None
        self._addr_list = {}
        self._event = Event()
        self._op = VDevFSOperation(manager)
        self._tokens = {uid:token, DEFAULT_UID:DEFAULT_TOKEN}
        self._init_sock(manager.addr)
        
        self.uid = uid
        self.addr = addr
        self.token = token
        self.user = manager.user
        self.devices = manager.devices
        self.synchronizer = manager.synchronizer
        self.request = VDevAuthRequest(uid, token)
    
    def get_device(self, name):
        addr = self._addr_list.get(name)
        if not addr:
            res = self.request.device.get(name=name)
            if res:
                addr = (res['uid'], res['addr'])
                self._addr_list.update({name:addr})
        if not addr:
            log_err(self, 'failed to get device')
            raise Exception(log_get(self, 'failed to get device'))
        return addr
    
    def get_token(self, name):
        token = self._tokens.get(name)
        if not token:
            if name == self.uid:
                log_err(self, 'failed to get token, invalid uid')
                raise Exception(log_get(self, 'failed to get token'))
            token = self.request.token.get(name=name)
            if token:
                self._tokens.update({name:token})
        if not token:
            log_err(self, 'failed to get token')
            raise Exception(log_get(self, 'failed to get token'))
        return token
    
    def _proc(self, sock):
        if not sock:
            return
        try:
            buf = tunnel.recv(sock)
            if len(buf) <= UID_SIZE:
                log_err(self, 'failed to process, invalid request')
                return
            uid = buf[0:UID_SIZE]
            token = self.get_token(uid)
            if not token:
                log_err(self, 'failed to process, no token')
                return
            req = crypto.unpack(uid, buf, token)
            if req:
                op = req.get('op')
                args = req.get('args')
                if not op or op[0] == '_' or type(args) != dict:
                    log_err(self, 'failed to process, invalid request, op=%s' % str(op))
                    return           
                func = getattr(self._op, op)
                if func:
                    res = func(**args)
                else:
                    res = ''
                    log_err(self, 'failed to process, cannot handle operation %s' % str(op))
                tunnel.send(sock, crypto.pack(uid, res, token))
        finally:
            sock.close()
    
    def _start(self):
        while True:
            try:
                conn, _ = self._sock.accept()
                conn.settimeout(TIMEOUT)
                Thread(target=self._proc, args=(conn,)).start()
            except:
                log_err(self, 'failed to process')
    
    def start(self):
        if not self._srv:
            self._srv = Thread(target=self._start)
            self._srv.start()
    