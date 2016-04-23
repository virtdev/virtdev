#      worker.py
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
#
#      This work originally started from the example of Paranoid Pirate Pattern,
#      which is provided by Daniel Lundin <dln(at)eintr(dot)org>

import socket
from lib import bson
from lib import codec
from lib.pool import Pool
from lib.queue import Queue
from service.key import Key
from threading import Thread
from service.user import User
from service.node import Node
from conf.log import LOG_WORKER
from service.token import Token
from service.guest import Guest
from service.device import Device
from lib.log import log_debug, log_err
from conf.virtdev import REQUESTER_PORT
from multiprocessing.pool import ThreadPool
from multiprocessing import TimeoutError, cpu_count
from lib.util import UID_SIZE, send_pkt, recv_pkt, unicode2str

TIMEOUT = 120 # seconds
QUEUE_LEN = 2
POOL_SIZE = cpu_count() * 4

class WorkerQueue(Queue):
    def __init__(self, srv):
        Queue.__init__(self, QUEUE_LEN)
        self._srv = srv
    
    def proc(self, sock):
        self._srv.proc(sock)

class Worker(Thread):
    def __init__(self, addr, query):
        Thread.__init__(self)
        self._init_sock(addr)
        self._init_services(query)
        self._pool = Pool()
        for _ in range(POOL_SIZE):
            self._pool.add(WorkerQueue(self))
    
    def _init_sock(self, addr):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((addr, REQUESTER_PORT))
        self._sock.listen(5)
        
    def _init_services(self, query):
        self._services = {}
        self._query = query
        self._add_service(Key(query))
        self._add_service(User(query))
        self._add_service(Node(query))
        self._add_service(Guest(query))
        self._add_service(Token(query))
        self._add_service(Device(query))
    
    def _log(self, text):
        if LOG_WORKER:
            log_debug(self, text)
    
    def _add_service(self, srv):
        self._services.update({str(srv):srv})
    
    def proc(self, sock):
        try:
            pkt = recv_pkt(sock)
            if pkt:
                reqest = unicode2str(bson.loads(pkt))
                uid = reqest['uid']
                token = reqest['token']
                buf = reqest['buf']
                req = codec.decode(None, buf, token)
                if not req:
                    log_err(self, 'failed to process, invalid request')
                    return
            else:
                log_err(self, 'failed to process')
                return
            op = req.get('op')
            srv = req.get('srv')
            args = req.get('args')
            if not op or not srv:
                log_err(self, 'failed to process, invalid arguments')
                return
            args.update({'uid':uid})
            if not self._services.has_key(srv):
                log_err(self, 'invalid service %s' % str(srv))
                return
            pool = ThreadPool(processes=1)
            result = pool.apply_async(self._services[srv].proc, args=(op, args))
            pool.close()
            try:
                res = result.get(timeout=TIMEOUT)
                ret = codec.encode(buf[:UID_SIZE], res, token)
                send_pkt(sock, bson.dumps({'':ret}))
            except TimeoutError:
                log_err(self, 'failed to process, timeout')
            finally:
                pool.join()
        except:
            log_err(self, 'failed to process')
        finally:
            sock.close()
    
    def run(self):
        self._log('start ...')
        while True:
            try:
                sock, _ = self._sock.accept()
                self._pool.push(sock)
            except:
                log_err(self, 'failed to process')
