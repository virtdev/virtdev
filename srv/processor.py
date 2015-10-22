#      processor.py
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
#
#      This work originally started from the example of Paranoid Pirate Pattern,
#      which is provided by Daniel Lundin <dln(at)eintr(dot)org>

import zerorpc
from lib.log import log_err
from threading import Thread
from services.key import Key
from services.user import User
from services.node import Node
from services.token import Token
from services.guest import Guest
from services.device import Device
from lib.package import pack, unpack
from lib.util import UID_SIZE, zmqaddr
from conf.virtdev import PROCESSOR_PORT
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool

TIMEOUT = 120 # seconds

class ProcessorServer(object):
    def __init__(self, query):
        self._query = query

    def _add_service(self, srv):
        self._services.update({str(srv):srv})
    
    def _init_services(self, query):
        self._services = {}
        self._query = query
        self._add_service(Key(query))
        self._add_service(User(query))
        self._add_service(Node(query))
        self._add_service(Guest(query))
        self._add_service(Token(query))
        self._add_service(Device(query))
    
    def proc(self, uid, token, buf):
        try:
            req = unpack(None, buf, token)
            if not req:
                log_err(self, 'failed to process, invalid request')
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
                return pack(buf[:UID_SIZE], res, token)
            except TimeoutError:
                log_err(self, 'failed to process, timeout')
            finally:
                pool.join()
        except:
            log_err(self, 'failed to process')

class Processor(Thread):
    def __init__(self, addr):
        self._addr = addr
    
    def run(self):
        srv = zerorpc.Server(ProcessorServer())
        srv.bind(zmqaddr(self._addr, PROCESSOR_PORT))
        srv.run()
