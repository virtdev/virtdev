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

from threading import Thread
from lib.log import log_err, log_get
from conf.virtdev import BROKER, WORKER
from lib.util import srv_start, srv_join

class Server(Thread):
    def __init__(self, query=None):
        Thread.__init__(self)
        self._query = query
    
    def run(self):
        srv = []
        if WORKER:
            if not self._query:
                log_err(self, 'no query')
                raise Exception(log_get(self, 'no query'))
            from worker import Worker
            srv.append(Worker(self._query))
        if BROKER:
            from broker import Broker
            srv.append(Broker())
        srv_start(srv)
        srv_join(srv)
