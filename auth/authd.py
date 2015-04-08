#      authd.py
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
from lib.util import service_start, service_join
from conf.virtdev import AUTH_BROKER, AUTH_WORKER

class VDevAuthD(Thread):
    def __init__(self, query=None):
        Thread.__init__(self)
        self._services = []
        if AUTH_WORKER:
            if not query:
                log_err(self, 'no query')
                raise Exception(log_get(self, 'no query'))
            from worker import VDevAuthWorker
            self._services.append(VDevAuthWorker(query))
        if AUTH_BROKER:
            from broker import VDevAuthBroker
            self._services.append(VDevAuthBroker())
    
    def run(self):
        if self._services:
            service_start(*self._services)
            service_join(*self._services)
    