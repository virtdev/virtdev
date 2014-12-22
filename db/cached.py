#      cached.py
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

import os
from threading import Thread
from conf.virtdev import VDEV_CACHE_PORTS
from lib.util import service_start, service_join

class VDevDBCacheD(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._services = []
        for i in VDEV_CACHE_PORTS:
            self._services.append(Thread(target=self._create, args=(VDEV_CACHE_PORTS[i],)))
    
    def _create(self, port):
        cmd = 'memcached -u root -m 10 -p %d' % port
        os.system(cmd)
    
    def run(self):
        if self._services:
            service_start(*self._services)
            service_join(*self._services)
