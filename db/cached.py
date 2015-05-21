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

from subprocess import call
from threading import Thread
from conf.virtdev import CACHE_PORTS
from lib.util import srv_start, srv_join

class CacheD(Thread):
    def _create(self, port):
        call(['memcached', '-u', 'root', '-m', '10', '-p', str(port)])
    
    def run(self):
        srv = []
        for i in CACHE_PORTS:
            srv.append(Thread(target=self._create, args=(CACHE_PORTS[i],)))
        
        if srv:
            srv_start(srv)
            srv_join(srv)
