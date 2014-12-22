#      mapper.py
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

import sandbox
from lib.log import log_err
from loader import VDevLoader
from base64 import encodestring
from sandbox import VDEV_SANDBOX_PUT
from conf.virtdev import VDEV_MAPPER_PORT

VDEV_MAPPER_TIMEOUT = 30000

class VDevMapper(object):
    def __init__(self, uid):
        self._mappers = {}
        self._loader = VDevLoader(uid)
    
    def _get_code(self, name):
        buf = self._mappers.get(name)
        if not buf:
            buf = self._loader.get_mapper(name)
            self._mappers.update({name:buf})
        return buf
    
    def check(self, name):
        if self._mappers.has_key(name):
            if self._mappers[name]:
                return True
        else:
            buf = self._loader.get_mapper(name)
            self._mappers.update({name:buf})
            if buf:
                return True
    
    def remove(self, name):
        if not self._mappers.has_key(name):
            return
        del self._mappers[name]
    
    def put(self, name, buf):
        try:
            code = self._get_code(name)
            if code:
                return sandbox.request(VDEV_MAPPER_PORT, VDEV_SANDBOX_PUT, code=encodestring(code), args=buf)
        except:
            log_err(self, 'failed to put')
    