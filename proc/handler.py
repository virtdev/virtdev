#      handler.py
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

import proc
from lib.log import log_err
from lib.loader import Loader
from fs.attr import ATTR_HANDLER
from conf.virtdev import PROC_ADDR, HANDLER_PORT

class Handler(object):  
    def __init__(self, uid, addr=PROC_ADDR):
        self._handlers = {}
        self._loader = Loader(uid)
        self._addr = (addr, HANDLER_PORT)
    
    def _get_code(self, name):
        buf = self._handlers.get(name)
        if not buf:
            buf = self._loader.get_attr(name, ATTR_HANDLER, str)
            self._handlers.update({name:buf})
        return buf
    
    def remove(self, name):
        if self._handlers.has_key(name):
            del self._handlers[name]
    
    def check(self, name):
        if self._handlers.get(name):
            return True
        else:
            buf = self._loader.get_attr(name, ATTR_HANDLER, str)
            if buf:
                self._handlers.update({name:buf})
                return True
    
    def put(self, name, buf):
        try:
            code = self._get_code(name)
            if code == None:
                code = self._get_code(name)
                if not code:
                    return
            return proc.put(self._addr, code=code, args=buf)
        except:
            log_err(self, 'failed to put')
