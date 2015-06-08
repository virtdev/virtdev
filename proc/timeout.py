#      timeout.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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

from lib.loader import Loader
from fs.attr import ATTR_TIMEOUT

class Timeout(object):
    def __init__(self, uid):
        self._timeout = {}
        self._loader = Loader(uid)
    
    def get(self, name):
        if self._timeout.has_key(name):
            return self._timeout[name]
        else:
            timeout = self._loader.get_attr(name, ATTR_TIMEOUT, float)
            if timeout != None:
                self._timeout[name] = timeout
                return timeout
    
    def remove(self, name):
        if self._timeout.has_key(name):
            del self._timeout[name]
