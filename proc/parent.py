#      parent.py
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
from fs.attr import ATTR_PARENT

class Parent(object):
    def __init__(self, uid):
        self._parent = {}
        self._loader = Loader(uid)
    
    def get(self, name):
        if self._parent.has_key(name):
            return self._parent[name]
        else:
            parent = self._loader.get_attr(name, ATTR_PARENT, str)
            self._parent[name] = parent
            return parent
    
    def remove(self, name):
        if self._parent.has_key(name):
            del self._parent[name]
