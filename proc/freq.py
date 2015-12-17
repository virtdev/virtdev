#      freq.py
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

from lib.loader import Loader
from lib.log import log, log_get
from lib.attributes import ATTR_FREQ

PRINT = True

class Freq(object):
    def __init__(self, uid):
        self._freq = {}
        self._loader = Loader(uid)
        
    def _print(self, text):
        if PRINT:
            log(log_get(self, text))
    
    def _get(self, name):
        freq = self._loader.get_attr(name, ATTR_FREQ, float)
        if freq != None:
            self._freq[name] = freq
            self._print('name=%s, freq=%s' % (str(name), str(freq)))
            return freq
    
    def get(self, name):
        if self._freq.has_key(name):
            ret = self._freq.get(name)
            if ret != None:
                return ret
        return self._get(name)
    
    def remove(self, name):
        if self._freq.has_key(name):
            del self._freq[name]
