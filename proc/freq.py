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

from loader import VDevLoader

class VDevFreq(object):
    def __init__(self, uid):
        self._freq = {}
        self._loader = VDevLoader(uid)
    
    def get(self, name):
        if self._freq.has_key(name):
            return self._freq[name]
        else:
            freq = self._loader.get_freq(name)
            self._freq[name] = freq
            return freq
    
    def remove(self, name):
        if self._freq.has_key(name):
            del self._freq[name]