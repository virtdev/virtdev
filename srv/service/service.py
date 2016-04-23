#      service.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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

from lib.log import log_err

class Service(object):
    def __init__(self, query):
        self._query = query
    
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def proc(self, op, args):
        try:
            if op[0] == '_':
                log_err(self, 'failed to process, invalid operation')
                return
            func = getattr(self, op)
            if not func:
                log_err(self, 'failed to process, invalid function')
                return
            return func(**args)
        except:
            log_err(self, 'failed to process')
