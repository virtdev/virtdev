#      timer.py
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
import shelve
from datetime import datetime
from dev.driver import Driver, check_input

PRINT = False
PATH = '/opt/timer'

class Timer(Driver):
    def _get_dir(self):
        return os.path.join(PATH, self.get_name())
    
    def _get_path(self, name):
        return os.path.join(self._get_dir(), name)
    
    def setup(self):
        if self.get_name():
            path = self._get_dir()
            if not os.path.exists(path):
                os.makedirs(path, 0o755)
    
    def _save(self, name):
        t = str(datetime.utcnow())
        path = self._get_path(name)
        if not os.path.exists(path):
            d = shelve.open(path)
            try:
                d['t'] = t
            finally:
                d.close()
            if PRINT:
                print('Timer: name=%s, time=%s' % (name, t))
        return True
    
    @check_input
    def put(self, args):
        name = args.get('name')
        if name:
            if self._save(name):
                args.update({'timer':self.get_name()})
                return args
