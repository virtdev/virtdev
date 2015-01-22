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
from aop import VDevAnonOper
from datetime import datetime

PATH_TIMER = '/tmp/timer'

class Timer(VDevAnonOper):
    def __init__(self, index):
        VDevAnonOper.__init__(self, index)
        if os.path.exists(PATH_TIMER):
            os.makedirs(PATH_TIMER, 0o755)
    
    def _create(self, name):
        start = False
        path = os.path.join(PATH_TIMER, name)
        if not os.path.exists(path):
            start = True
        d = shelve.open(path)
        try:
            if start:
                d['start'] =  str(datetime.utcnow())
                return True
            else:
                t_end = datetime.utcnow()
                t_start = datetime.strptime(d['start'], "%Y-%m-%d %H:%M:%S.%f")
                d['time'] = (t_end - t_start).total_seconds()
        finally:
            d.close()
    
    def put(self, buf):
        args = self._get_args(buf)
        if args and type(args) == dict:
            name = args.get('Name')
            if name:
                if self._create(name):
                    return args
    