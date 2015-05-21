#      timesaver.py
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
from dev.driver import Driver
from drivers.timer import get_path

PRINT = False

class TimeSaver(Driver):
    def _save(self, timer, name):
        path = os.path.join(get_path(timer), name)
        if not os.path.exists(path):
            return
        d = shelve.open(path)
        try:
            t_end = datetime.utcnow()
            t_start = datetime.strptime(d['start'], "%Y-%m-%d %H:%M:%S.%f")
            t = (t_end - t_start).total_seconds()
            d['time'] = t
            if PRINT:
                print('TimeSaver: name=%s, time=%f' % (name, t))
            return t
        finally:
            d.close()
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            name = args.get('Name')
            timer = args.get('Timer')
            if name and timer:
                t = self._save(timer, name)
                if t:
                    return {'Time':t}
