#      timecalc.py
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

from anon import VDevAnon
from threading import Lock

DEBUG_TIMECALC = False
TIMECALC_TOTAL = 1000

class TimeCalc(VDevAnon):
    def __init__(self, name=None, sock=None):
        VDevAnon.__init__(self, name, sock)
        self._lock = Lock()
        self._time = 0
        self._cnt = 0
    
    def _calc(self, time):
        self._lock.acquire()
        try:
            if self._cnt < TIMECALC_TOTAL:
                self._time += time
                self._cnt += 1
                if self._cnt == TIMECALC_TOTAL:
                    t = self._time / TIMECALC_TOTAL
                    self._time = 0
                    self._cnt = 0
                    if DEBUG_TIMECALC:
                        print('TimeCalc: time=%f' % t)
                    return t
        finally:
            self._lock.release()
    
    def put(self, buf):
        args = self._get_args(buf)
        if args and type(args) == dict:
            time = args.get('Time')
            if time:
                t = self._calc(time)
                if t:
                    return {'Time':t}
    
if __name__ == '__main__':
    import random
    calc = TimeCalc()
    for _ in range(TIMECALC_TOTAL):
        args = str({'Time':random.uniform(0, 1)})
        ret = calc.put(args)
        if ret:
            print 'TimeCalc: ret=%s' % str(ret)
    