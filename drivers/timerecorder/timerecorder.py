# timerecorder.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import shelve
from datetime import datetime
from lib.util import readlink
from dev.driver import Driver, check_input

PRINT = False
INTERVAL = 1
HOME_TIMER = '~/vdev/dev/timer'
HOME_RECORDER = '~/vdev/dev/timerecorder'

class TimeRecorder(Driver):
    def setup(self):
        path = self._get_path()
        os.system('mkdir -p %s' % path)
        self._cnt = {}
    
    def _get_timer_path(self, timer, name):
        path = os.path.join(HOME_TIMER, timer, name)
        return readlink(path)
    
    def _get_path(self, name=''):
        path = os.path.join(HOME_RECORDER, self.get_name(), name)
        return readlink(path)
    
    def _save(self, timer, name):
        t_end = datetime.utcnow()
        path = self._get_timer_path(timer, name)
        if not os.path.exists(path):
            return
        d = shelve.open(path)
        try:
            t_start = datetime.strptime(d['t'], "%Y-%m-%d %H:%M:%S.%f")
        finally:
            d.close()
        t = (t_end - t_start).total_seconds()
        path = self._get_path(name)
        d = shelve.open(path)
        try:
            d['t'] = t
        finally:
            d.close()
        if PRINT:
            print('TimeRecorder: name=%s, time=%f' % (name, t))
        return t
    
    @check_input
    def put(self, args):
        name = args.get('name')
        timer = args.get('timer')
        if name and timer:
            if not self._cnt.has_key(name):
                cnt = 0
            else:
                cnt = self._cnt.get(name)
            
            if cnt < INTERVAL:
                cnt += 1
                self._cnt.update({name:cnt})
                if cnt == INTERVAL: 
                    t = self._save(timer, name)
                    if t:
                        return {'name':name, 'time':t}
