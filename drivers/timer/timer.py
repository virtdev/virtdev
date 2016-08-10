# timer.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import shelve
from datetime import datetime
from dev.driver import Driver, check_input

PRINT = False
HOME = '/opt/timer'

class Timer(Driver):
    def _get_dir(self):
        return os.path.join(HOME, self.get_name())
    
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
