#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import shelve
from copy import copy
from datetime import datetime
from lib.util import readlink
from dev.driver import Driver, wrapper

PRINT = False
HOME = '~/vdev/dev/timer'

class Timer(Driver):
    def _get_dir(self):
        path = os.path.join(HOME, self.get_name())
        return readlink(path)

    def _get_path(self, name):
        return os.path.join(self._get_dir(), name)

    def setup(self):
        path = self._get_dir()
        os.system('mkdir -p %s' % path)

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

    @wrapper
    def put(self, *args, **kwargs):
        name = kwargs.get('name')
        if name:
            if self._save(name):
                output = copy(kwargs)
                output.update({'timer':self.get_name()})
                return output
