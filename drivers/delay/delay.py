# delay.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import time
from dev.driver import Driver, wrapper

PRINT = False

class Delay(Driver):
    @wrapper
    def put(self, *args, **kwargs):
        if kwargs.has_key('__time__'):
            t = float(kwargs.get('__time__'))
            if t > 0:
                if PRINT:
                    print('Delay: time=%s' % str(t))
                time.sleep(t)
            del kwargs['__time__']
        return kwargs
