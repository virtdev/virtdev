# countdown.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver, wrapper

PRINT = False

class Countdown(Driver):
    @wrapper
    def put(self, *args, **kwargs):
        output = {}
        if kwargs and kwargs.has_key('__cnt__'):
            cnt = int(kwargs['__cnt__']) - 1
            if cnt >= 0:
                output['__cnt__'] = cnt
                if PRINT:
                    print('Countdown: output=%d' % output)
                return output
