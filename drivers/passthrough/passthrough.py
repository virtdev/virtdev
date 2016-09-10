# Passthrough.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver, wrapper

PRINT = False

class Passthrough(Driver):
    @wrapper
    def put(self, *args, **kwargs):
        if PRINT:
            print('Passthrough: input=%s' % str(kwargs))
        return kwargs
