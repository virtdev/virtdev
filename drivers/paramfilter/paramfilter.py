# paramfilter.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver, wrapper

PRINT = False

class ParamFilter(Driver):
    @wrapper
    def put(self, *args, **kwargs):
        output = {}
        for i in kwargs:
            if i.startswith('__'):
                output[i] = kwargs[i]
        if output:
            if PRINT:
                print('ParamFilter: output=%s' % str(output))
            return output
