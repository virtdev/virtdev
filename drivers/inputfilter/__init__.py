#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver, wrapper

PRINT = False

class InputFilter(Driver):
    @wrapper
    def put(self, *args, **kwargs):
        output = {}
        for i in kwargs:
            if not i.startswith('__'):
                output[i] = kwargs[i]
        if output:
            if PRINT:
                print('InputFilter: output=%s' % str(output))
            return output
