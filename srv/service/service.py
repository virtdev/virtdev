# service.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.log import log_err

class Service(object):
    def __init__(self, query):
        self._query = query
    
    def __str__(self):
        return self.__class__.__name__.lower()
    
    def proc(self, op, args):
        try:
            if op[0] == '_':
                log_err(self, 'failed to process, invalid operation')
                return
            func = getattr(self, op)
            if not func:
                log_err(self, 'failed to process, invalid function')
                return
            return func(**args)
        except:
            log_err(self, 'failed to process')
