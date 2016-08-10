# freq.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.log import log_debug
from conf.log import LOG_FREQ
from lib.loader import Loader
from lib.attributes import ATTR_FREQ

class Freq(object):
    def __init__(self, uid):
        self._freq = {}
        self._loader = Loader(uid)
    
    def _log(self, text):
        if LOG_FREQ:
            log_debug(self, text)
    
    def _get(self, name):
        freq = self._loader.get_attr(name, ATTR_FREQ, float)
        if freq != None:
            self._freq[name] = freq
            self._log('name=%s, freq=%s' % (str(name), str(freq)))
            return freq
    
    def get(self, name):
        if self._freq.has_key(name):
            ret = self._freq.get(name)
            if ret != None:
                return ret
        return self._get(name)
    
    def remove(self, name):
        if self._freq.has_key(name):
            del self._freq[name]
