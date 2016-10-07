#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver
from lib.modes import MODE_OVP, MODE_SWITCH

class TextGen(Driver):
    def __init__(self, name=None):
        Driver.__init__(self, name=name, mode=MODE_OVP | MODE_SWITCH, freq=1)
    
    def setup(self):
        self._active = False
    
    def get(self):
        if self._active:
            self._active = False
            return {'name':self.get_name(), 'content':'This is TextGen'}
    
    def open(self):
        self._active = True
