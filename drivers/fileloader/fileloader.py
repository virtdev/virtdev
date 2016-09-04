# fileloader.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
from base64 import b64encode
from dev.driver import Driver
from lib.util import readlink
from lib.modes import MODE_OVP, MODE_SWITCH

HOME = '~/vdev/dev/fileloader'

class FileLoader(Driver):
    def __init__(self, name=None):
        Driver.__init__(self, name=name, mode=MODE_OVP | MODE_SWITCH, freq=1)
    
    def setup(self):
        path = self._get_path()
        os.system('mkdir -p %s' % path)
        self._files = None
        self._active = False
    
    def _get_path(self):
        path = os.path.join(HOME, self.get_name())
        return readlink(path)
    
    def _load(self):
        path = self._get_path()
        for name in os.listdir(path):
            file_path = os.path.join(path, name)
            with open(file_path) as f:
                buf = f.read()
            if buf:
                yield {'name':self.get_name(), 'content':b64encode(buf)}
    
    def get(self):
        if not self._active:
            return
        try:
            return self._files.next()
        except StopIteration:
            self._active = False
    
    def open(self):
        self._files = self._load()
        self._active = True
