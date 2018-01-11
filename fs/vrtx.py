# vrtx.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
from entry import Entry
from  errno import EINVAL
from lib.log import log_err
from fuse import FuseOSError

class Vrtx(Entry):
    def getattr(self, uid, name):
        return self.lsattr(uid, name, symlink=True)

    def create(self, uid, name):
        self.symlink(uid, name)
        return 0

    def open(self, uid, name, flags):
        return self.create(uid, name)

    def unlink(self, uid, name):
        self.remove(uid, name)

    def readdir(self, uid, name):
        return self.lsdir(uid, name)

    def readlink(self, uid, name):
        return self.lslink(uid, name)

    def initialize(self, uid, name, vrtx):
        if type(vrtx) != list:
            log_err(self, 'failed to initialize')
            raise FuseOSError(EINVAL)

        for i in vrtx:
            v = os.path.join(name, i)
            self.create(uid, v)
