#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver
from lib.modes import MODE_VIRT

class VDev(Driver):
    def __init__(self, name=None):
        Driver.__init__(self, name=name, mode=MODE_VIRT)
