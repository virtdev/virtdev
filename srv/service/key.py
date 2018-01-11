# key.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from service import Service

class Key(Service):
    def get(self, uid, name):
        device = self._query.device.get(name)
        if device:
            if device['uid'] != uid:
                guests = self._query.guest.get(uid)
                if not guests or name not in guests:
                    return
            return self._query.key.get(name)
