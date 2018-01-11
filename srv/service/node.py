# node.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from service import Service
from lib.util import str2tuple
from lib.modes import MODE_VISI

class Node(Service):
    def _get_uid(self, user):
        return self._query.user.get(user, 'uid')

    def find(self, uid, user, node):
        uid = self._get_uid(user)
        if not uid:
            return
        nodes = self._query.node.get(uid)
        for item in nodes:
            n, _, mode = str2tuple(item)
            if mode and int(mode) & MODE_VISI and n == node:
                return {'uid':uid}
