# user.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib import resolv
from lib.types import *
from service import Service
from db.marker import Marker
from conf.route import ROUTE, AREA
from lib.log import log_err, log_get
from lib.util import get_name, update_device, gen_key, gen_token

class User(Service):
    def __init__(self, query):
        Service.__init__(self, query)
        if ROUTE:
            self._marker = Marker()

    def _check_node(self, uid, node, addr, mode, key):
        name = get_name(uid, node)
        if self._query.key.get(name):
            self._query.node.delete(uid, (node,))
        else:
            if ROUTE:
                self._marker.mark(name, DOMAIN_DEV, AREA)
        self._query.key.put(name, key)
        self._query.node.put(uid, (node, addr, str(mode)))
        update_device(self._query, uid, node, addr, name)

    def login(self, uid, node, mode):
        key = gen_key()
        token = self._query.token.get(uid)
        if not token:
            self._query.token.put(uid, gen_token())
            token = self._query.token.get(uid)
            if not token:
                log_err(self, 'no token')
                raise Exception(log_get(self, 'no token'))
        addr = resolv.get_addr(uid, node)
        if not addr:
            log_err(self, 'invalid address')
            raise Exception(log_get(self, 'invalid address'))
        self._check_node(uid, node, addr, mode, key)
        return {'uid':uid, 'addr':addr, 'token':token, 'key':key}

    def logout(self, uid, node, addr):
        self._query.node.delete(uid, (node,))
        self._query.member.delete(uid, ('', node))
        return True
