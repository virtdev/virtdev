# remotedb.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.util import lock
from threading import Lock
from conf.log import LOG_REMOTEDB
from lib.log import log_debug, log_err, log_get

COLLECTION_MAX = 1024

class RemoteDB(object):
    def __init__(self, router, domain=None):
        if not router:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
        self._lock = Lock()
        self._router = router
        self._domain = domain
        self._collections = {}

    def _log(self, text):
        if LOG_REMOTEDB:
            log_debug(self, text)

    @lock
    def _get_collection(self, addr):
        coll = self._collections.get(addr)
        if not coll:
            if len(self._collections) >= COLLECTION_MAX:
                _, coll = self._collections.popitem()
                self.release(coll)
            coll = self.connect(addr)
            self._collections.update({addr:coll})
        return coll

    def collection(self, name):
        if not name:
            log_err(self, 'failed to get collection')
            raise Exception(log_get(self, 'failed to get collection'))

        addr = self._router.get(name, self._domain)
        if addr:
            self._log('collection, addr=%s, name=%s' % (addr, name))
            return self._get_collection(addr)

    def release(self, collection):
        pass

    def connection(self, coll):
        raise Exception(log_get(self, "no connection"))

    def connect(self, addr):
        raise Exception(log_get(self, "connect is not defined"))

    def get(self, conn, key):
        raise Exception(log_get(self, "get is not defined"))

    def put(self, conn, key, val, create=False):
        raise Exception(log_get(self, "put is not defined"))

    def delete(self, conn, key):
        raise Exception(log_get(self, "delete is not defined"))

    def get_counter(self, conn, key, name):
        raise Exception(log_get(self, "get_counter is not defined"))

    def set_counter(self, conn, key, name, num):
        raise Exception(log_get(self, "set_counter is not defined"))

    def inc_counter(self, conn, key, name):
        raise Exception(log_get(self, "inc_counter is not defined"))
