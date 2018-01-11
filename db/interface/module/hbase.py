# hbase.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from happybase import ConnectionPool
from multiprocessing import cpu_count

POOL_SIZE = cpu_count() * 2

def chkconn(func):
    def _chkconn(*args, **kwargs):
        self = args[0]
        conn = args[1]
        table = conn.table(self._name)
        return func(self, table, *args[2:], **kwargs)
    return _chkconn

class HBase(object):
    def __init__(self, name):
        self._name = name

    def connect(self, addr):
        return ConnectionPool(size=POOL_SIZE, host=addr)

    def connection(self, coll):
        return coll.connection()

    @chkconn
    def get(self, conn, key):
        return conn.row(key)

    @chkconn
    def put(self, conn, key, val, create=False):
        conn.put(key, val)

    @chkconn
    def get_counter(self, conn, key, name):
        return conn.counter_get(key, name)

    @chkconn
    def set_counter(self, conn, key, name, num):
        conn.counter_set(key, name, num)

    @chkconn
    def inc_counter(self, conn, key, name):
        conn.counter_inc(key, name)
