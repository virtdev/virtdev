# channel.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import codec
from pool import Pool
from log import log_err
from queue import Queue
from bridge import get_bridge
from conf.defaults import DEBUG
from conf.prot import PROT_NETWORK
from multiprocessing import cpu_count
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC

ASYNC = False
QUEUE_LEN = 2
POOL_SIZE = cpu_count() * 4

_pool = None
_channel = None

if PROT_NETWORK == PROTOCOL_N2N:
    from protocol.n2n.channel import Channel as N2NChannel
    _channel = N2NChannel()
elif PROT_NETWORK == PROTOCOL_WRTC:
    from protocol.wrtc.channel import Channel as WRTCChannel
    _channel = WRTCChannel()

class ChannelQueue(Queue):
    def _proc(self, buf):
        _put(*buf)
    
    def _proc_safe(self, buf):
        try:
            self._proc(buf)
        except:
            log_err(self, 'failed to process')
    
    def proc(self, buf):
        if DEBUG:
            self._proc(buf)
        else:
            self._proc_safe(buf)

if ASYNC:
    _pool = Pool()
    for _ in range(POOL_SIZE):
        _pool.add(ChannelQueue(QUEUE_LEN))

def has_network(addr):
    return _channel.has_network(addr)

def initialize():
    _channel.initialize()

def clean():
    _channel.clean()

def create(uid, addr, key):
    bridge = get_bridge(key)
    return _channel.create(addr, key, bridge)

def connect(uid, addr, key, gateway=False):
    bridge = get_bridge(key)
    _channel.connect(addr, key, bridge, gateway)

def disconnect(addr, release=False):
    _channel.disconnect(addr, release)

def send(uid, addr, op, args, token):
    req = {'op':op, 'args':args}
    buf = codec.encode(req, token, uid)
    _channel.send(addr, buf)

def allocate(uid, addr, key):
    bridge = get_bridge(key)
    _channel.allocate(addr, key, bridge)

def free(addr):
    _channel.free(addr)

def _put(uid, addr, op, args, token):
    req = {'op':op, 'args':args}
    buf = codec.encode(req, token, uid)
    _channel.put(addr, buf)

def put(uid, addr, op, args, token):
    if ASYNC:
        _pool.push((uid, addr, op, args, token))
    else:
        _put(uid, addr, op, args, token)

def exist(addr):
    return _channel.exist(addr)
