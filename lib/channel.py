# channel.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib import codec
from lib.pool import Pool
from lib.queue import Queue
from conf.defaults import DEBUG
from lib.log import log_err, log
from conf.log import LOG_CHANNEL
from lib.bridge import get_bridge
from conf.virtdev import PROT_NETWORK
from multiprocessing import cpu_count
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC

SAFE = True
ASYNC = False
QUEUE_LEN = 2
TIMEOUT = 1200 # seconds
POOL_SIZE = cpu_count() * 4

_pool = None
_channel = None

if PROT_NETWORK == PROTOCOL_N2N:
    from protocols.n2n.channel import Channel as N2NChannel
    _channel = N2NChannel()
elif PROT_NETWORK == PROTOCOL_WRTC:
    from protocols.wrtc.channel import Channel as WRTCChannel
    _channel = WRTCChannel()

class ChannelQueue(Queue):
    def __init__(self, parent):
        Queue.__init__(self, parent, QUEUE_LEN, TIMEOUT)

    def _do_proc(self, buf):
        _put(*buf)

    def _proc_safe(self, buf):
        try:
            self._do_proc(buf)
        except:
            log_err(self, 'failed to process')

    def proc(self, buf):
        if DEBUG and not SAFE:
            self._proc(buf)
        else:
            self._proc_safe(buf)

class ChannelPool(Pool):
    def __init__(self):
        Pool.__init__(self)
        for _ in range(POOL_SIZE):
            self.add(ChannelQueue(self))

if ASYNC:
    _pool = ChannelPool()

def _log(text):
    if LOG_CHANNEL:
        log('Channel: %s' % str(text))

def has_network(addr):
    return _channel.has_network(addr)

def create(uid, addr, key):
    bridge = get_bridge(key)
    return _channel.create(addr, key, bridge)

def connect(uid, addr, key, gateway=False):
    bridge = get_bridge(key)
    _channel.connect(addr, key, bridge, gateway)
    _log("connect, addr=%s" % str(addr))

def disconnect(addr, release=False):
    _channel.disconnect(addr, release)
    _log("disconnect, addr=%s" % str(addr))

def send(uid, addr, op, args, token):
    req = {'op':op, 'args':args}
    buf = codec.encode(req, token, uid)
    _channel.send(addr, buf)
    _log("send, addr=%s" % str(addr))

def allocate(uid, addr, key):
    bridge = get_bridge(key)
    _channel.allocate(addr, key, bridge)
    _log("allocate, addr=%s" % str(addr))

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
    _log("put, addr=%s" % str(addr))

def exist(addr):
    ret = _channel.exist(addr)
    _log("exist, addr=%s, ret=%s" % (str(addr), str(ret)))
    return ret
