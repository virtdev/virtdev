#      channel.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
#      
#      This program is free software; you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation; either version 2 of the License, or
#      (at your option) any later version.
#      
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#      
#      You should have received a copy of the GNU General Public License
#      along with this program; if not, write to the Free Software
#      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#      MA 02110-1301, USA.

import codec
from pool import Pool
from log import log_err
from queue import Queue
from bridge import get_bridge
from conf.virtdev import DEBUG
from conf.types import TYPE_PROTOCOL
from multiprocessing import cpu_count
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC

CACHE = False
QUEUE_LEN = 2
POOL_SIZE = cpu_count() * 2

pool = None
channel = None

if TYPE_PROTOCOL == PROTOCOL_N2N:
    from protocol.n2n.channel import Channel as N2NChannel
    channel = N2NChannel()
elif TYPE_PROTOCOL == PROTOCOL_WRTC:
    from protocol.wrtc.channel import Channel as WRTCChannel
    channel = WRTCChannel()

class ChannelQueue(Queue):
    def _proc(self, buf):
        put(*buf)
    
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

if CACHE:
    pool = Pool()
    for _ in range(POOL_SIZE):
        pool.add(ChannelQueue(QUEUE_LEN))

def create(uid, addr, key):
    bridge = get_bridge(key)
    return channel.create(addr, key, bridge)

def connect(uid, addr, key, static=False, verify=False):
    bridge = get_bridge(key)
    channel.connect(addr, key, static, verify, bridge)

def disconnect(addr, release=False):
    channel.disconnect(addr, release)

def put(uid, addr, op, args, token):
    req = {'op':op, 'args':args}
    buf = codec.encode(uid, req, token)
    channel.put(addr, buf)

def clean():
    channel.clean()

def push(uid, addr, op, args, token):
    if CACHE:
        pool.push((uid, addr, op, args, token))
    else:
        put(uid, addr, op, args, token)

def exist(addr):
    return channel.exist(addr)

def has_network(addr):
    return channel.has_network(addr)

def initialize():
    channel.initialize()

