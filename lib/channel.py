#      channel.py
#      
#      Copyright (C) 2014 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from queue import Queue
from hash_ring import HashRing
from conf.virtdev import BRIDGE_SERVERS, PROTOCOL
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC
from protocol.n2n.channel import Channel as N2NChannel
from protocol.wrtc.channel import Channel as WRTCChannel 

QUEUE_LEN = 2
POOL_SIZE = 0

def get_bridge(key):
    ring = HashRing(BRIDGE_SERVERS)
    return ring.get_node(key)

class ChannelQueue(Queue):
    def __init__(self):
        Queue.__init__(self, QUEUE_LEN)
        
    def proc(self, buf):
        put(*buf)

class ChannelPool(object):
    def __init__(self):
        self._pool = Pool()
        for _ in range(POOL_SIZE):
            self._pool.add(ChannelQueue())
    
    def push(self, buf):
        self._pool.push(buf)

channels = {PROTOCOL_N2N:N2NChannel(), PROTOCOL_WRTC:WRTCChannel()}

if POOL_SIZE > 0:
    pool = ChannelPool()
else:
    pool = None

def create(uid, addr, key, protocol=PROTOCOL):
    bridge = get_bridge(uid)
    return channels[protocol].create(addr, key, bridge)

def connect(uid, addr, key, static=False, verify=False, protocol=PROTOCOL):
    bridge = get_bridge(uid)
    channels[protocol].connect(addr, key, static, verify, bridge)

def disconnect(addr, release=False, protocol=PROTOCOL):
    channels[protocol].disconnect(addr, release)

def put(uid, addr, op, args, token, protocol=PROTOCOL):
    req = {'op':op, 'args':args}
    buf = codec.encode(uid, req, token)
    channels[protocol].put(addr, buf)

def clean():
    for i in channels:
        channels[i].clean()

def push(uid, addr, op, args, token):
    if POOL_SIZE:
        pool.push((uid, addr, op, args, token))
    else:
        put(uid, addr, op, args, token)
        
def exist(self, addr, protocol=PROTOCOL):
    return channels[protocol].exist(addr)
