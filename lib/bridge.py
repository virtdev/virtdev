#      bridge.py
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

from hash_ring import HashRing
from conf.prot import PROT_NETWORK
from conf.virtdev import BRIDGE_SERVERS
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC

bridge = None

def get_bridge(key):
    ring = HashRing(BRIDGE_SERVERS)
    return ring.get_node(key)

if PROT_NETWORK == PROTOCOL_N2N:
    from protocol.n2n.bridge import Bridge as N2NBridge
    bridge = N2NBridge()
elif PROT_NETWORK == PROTOCOL_WRTC:
    from protocol.wrtc.bridge import Bridge as WRTCBridge
    bridge = WRTCBridge()
