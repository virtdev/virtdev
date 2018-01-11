# bridge.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from hash_ring import HashRing
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC
from conf.virtdev import PROT_NETWORK, BRIDGE_SERVERS

def get_bridge(key):
    ring = HashRing(BRIDGE_SERVERS)
    return ring.get_node(key)

if PROT_NETWORK == PROTOCOL_N2N:
    from protocols.n2n.bridge import Bridge as N2NBridge
    class Bridge(N2NBridge):
        pass
elif PROT_NETWORK == PROTOCOL_WRTC:
    from protocols.wrtc.bridge import Bridge as WRTCBridge
    class Bridge(WRTCBridge):
        pass
