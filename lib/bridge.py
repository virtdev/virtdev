# bridge.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from hash_ring import HashRing
from conf.prot import PROT_NETWORK
from conf.virtdev import BRIDGE_SERVERS
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC

def get_bridge(key):
	ring = HashRing(BRIDGE_SERVERS)
	return ring.get_node(key)

if PROT_NETWORK == PROTOCOL_N2N:
	from protocol.n2n.bridge import Bridge as N2NBridge
	class Bridge(N2NBridge):
		pass
elif PROT_NETWORK == PROTOCOL_WRTC:
	from protocol.wrtc.bridge import Bridge as WRTCBridge
	class Bridge(WRTCBridge):
		pass
