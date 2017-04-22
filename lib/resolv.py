# resolv.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from conf.prot import PROT_NETWORK
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC

resolv = None

if PROT_NETWORK == PROTOCOL_N2N:
	from protocol.n2n.resolv import Resolv as N2NResolv
	resolv = N2NResolv()
elif PROT_NETWORK == PROTOCOL_WRTC:
	from protocol.wrtc.resolv import Resolv as WRTCResolv
	resolv = WRTCResolv()

def get_addr(uid=None, node=None):
	return resolv.get_addr(uid, node)
