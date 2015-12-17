#      resolv.py
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

from conf.virtdev import PROTOCOL
from protocols import PROTOCOL_N2N, PROTOCOL_WRTC
from protocol.n2n.resolv import Resolv as N2NResolv
from protocol.wrtc.resolv import Resolv as WRTCResolv

resolv = {PROTOCOL_N2N:N2NResolv(), PROTOCOL_WRTC:WRTCResolv()}

def get_addr(uid, node, networks=None, protocol=PROTOCOL):
    return resolv[protocol].get_addr(uid, node, networks)
