#      modes.py
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

MODE_POLL   = 0x00000001
MODE_TRIG   = 0x00000002
MODE_SYNC   = 0x00000004
MODE_VISI   = 0x00000008
MODE_VIRT   = 0x00000010
MODE_SWITCH = 0x00000020
MODE_IN     = 0x00000040
MODE_OUT    = 0x00000080
MODE_REFLECT= 0x00000100
MODE_LINK   = 0x00000200
MODE_CTRL   = 0x00000400
MODE_CLONE  = 0x00000800
MODE_ACTIVE = 0x00001000

MODE_IV = MODE_IN | MODE_VISI
MODE_OVP = MODE_OUT | MODE_VISI | MODE_POLL
