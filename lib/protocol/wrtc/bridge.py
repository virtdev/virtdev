#      bridge.py (wrtc)
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

import os
from lib.util import call
from threading import Thread
from conf.conf import CONF_MQTT
from conf.virtdev import BRIDGE_PORT

class Bridge(Thread):
    def run(self):
        if CONF_MQTT:
            if not os.path.exists(CONF_MQTT):
                raise Exception('Error: failed to start bridge, cannot find configuration %s' % CONF_MQTT)
            call('mosquitto', '-p', str(BRIDGE_PORT), '-c', CONF_MQTT)
        else:
            call('mosquitto', '-p', str(BRIDGE_PORT))
