# bridge.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#


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
