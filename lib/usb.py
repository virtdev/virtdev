# usb.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import time
import serial

RATE = 9600
WAIT_TIME = 2 # seconds
TIMEOUT = 0.5 # seconds

class USBSocket(object):
    def __init__(self, name, rate=RATE, timeout=TIMEOUT):
        self._serial = serial.Serial(name, rate, timeout=timeout)
        time.sleep(WAIT_TIME)

    def sendall(self, buf):
        if self._serial:
            self._serial.write(buf)

    def send(self, buf):
        if self._serial:
            self._serial.write(buf)

    def recv(self, length):
        if self._serial:
            return self._serial.read(length)

    def close(self):
        try:
            if self._serial:
                self._serial.close()
                self._serial = None
        except:
            pass

    def __del__(self):
        self.close()
