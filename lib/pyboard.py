# pyboard.py
#
# Copyright (C) 2013, 2014 Damien P. George
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import time

RETRY_MAX = 3
SLEEP_TIME = 0.1 # seconds

class Pyboard(object):
    def __init__(self, sock):
        self._socket = sock

    def _read(self, length, ending=None):
        buf = self._socket.recv(length)
        if not ending:
            return buf
        count = 0
        while count < RETRY_MAX:
            if buf.endswith(ending):
                break
            val = self._socket.recv(1)
            if not val:
                time.sleep(SLEEP_TIME)
                count += 1
            else:
                buf += val
                count = 0
        return buf

    def _write(self, buf):
        self._socket.sendall(buf)

    def _enter(self):
        self._write(b'\r\x03\x03') # ctrl-C twice: interrupt any running program
        self._write(b'\r\x01') # ctrl-A: enter raw REPL
        buf = self._read(1, b'to exit\r\n>')
        if not buf.endswith(b'raw REPL; CTRL-B to exit\r\n>'):
            raise Exception('could not enter raw repl')

    def _exit(self):
        self._write(b'\r\x02') # ctrl-B: enter friendly REPL

    def _follow(self):
        # wait for normal output
        buf = self._read(1, b'\x04')
        if not buf.endswith(b'\x04'):
            raise Exception('timeout waiting for first EOF reception')
        buf = buf[:-1]

        # wait for error output
        err = self._read(2, b'\x04>')
        if not err.endswith(b'\x04>'):
            raise Exception('timeout waiting for second EOF reception')
        err = err[:-2]

        # return normal and error output
        return buf, err

    def _exec(self, buf):
        if not isinstance(buf, bytes):
            buf = bytes(buf, encoding='utf8')

        # write command
        for i in range(0, len(buf), 256):
            self._write(buf[i:min(i + 256, len(buf))])
            time.sleep(0.01)
        self._write(b'\x04')

        # check if we could exec command
        if self._read(2) != b'OK':
            raise Exception('could not exec')

        ret, err = self._follow()
        if err:
            raise Exception('could not exec, err=%s' % str(err))
        return ret

    def enter(self, buf):
        self._enter()
        ret = self._exec(buf)
        self._exit()
        return ret

if __name__ == '__main__':
    import sys
    from usb import USBSocket

    sock = USBSocket('/dev/ttyACM0')
    pyb = Pyboard(sock)

    argc = len(sys.argv)
    if argc == 2:
        cmd = sys.argv[1]
    elif argc == 1:
        cmd = "import time\r\npyb.LED(1).on()\r\ntime.sleep(1)\r\npyb.LED(1).off()"
    ret = pyb.enter(cmd)
    print("ret=%s" % str(ret))
