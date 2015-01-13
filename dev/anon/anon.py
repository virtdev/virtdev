#      anon.py
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

from lib import stream
from lib.log import log_err
from threading import Thread, Lock, Event
from dev.req import VDEV_REQ_OPEN, VDEV_REQ_CLOSE, VDEV_REQ_GET, VDEV_REQ_PUT, VDEV_REQ_SECRET, VDEV_REQ_PAIR, parse

def excl(func):
    def _excl(*args, **kwargs):
        self = args[0]
        self._lock.acquire()
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release()
    return _excl

def anon_index(name):
    res = str(name).split('_')
    if len(res) == 2:
        return (res[0], int(res[1]))
    else:
        return (None, None)

class VDevAnon(Thread):
    def __init__(self, dev, sock):
        self._buf = []
        self._dev = dev
        self._sock = sock
        self._lock = Lock()
        self._event = Event()
        Thread.__init__(self)
        self.start()
    
    def __str__(self):
        return str(self._dev)
    
    @excl
    def _reply(self, buf):
        if not buf:
            buf = ''
        return stream.put(self._sock, str(buf), anon=True)
    
    @excl
    def _push(self, buf):
        if type(buf) != dict or buf.has_key('_i'):
            return
        self._buf.append(buf)
        self._event.set()
    
    @excl
    def _pop(self):
        if self._buf:
            return self._buf.pop(0)
        else:
            self._event.clear()
    
    def _pair(self, buf):
        if buf == VDEV_REQ_SECRET:
            name = str(self._dev)
            d_type, _ = anon_index(name)
            stream.put(self._sock, str({'0':d_type}), anon=True)
    
    def _proc(self):
        while True:
            self._event.wait()
            while True:
                buf = self._pop()
                if buf:
                    self._reply(buf)
                else:
                    break
    
    def run(self):
        Thread(target=self._proc).start()
        while True:
            try:
                res = ''
                _, flags, buf = parse(stream.get(self._sock, anon=True))
                if flags & VDEV_REQ_PAIR:
                    self._pair(buf)
                elif flags & VDEV_REQ_OPEN:
                    ret = self._dev.open()
                    self._push(ret)
                elif flags & VDEV_REQ_CLOSE:
                    ret = self._dev.close()
                    self._push(ret)
                elif flags & VDEV_REQ_GET:
                    try:
                        ret = self._dev.get()
                        if ret:
                            res = ret
                    finally:
                        self._reply(res)
                elif flags & VDEV_REQ_PUT:
                    try:
                        ret = self._dev.put(buf)
                        if ret:
                            res = ret
                    finally:
                        self._reply(ret)
            except:
                log_err(self, 'failed to handle')
