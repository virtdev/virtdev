#      ws.py
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

import tornado.web
import tornado.ioloop
import tornado.websocket
import tornado.httpserver
from threading import Lock, Thread
from websocket import create_connection

_ioloop_lock = Lock()
_ioloop_active = False
ws_connect = create_connection

def _start_ioloop():
    global _ioloop_active
    _ioloop_lock.acquire()
    try:
        if not _ioloop_active:
            tornado.ioloop.IOLoop.instance().start()
            _ioloop_active = True
    finally:
        _ioloop_lock.release()

def _ws_start(handler, port, addr, path):
    app = tornado.web.Application([(path, handler)], debug=False)
    server = tornado.httpserver.HTTPServer(app)
    if addr:
        server.listen(port, addr)
    else:
        server.listen(port)
    _start_ioloop()

def ws_start(handler, port, addr=None, path=r'/'):
    Thread(target=_ws_start, args=(handler, port, addr, path)).start()

def ws_addr(addr, port):
    return "ws://%s:%d" % (addr, port)

class WSHandler(tornado.websocket.WebSocketHandler):
    pass
