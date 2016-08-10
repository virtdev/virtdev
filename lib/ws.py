# ws.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

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
