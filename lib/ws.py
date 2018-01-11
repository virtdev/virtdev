# ws.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import tornado.web
import tornado.ioloop
import tornado.websocket
from threading import Lock, Thread
from websocket import create_connection

_servers = []
_lock = Lock()

ws_connect = create_connection

def _ws_create(handler, port, addr, path):
    app = tornado.web.Application([(path, handler)], debug=False)
    if addr:
        return app.listen(port, addr)
    else:
        return app.listen(port)

def ws_start(handler, port, addr=None, path=r'/'):
    srv = _ws_create(handler, port, addr, path)
    _lock.acquire()
    try:
        _servers.append(srv)
        if len(_servers) == 1:
            Thread(target=tornado.ioloop.IOLoop.current().start).start()
    finally:
        _lock.release()

def ws_addr(addr, port):
    return "ws://%s:%d" % (addr, port)

class WSHandler(tornado.websocket.WebSocketHandler):
    pass
