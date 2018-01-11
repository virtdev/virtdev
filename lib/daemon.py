# daemon.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zerorpc
from json import dumps
from threading import Thread
from conf.defaults import DAEMON_PORT
from lib.util import zmqaddr, get_name, get_node

class VDevDaemonRequest(object):
    def __init__(self, manager):
        self._manager = manager
        self._name = get_name(manager.uid, get_node())

    def accept(self, user, node, name):
        if name != self._name:
            if self._manager.guest.accept(name, self._name):
                self._manager.member.update({name:{'user':user, 'node':node, 'state':'accept'}})
                self._manager.notify('list', dumps({'name':name, 'state':'accept'}))
                return True

    def join(self, name):
        if name != self._name:
            return self._manager.guest.join(name, self._name)

    def drop(self, name):
        if name != self._name:
            if self._manager.guest.drop(name, self._name):
                self._manager.member.delete(name)
                return True

    def find(self, user, node):
        res = self._manager.node.find(user, node)
        if res:
            uid = res['uid']
            name = get_name(uid, node)
            return {'name':name}

    def remove(self, name):
        self._manager.member.delete(name)

    def list(self):
        return self._manager.member.list()

    def chknode(self):
        return self._manager.chknode()

    def chkaddr(self, name):
        return self._manager.chkaddr(name)

class VDevDaemon(Thread):
    def __init__(self, manager):
        Thread.__init__(self)
        self._request = VDevDaemonRequest(manager)

    def run(self):
        srv = zerorpc.Server(self._request)
        srv.bind(zmqaddr('127.0.0.1', DAEMON_PORT))
        srv.run()
