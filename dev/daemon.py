#      daemon.py
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

import zerorpc
from threading import Thread
from conf.virtdev import DAEMON_PORT
from lib.util import cat, zmqaddr, dev_name

NODE_MAX = 8

class DaemonRequest(object):
    def __init__(self, manager):
        self._manager = manager
        self._name = dev_name(manager.uid)
    
    def accept(self, uid, user, node, dest, src):
        if self._manager.guest.accept(dest, src):
            self._manager.member.update({dest:{'uid':uid, 'user':user, 'node':node, 'state':'accept'}})
            self._manager.notify('list', cat(dest, 'accept'))
            return True
    
    def join(self, dest, src):
        if not src:
            src = self._name
        return self._manager.guest.join(dest, src)
    
    def drop(self, dest, src):
        if not src:
            src = self._name
        if self._manager.guest.drop(dest, src):
            self._manager.member.remove(dest)
            return True
    
    def search(self, user):
        res = self._manager.node.search(user, random=True, limit=NODE_MAX)
        if res:
            uid = res['uid']
            node = res['node'][0]
            name = dev_name(uid, node)
            return {'node':node, 'uid':uid, 'name':name}
    
    def find(self, user, node):
        res = self._manager.node.find(user, node)
        if res:
            uid = res['uid']
            name = dev_name(uid, node)
            return {'uid':uid, 'name':name}
    
    def chkaddr(self, name):
        return self._manager.chkaddr(name)
    
    def remove(self, name):
        self._manager.member.remove(name)

    def list(self):
        return self._manager.member.list()

class Daemon(Thread):
    def __init__(self, manager):
        Thread.__init__(self)
        self._request = DaemonRequest(manager)
    
    def run(self):
        srv = zerorpc.Server(self._request)
        srv.bind(zmqaddr('127.0.0.1', DAEMON_PORT))
        srv.run()
