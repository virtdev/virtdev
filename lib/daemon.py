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
from conf.virtdev import VDEV_DAEMON_PORT
from lib.util import cat, zmqaddr, vdev_name

NODE_MAX = 8

class VDevUserInterface(object):
    def __init__(self, interface):
        self._interface = interface
        name = vdev_name(interface.uid)
        self._interface.device.add(name)
        self._name = name
    
    def accept(self, uid, user, node, dest, src):
        if self._interface.guest.accept(dest, src):
            self._interface.member.update({dest:{'uid':uid, 'user':user, 'node':node, 'state':'accept'}})
            self._interface.notify('list', cat(dest, 'accept'))
            return True
    
    def join(self, dest, src):
        if not src:
            src = self._name
        return self._interface.guest.join(dest, src)
    
    def drop(self, dest, src):
        if not src:
            src = self._name
        if self._interface.guest.drop(dest, src):
            self._interface.member.remove(dest)
            return True
    
    def search(self, user):
        res = self._interface.node.search(user, random=True, limit=NODE_MAX)
        if res:
            uid = res['uid']
            node = res['node'][0]
            name = vdev_name(uid, node)
            return {'node':node, 'uid':uid, 'name':name}
    
    def find(self, user, node):
        res = self._interface.node.find(user, node)
        if res:
            uid = res['uid']
            name = vdev_name(uid, node)
            return {'uid':uid, 'name':name}
    
    def chkaddr(self, name):
        return self._interface.chkaddr(name)
    
    def remove(self, name):
        self._interface.member.remove(name)

    def list(self):
        return self._interface.member.list()

class VDevDaemon(Thread):
    def __init__(self, interface):
        Thread.__init__(self)
        self._interface = VDevUserInterface(interface)
    
    def run(self):
        srv = zerorpc.Server(self._interface)
        srv.bind(zmqaddr('127.0.0.1', VDEV_DAEMON_PORT))
        srv.run()
