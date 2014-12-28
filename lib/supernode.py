import os
from threading import Thread
from conf.virtdev import VDEV_SUPERNODE_PORT

class VDevSupernode(Thread):
    def __init__(self):
        Thread.__init__(self)
    
    def start_super(self):
        cmd = 'supernode -l %d>/dev/null' % VDEV_SUPERNODE_PORT
        os.system(cmd)
    
    def run(self):
        self.start_super()