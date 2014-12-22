#      camera.py
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

import time
import pygame
from PIL import Image
from pygame import camera
from aop import VDevAnonOper
from StringIO import StringIO
from base64 import encodestring
from lib.log import log_get, log_err

CAMERA_WIDTH = 640
CAMERA_HEIGHT = 480

_camera_init = False

SLEEP_TIME = 6 # seconds
RETRY_TIMES = 2

if not _camera_init:
    _camera_init = True
    pygame.init()
    pygame.camera.init()

class Camera(VDevAnonOper):
    def __init__(self, identity):
        self._cam = None
        self._identity = identity
        cameras = camera.list_cameras()
        if identity >= len(cameras):
            log_err(self, 'no camera')
            raise Exception(log_get(self, 'no camera'))
        self._cam = camera.Camera(cameras[identity], (CAMERA_WIDTH, CAMERA_HEIGHT))
        self._start()
    
    def __str__(self):
        return 'CAM_%d' % self._identity
    
    def _start(self):
        self._cam.start()
        time.sleep(SLEEP_TIME)
        for _ in range(RETRY_TIMES):    
            self.get()
    
    def get(self):
        if not self._cam:
            return
        res = StringIO()
        surf = self._cam.get_image()
        buf = pygame.image.tostring(surf, 'RGBA')
        img = Image.fromstring('RGBA', (CAMERA_WIDTH, CAMERA_HEIGHT), buf)
        img.save(res, 'JPEG')
        return {'Image':encodestring(res.getvalue())}

if __name__ == '__main__':
    cam = Camera(0)
    ret = cam.get()
    if ret:
        with open('/tmp/cam0.jpg', 'w') as f:
            f.write(ret)
    