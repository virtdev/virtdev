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

import pygame
from PIL import Image
from pygame import camera
from base64 import b64encode
from dev.driver import Driver
from StringIO import StringIO
from lib.modes import MODE_OVP, MODE_SYNC

CAMERA_INDEX = 0
CAMERA_WIDTH = 640
CAMERA_HEIGHT = 480

pygame.init()
pygame.camera.init()

class Camera(Driver):
    def __init__(self, name=None):
        Driver.__init__(self, name=name, mode=MODE_OVP | MODE_SYNC, spec={'content':{'type':'image/jpeg;base64'}})
    
    def setup(self):
        cameras = camera.list_cameras()
        if CAMERA_INDEX >= len(cameras):
            raise Exception('Error: no camera')
        self._cam = camera.Camera(cameras[CAMERA_INDEX], (CAMERA_WIDTH, CAMERA_HEIGHT))
        self._cam.start()
    
    def get(self):
        res = StringIO()
        surf = self._cam.get_image()
        buf = pygame.image.tostring(surf, 'RGBA')
        img = Image.fromstring('RGBA', (CAMERA_WIDTH, CAMERA_HEIGHT), buf)
        img.save(res, 'JPEG')
        return {'content':b64encode(res.getvalue())}
