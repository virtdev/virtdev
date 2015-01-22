#      facerec.py
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

import cv2
import PIL
import numpy
from aop import VDevAnonOper
from StringIO import StringIO
from base64 import decodestring

CLASSIFIER = 'haarcascade_frontalface_default.xml'

class Facerec(VDevAnonOper):
    def __init__(self, index):
        VDevAnonOper(self, index)
        self._classifier = cv2.CascadeClassifier(CLASSIFIER)
    
    def recognize(self, image):
        buf = decodestring(image)
        if buf:
            f = StringIO(buf)
            src = PIL.Image.open(f).convert('RGB')
            image = numpy.array(src)
            image = image[:, :, ::-1].copy()
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            faces = self._classifier.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30), flags = cv2.cv.CV_HAAR_SCALE_IMAGE)
            if faces:
                return True
    
    def put(self, buf):
        args = self._get_args(buf)
        if args and type(args) == dict:
            image = args.get('Image')
            if self.recognize(image):
                return {'Enable':'True'}
    