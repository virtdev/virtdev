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
import numpy
from PIL import Image
from base64 import b64decode
from StringIO import StringIO
from dev.driver import Driver, check_output

PRINT = False
RESIZE = False
PATH_CASCADE = '/usr/share/opencv/haarcascades/haarcascade_frontalface_default.xml'

class FaceRec(Driver):
    def setup(self):
        self._cascade = cv2.CascadeClassifier(PATH_CASCADE)
    
    def _recognize(self, image):
        try:
            buf = b64decode(image)
            if buf:
                f = StringIO(buf)
                img = Image.open(f)
                src = img.convert('RGB')
                array = numpy.array(src)
                image = array[:, :, ::-1].copy()
                if RESIZE:
                    image = cv2.resize(image, (img.size[0] / 2, img.size[1] / 2))
                gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
                gray = cv2.equalizeHist(gray)
                faces = self._cascade.detectMultiScale(gray, scaleFactor=1.2, minNeighbors=2, minSize=(100, 100), flags=cv2.cv.CV_HAAR_DO_ROUGH_SEARCH)
                if len(faces) > 0:
                    return True
        except:
            if PRINT:
                print('FaceRec: failed to recognize')
    
    @check_output
    def put(self, args):
        image = args.get('content')
        if self._recognize(image):
            return {'enable':'true'}
        else:
            return {'enable': 'false'}
