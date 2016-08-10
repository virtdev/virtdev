# facerec.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import cv2
import numpy
from PIL import Image
from base64 import b64decode
from StringIO import StringIO
from dev.driver import Driver, check_output

PRINT = False
RESIZE = False
CASCADE = '/usr/share/opencv/haarcascades/haarcascade_frontalface_default.xml'

class FaceRec(Driver):
    def setup(self):
        self._cascade = cv2.CascadeClassifier(CASCADE)
    
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
