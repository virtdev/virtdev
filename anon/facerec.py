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
from cv2 import cv
from lib.log import log_err
from dev.anon import VDevAnon
from base64 import decodestring

PATH_CASCADE = '/usr/share/opencv/haarcascades/haarcascade_frontalface_default.xml'

class FaceRec(VDevAnon):
    def __init__(self, name=None, sock=None):
        VDevAnon.__init__(self, name, sock)
        self._cascade = cv.Load(PATH_CASCADE)
    
    def recognize(self, image):
        try:
            buf = decodestring(image)
            if buf:
                array = numpy.fromstring(buf, numpy.uint8)
                source = cv2.imdecode(array, cv2.CV_LOAD_IMAGE_COLOR)
                size = (source.shape[1], source.shape[0])
                bitmap = cv.CreateImageHeader(size, cv.IPL_DEPTH_8U, 3)
                cv.SetData(bitmap, source.tostring(), source.dtype.itemsize * 3 * source.shape[1])
                gray = cv.CreateImage(size, 8, 1)
                cv.CvtColor(bitmap, gray, cv.CV_BGR2GRAY)
                cv.EqualizeHist(gray, gray)
                storage = cv.CreateMemStorage(0)
                faces = cv.HaarDetectObjects(image=gray, cascade=self._cascade, storage=storage, scale_factor=1.2, min_neighbors=2, flags=cv.CV_HAAR_DO_CANNY_PRUNING)
                if len(faces) > 0:
                    return True
        except:
            log_err(self, 'failed to recognize')
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            image = args.get('File')
            if self.recognize(image):
                ret = {'Enable':'True'}
            else:
                ret = {'Enable': 'False'}
            name = args.get('Name')
            if name:
                ret.update({'Name':name})
            timer = args.get('Timer')
            if timer:
                ret.update({'Timer':timer})
            return ret
