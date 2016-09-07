# imgrec.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver, wrapper
from classify_image import maybe_download_and_extract, recognize

class ImgRec(Driver):
    def setup(self):
        maybe_download_and_extract()
    
    @wrapper
    def put(self, *args, **kwargs):
        image = kwargs.get('content')
        res = recognize(image)
        if res:
            return {'objects':res}
