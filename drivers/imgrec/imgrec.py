# imgrec.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver, check_output
from classiify_image import maybe_download_and_extract, recognize

class ImgRec(Driver):
    def setup(self):
        maybe_download_and_extract()
    
    @check_output
    def put(self, args):
        image = args.get('content')
        res = recognize(image)
        if res:
            return {'objects':res}
