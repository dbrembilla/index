#!python
# -*- coding: utf-8 -*-
# Copyright (c) 2022 The OpenCitations Index Authors.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
from re import sub
from urllib.parse import unquote, quote
from requests import get
from json import loads
from requests import ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep

from oc.index.identifier.base import IdentifierManager

class MetaIDManager(IdentifierManager):
    def __init__(self, data = {}, use_api_service=False): #da mettere True per il futuro

        self.p = "meta:br/"
        self.use_api_service=use_api_service
        #self.metaid_uri = "https://w3id.org/oc/meta/br/060" #test from the suffix
        super(MetaIDManager, self).__init__()

    def set_valid(self, id_string):
        metaid = self.normalise(id_string, include_prefix=True)

        if self.valid_metaid.get_value(metaid) is None:
            self.valid_metaid.add_value(metaid, "v")

    def is_valid(self, id_string): #  verifies if is valid and is in the list of valid metaids
        metaid = self.normalise(id_string, include_prefix=True)
        if metaid is None:

            return False
        else: #to be added once i clarify for the existence
            if self.valid_metaid.get_value(metaid) is None:
                if self.__metaid_exists(metaid):
                    self.valid_metaid.add_value(metaid, "v")
                else:
                    self.valid_metaid.add_value(metaid, "i")

            return "v" in self.valid_metaid.get_value(metaid) 

    def normalise(self, id_string, include_prefix=False): # Returns MetaID itself without the prefix
        try: # QUA VEDERE BISOGNA UNMATCHARE SE NON INIZIA CON 06< e termina con 0
            metaid_string = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("06"):])))
            return "%s%s" % (self.p if include_prefix else "", metaid_string.lower().strip())
        except:  # Any error in processing the MetaID will return None
            return None


