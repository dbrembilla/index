#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Silvio Peroni <essepuntato@gmail.com>
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
from re import sub, match
from urllib.parse import unquote, quote
from requests import get
from json import loads
from requests import ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep
import os
from SPARQLWrapper import SPARQLWrapper, JSON
from oc.index.identifier.base import IdentifierManager
    
class WikiDataIDManager(IdentifierManager):
    '''This class is used to validate WikiData Identifiers. This is done through an ASK query to the WikiData SPARQL Endpoint.'''
    def __init__(self, valid_wdid=None, use_api_service=True):
        super().__init__()
        self.wd_sparql_endpoint = "https://query.wikidata.org/sparql"
        self.headers = "OpenCitations - http://opencitations.net;  mailto:contact@opencitations.net"# chi metto?
        self.sparql = SPARQLWrapper(self.wd_sparql_endpoint , agent= self.headers)
        self.valid_wdid = valid_wdid
        self.use_api_service = use_api_service
        self.p = "wikidata:"

    def is_valid(self, id_string):
        wdid = self.normalise(id_string)

        if wdid is None:
            return False
        else:
            if not wdid in self._data or self._data[wdid] is None:
                return self.__wdid_exists(wdid)
            return self._data[wdid].get("valid")

    def normalise(self, id_string, include_prefix=False):
        try:
            wdid_string = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("Q"):])))
            return "%s%s" % (self.p if include_prefix else "", wdid_string.strip())
        except Exception as e:  # Any error in processing the WikiData ID will return None
            return None

    def __wdid_exists(self, wdid_full):
        if self.use_api_service:
            wdid = self.normalise(wdid_full)
            tentative = 3
            while tentative:
                tentative -= 1
                try:
                    query = """ASK { VALUES ?type {wd:Q13442814 wd:Q571 wd:Q88392887 wd:Q1143604 wd:Q7318358}
                    %s wdt:P31 ?type }
                    """ % wdid
                    self.sparql.setQuery(query)
                    self.sparql.setReturnFormat(JSON)
                    return self.sparql.query().convert()
                except ReadTimeout:
                    pass  # Do nothing, just try again
                except ConnectionError:
                    sleep(5)  # Sleep 5 seconds, then try again

        return False