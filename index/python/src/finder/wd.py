#!python
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

from requests import get
from datetime import datetime
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.parse import quote
from oc.index.finder.base import ApiDOIResourceFinder

import oc.index.utils.dictionary as dict_utils

class WikidataResourceFinder(ApiDOIResourceFinder):
    '''This class allows for querying Wikidata'''

    def __init__(self, data={}, use_api_service=True, api_key=None, queries = dict()):
        super().__init__(data, use_api_service)
        self.api = "https://query.wikidata.org/sparql"
        self._headers = 'ResourceFinder / OpenCitations Indexes - (http://opencitations.net; mailto:contact@opencitations.net)'
        self.sparql = SPARQLWrapper(self.api , agent= self._headers)
        self.valid_queries = dict()
        for el in queries:
            self.valid_queries[el] = queries[el]        
    
    def _call_api(self, to_search, value):
        query = self.valid_queries[to_search].format(value=value)
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON) 
        response = self.sparql.query().convert()
        result = dict()
        for el in response['results']['bindings']:
            for variable in el:
                result[variable] = el[variable]['value']
        return result
