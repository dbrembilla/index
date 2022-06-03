#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
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

from oc_graphenricher.oc_graphenricher.APIs import QueryInterface

class CrossrefDoi(QueryInterface):
    '''This class allows to query Crossref with just the DOI'''
    def __init__(self,
                 crossref_min_similarity_score=0.95,
                 max_iteration=6,
                 sec_to_wait=10,
                 headers={"User-Agent": "GraphEnricher (via OpenCitations - http://opencitations.net; "
                                        "mailto:contact@opencitations.net)"},
                 timeout=30,
                 is_json=True):
        self.max_iteration = max_iteration
        self.sec_to_wait = sec_to_wait
        self.headers = headers
        self.timeout = timeout
        self.is_json = is_json
        self.crossref_min_similarity_score = crossref_min_similarity_score
        self.__crossref_doi_url = 'https://api.crossref.org/works/'
        super().__init__()

        def query(self, doi):
            '''
            This methods extracts information about a publication given a DOI.
            '''
            url_cr = self.__crossref_doi_url + doi
            try:
                r_cr = requests.get(url_cr, headers=self.headers, timeout=60)
                
