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

from os import walk, sep, remove
from os.path import isdir
from json import load
from re import match
from csv import DictWriter
from index.citation.citationsource import CSVFileCitationSource
from index.identifier.doimanager import DOIManager
from index.identifier.metaidmanager import MetaIDManager
from index.citation.oci import Citation
#from meta.scripts.creator import Creator


class CrowdsourcedCitationSource(CSVFileCitationSource):
    def __init__(self, src, local_name=""):
        self.doi = DOIManager()
        self.metaid = MetaIDManager()
        super(CrowdsourcedCitationSource, self).__init__(src, local_name)

    def get_next_citation_data(self):
        row = self._get_next_in_file()

        while row is not None:
            # Citing and Cited may have multiple ids? 
            citing = row.get('citing_id')
            cited = row.get('cited_id')
            if citing is not None and cited is not None:
                citing_list = []
                for id in citing.split(' '):
                    # Normalise the id, whether it is a metaid, a doi or a pmid
                    if 'meta:' in id: # Chec with id_workerk match('^06(.)+0$')
                        citing_list.append(self.metaid.normalise(id))
                    elif 'doi:' in id: #match('^10\..+/.+$')
                        citing_list.append(self.doi.normalise(id))
                    #elif 'pmid:' in id: # To be added
                     #   pass
                cited_list = []
                # For each id in citing, find the type of id and normalise it.
                for id in cited.split(' '):
                    # Normalise the id, whether it is a metaid, a doi or a pmid
                    if 'meta:' in id: #match('^06(.)+0$')
                        cited_list.append(self.metaid.normalise(id))
                    elif 'doi:' in id: #or match('^10\..+/.+$')
                        cited_list.append(self.doi.normalise(id))
                    #elif 'pmid:' in id:
                     #   pass
                    # No id specified. Might come back to this.
                # For now, let's start with dois and then it will be integrated with all ids
                created = row.get("citing_publication_date")
                if not created:
                    created = None # Get from meta + merge

                cited_pub_date = row.get("cited_publication_date")
                if not cited_pub_date:
                    timespan = None # Get from meta + merge
                else:
                    c = Citation(None, None, created, None, cited_pub_date, None, None, None, None, "", None, None, None, None, None)
                    timespan = c.duration
                
                self.update_status_file()
                return citing, cited, created, timespan, None, None

            self.update_status_file()
            row = self._get_next_in_file()

        remove(self.status_file)
