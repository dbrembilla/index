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

import encodings
from os import makedirs, walk, sep, remove
import os
from os.path import isdir, exists
from json import load
from re import match
from csv import DictReader,DictWriter
from index.citation.citationsource import CSVFileCitationSource
from index.identifier.doimanager import DOIManager
from index.identifier.metaidmanager import MetaIDManager
from index.citation.oci import Citation
#from meta.scripts.creator import Creator


class CrowdsourcedCitationSource(CSVFileCitationSource):
    def __init__(self, src, local_name="",):
        self.doi = DOIManager()
        self.metaid = MetaIDManager()
        self.meta_path = f'.{sep}tmp{sep}meta'
        if not os.path.isdir(self.meta_path):
            makedirs(self.meta_path)
        super(CrowdsourcedCitationSource, self).__init__(src, local_name)

    def _meta_preprocess(self):
        # If there is already a file with the mapping, take that
        if exists('from_meta'+sep+self.last_file):
            return self.last_file 
        # Else, create a  file to give to meta.
        # This is done by accumulating all valid ids, populating them with additional information
        # and creating a pointer to the row containing info about the id.
        pointers = dict()
        to_meta = []
        with open(self.last_file, encoding = 'utf8') as source:

            for row in DictReader(source):

                ids_citing = []
                ids_cited = []

                # For each id, normalise the id. If one of the rows is all None, skip the row, since the citation is not valid.
                for id in row.get('citing_id').split(' '):

                    if 'doi:' in id:
                        id= self.doi.normalise(id, include_prefix=True)
                        if id is not None:
                            ids_citing.append(id)
                    elif 'meta:' in id:
                        id = self.metaid.normalise(id, include_prefix=True)
                        if id is not None:
                            ids_citing.append(id)

                if len(ids_citing) == 0:
                    continue

                # Repeat the process for cited ids
                for id in row.get('cited_id').split(' '):
                    if 'doi:' in id:
                        id= self.doi.normalise(id, include_prefix=True)
                        if id is not None:
                            ids_cited.append(id)
                    elif 'meta:' in id:
                        id = self.metaid.normalise(id, include_prefix=True)
                        if id is not None:
                            ids_cited.append(id)

                if len(ids_cited) == 0:
                    continue

                creation = row.get('citing_publication_date')
                if not creation:
                    creation = ''

                cited_pub_date = row.get("cited_publication_date")
                if not cited_pub_date:
                    cited_pub_date = ''

                for id in ids_citing:
                    if not id in pointers:
                        pointers[id] = len(to_meta)
                    #get the info!
                    
                    to_meta.append({'id':id,'date':creation})
                
                for id in ids_cited:
                    if not id in pointers:
                        pointers[id] = len(to_meta)
                    to_meta.append({'id':id,'date':cited_pub_date})
                
        with open('tmp%smapping_%s' % (sep,self.file_idx), 'w', encoding='utf8') as meta_input:
            fieldnames = ['id', 'date']
            writer = DictWriter(meta_input, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(to_meta)


            
                
        
        


            

            
        
        

    def _get_next_in_file(self):
        # A file containing citations was already open
        if self.data is not None:
            # Set the next row in the file to consider
            self.last_row += 1
            # There are citations in the open fine that have not
            # being gathered yet
            if self.last_row < self.len:
                return self.data[self.last_row]
            # All the citations of the last file parsed have been
            # gathered, thus a new file must be considered, if available
            elif self.all_files:
                self.last_file = self.all_files.popleft()
                self.file_idx += 1
                print("Opening file '%s' (%s out of %s)" % (self.last_file, self.file_idx, self.file_len))
                #self.last_file = self._meta_preprocess(self, self.last_file)
                self.data, self.len = self.load(self.last_file)
                self.last_row = -1
                return self._get_next_in_file()
            # There are no citation data to consider, since no more
            # files are available
            else:
                self.data = None




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
                return citing, cited, created, timespan, None, None# da sistemare

            self.update_status_file()
            row = self._get_next_in_file()

        remove(self.status_file)
