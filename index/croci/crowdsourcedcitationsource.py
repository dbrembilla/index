#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright (c) 2022, Davide Brembilla <davide.brembilla98@gmail.com>
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
from oc_meta.run.meta_process import MetaProcess, run_meta_process
from index.croci import populator
from oc_meta.lib.file_manager import get_data
import shutil
import subprocess
import time
from collections import deque

class CrowdsourcedCitationSource(CSVFileCitationSource):
    def __init__(self, src, local_name="",):
        self.populator = populator.Populator()
        self.doi = DOIManager()
        self.metaid = MetaIDManager()
        self.pointer_path = '..%scroci_tmp%spointers%sid_pointers.csv' % (sep, sep, sep)
        self.meta_config = '..%smeta_config.yaml'
        self.latest_meta=[]
        self.last_file = None
        self.last_row = None
        self.data = None
        self.len = None
        self.status_file = None
        self.all_files = []
        self.file_idx = 0
        self.file_len = 0
        self.meta_process = MetaProcess(config=os.path.join('..', 'meta_config.yaml'))
        self.meta_process.workers_number = 3
        if isinstance(src, (list, set, tuple)):
            src_collection = src
        else:
            src_collection = [src]

        for src in sorted(src_collection):
            cur_dir = src
            if not isdir(cur_dir):
                cur_dir = os.path.dirname(cur_dir)

            if self.status_file is None:
                self.status_file = cur_dir + sep + ".dir_citation_source" + local_name

            if exists(self.status_file):
                with open(self.status_file, encoding="utf8") as f:
                    row = next(DictReader(f))
                    self.last_file = row["file"] if row["file"] else None
                    self.last_row = int(row["line"]) if row["line"] else None

            if isdir(src):
                for cur_dir, cur_subdir, cur_files in walk(src):
                    for cur_file in cur_files:
                        full_path = cur_dir + sep + cur_file
                        if self.select_file(full_path):
                            self.all_files.append(full_path)
            elif self.select_file(src):
                self.all_files.append(src)

        self.all_files.sort()
        self.file_len = len(self.all_files)

        self.all_files = deque(self.all_files)
        # If case the directory contains some files
        if self.all_files:
            # If there is no status file (i.e. '.dir_citation_source') available,
            # the we have to set the current last file as the first of the list of files
            if self.last_file is None:
                self.last_file = self.all_files.popleft()
            # In case the status file is available (i.e. '.dir_citation_source') and
            # it already specifies the last file that have been processed,
            # we need to exclude all the files in the list that are lesser than the
            # last one considered
            else:
                tmp_file = self.all_files.popleft()
                while tmp_file < self.last_file:
                    tmp_file = self.all_files.popleft()
        if self.last_file is not None:
            self.file_idx += 1
            print("Opening file '%s' (%s out of %s)" % (self.last_file, self.file_idx, self.file_len))
            self._meta_preprocess(self.last_file)
            self.data, self.len = self.load(self.pointer_path)
        
        if self.last_row is None:
            self.last_row = -1


    def _meta_preprocess(self, last_file):
        '''This function preprocesses the file and gets the output from meta.
        To launch this, it needs a meta_config.yaml file containing the specifications.'''

        with open(last_file, 'r', encoding = 'utf8') as source:
            for row in DictReader(source):
                self.populator.run(row)
        output_folder = os.path.join('..', 'output')
        start = time.time()
        run_meta_process(self.meta_process)
        print('TIME: %s' % (time.time() - start))
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'csv')):
            for file in filenames:
                self.latest_meta.extend(get_data(os.path.join(dirpath, file)))
        

    def _get_next_in_file(self): #TODO: trasforma perché passi la lunghezza dei pointer
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
                self.populator.clean_dir()
                self.file_idx += 1

                print("Opening file '%s' (%s out of %s)" % (self.last_file, self.file_idx, self.file_len))
                self._meta_preprocess(self.last_file)
                
                self.data, self.len = self.load(self.pointer_path) #instead of loading the last file, it needs to load the pointers file
                
                self.last_row = -1
                return self._get_next_in_file()
            # There are no citation data to consider, since no more
            # files are available
            else:
                self.data = None




    def get_next_citation_data(self): # TODO: trasforma per prendere passo per passo i dati citazionali dai dati di meta
        row = self._get_next_in_file()
        
        while row is not None:
            # There is no possibility that citing or cited are None, since they are preprocessed. 
            idx_citing = int(row.get('citing_id'))
            citing = self.latest_meta[idx_citing]['id']
            created = self.latest_meta[idx_citing]['pub_date']
            # We get the metaid
            citing = citing.split('meta:')[1]
            if '\s' in citing:
                citing = citing.split('\s')[0]

            idx_cited = int(row.get('cited_id'))
            cited = self.latest_meta[idx_cited]['id']
            cited_pub_date = self.latest_meta[idx_cited]['pub_date']
            cited = cited.split('meta:')[1]
            if '\s' in cited:
                cited = cited.split('\s')[0]

            if not created:
                created = None

            cited_pub_date = row.get("cited_publication_date")
            if not cited_pub_date:
                timespan = None

            else:
                c = Citation(None, None, created, None, cited_pub_date, None, None, None, None, "", None, None, None, None, None)
                timespan = c.duration
            
            self.update_status_file()
            return citing, cited, created, timespan, None, None# da sistemare

        self.update_status_file()
        row = self._get_next_in_file()
        remove(self.status_file)

if __name__ == '__main__':
    input_file = "..%ssrc_doi" % (sep)
    croci = CrowdsourcedCitationSource(input_file)
    new = []
    cit = croci.get_next_citation_data()
    while cit is not None:
            citing, cited, creation, timespan, journal_sc, author_sc = cit
            new.append({
                "citing": citing,
                "cited": cited,
                "creation": "" if creation is None else creation,
                "timespan": "" if timespan is None else timespan,
                "journal_sc": "" if journal_sc is None else journal_sc,
                "author_sc": "" if author_sc is None else author_sc
            })
            cit = croci.get_next_citation_data()
    with open('croci_test_complete.csv', 'w+') as w:
        writer = DictWriter(w, fieldnames=new[0].keys())
        writer.writeheader()
        for line in new:
            writer.writerow(line)
    #croci.populator.clean_dir()

#java -server -Xmx4g -Dcom.bigdata.journal.AbstractJournal.file=../blazegraph.jnl -Djetty.port=9999 -jar ../blazegraph.jar
    