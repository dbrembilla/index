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
import unittest
from os import sep, remove
from os.path import exists
from urllib.parse import unquote
from index.croci.populator import Populator
import csv 

class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.populator = Populator()
        self.test_ids = []
        self.result_ids = []
        with open("index%smy_test%stest_data%stest_ids.csv" % (sep,sep,sep),'r', encoding='utf8') as r:
            reader = csv.DictReader(r)
            for row in reader:
                self.test_ids.append(row['starting_ids'])
                self.result_ids.append(row['expected_ids'])
        self.dois = []
        self.list_crossref_pipeline = []
        with open("index%smy_test%stest_data%stest_crossref_pipeline.csv" % (sep,sep,sep),'r', encoding='utf8') as r:
            reader = csv.DictReader(r)
            for row in reader:
                self.list_crossref_pipeline.append(row)
                self.dois.append(row['id'])
        self.list_pointers = []
        with open("index%smy_test%stest_data%stest_id_pointers.csv"% (sep,sep,sep),'r', encoding='utf8') as r:
            reader = csv.reader(r)
            for row in reader:
                self.list_pointers.append(row)
        self.list_run_before_meta = []
        with open("index%smy_test%stest_data%stest_run.csv"% (sep,sep,sep),'r', encoding='utf8') as r:
            reader = csv.DictReader(r)
            for row in reader:
                self.list_run_before_meta.append(row)
        self.citations = [['doi:10.1186/1475-2875-10-378','wd:Q34247475; pmid:20889199'],['doi:10.1016/s0140-6736(10)61340-2','doi:10.1016/S0140-6736(08)60424-9'],['doi:10.1016/S0140-6736(08)60424-9','pmid:16635265'],['wd:Q46061806; pmid:18374409','doi:10.1007/BF02754849'],['wd:Q46061806; pmid:18374409','pmid:17255227'],['pmid:3910572','wd:Q41194190; pmid:398330']]

    def test_populate_ids(self):
        for i,id in enumerate(self.test_ids):
            with self.subTest(i=i):
                to_test = list(self.populator.populate_ids(self.test_ids[i], False)[0].values())
                for j in range(len(to_test)):
                    with self.subTest(j=j):
                        self.assertIn(to_test[j], self.result_ids[i])

    def test_crossref_pipeline(self):
        for i,id in enumerate(self.dois):
            with self.subTest(i=i):
                self.assertEqual(self.populator.crossref_pipeline(self.dois[i]), self.list_crossref_pipeline[i])
        
    def test_pointers(self):
        self.populator.clean_cache()
        for citation in self.citations:
            self.populator.run(citation)
        pointers = list()
        values = list()
        with open('tmp%sid_pointers.csv' %(sep)) as r:
            reader = csv.reader(r)
            for row in reader:
                pointers.append(row)
        with open('tmp%sto_meta.csv' %(sep)) as r:
            reader = csv.DictReader(r)
            for row in reader:
                values.append(row)
        for i,citation in enumerate(self.list_pointers):
            with self.subTest(i=i):
                self.assertEqual(pointers[i],citation)
        for i,populated_row in enumerate(self.list_run_before_meta): 
            with self.subTest(i=i):
                self.assertIn(values[i]['id'],populated_row['id'])
                
    def test_to_meta(self):
        self.populator.clean_cache()
        for citation in self.citations:
            self.populator.run(citation)
        values = list()
        results = list()
        with open('tmp%sto_meta.csv' %(sep)) as r:
            reader = csv.DictReader(r)
            for row in reader:
                row.pop('id')
                values.append(row)
        with open('index%smy_test%stest_data%stest_run.csv' %(sep,sep,sep)) as r:
            reader = csv.DictReader(r)
            for row in reader:
                row.pop('id')
                results.append(row)
        for i,row in enumerate(values):
            with self.subTest(i=i):
                self.assertEqual(row,results[i])
    
if __name__ == '__main__':
    unittest.main()