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
from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.finder.datacite import DataCiteResourceFinder
from oc.index.finder.wd import WikidataResourceFinder
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.isbn import ISBNManager
from oc.index.identifier.pmid import PMIDManager
from oc.index.identifier.wikidata import WikiDataIDManager

from oc_graphenricher.APIs import *
import shutil
import csv
from oc_ocdm.graph.graph_entity import GraphEntity
import os
from os import remove, sep
import json
import time
import random
import requests_cache
from oc_meta.plugins.crossref.crossref_processing import CrossrefProcessing
from oc.index.identifier.metaid import MetaIDManager
from oc_meta.run.meta_process import MetaProcess, run_meta_process
import dateutil.parser as parser
from oc_meta.lib.file_manager import get_data


VALID_QUERIES = {'other_ids': '''
    SELECT ?doi ?pmid  (GROUP_CONCAT( ?booknumber; separator = ' ') as ?isbn) WHERE {{{{ wd:{value} wdt:P356 ?doi }} UNION {{ wd:{value} wdt:P698 ?pmid }} UNION {{ wd:{value} wdt:P212|wdt:P957 ?booknumber}} }} group by ?doi ?pmid ?isbn
        ''', 
        "wdid" : """SELECT DISTINCT ?wdid WHERE {{{{  ?wdid wdt:P356 '{value}'}} UNION {{?wdid wdt:P698 '{value}'}} UNION {{  ?wdid wdt:P212|wdt:P957 '{value}'}}
        }}""",
        'metadata': """SELECT ?id (GROUP_CONCAT( ?a; separator = '; ') as ?author) ?venue ?pub_date ?title ?volume ?editor ?issue ?page ?type_id ?publisher (lang(?title) as ?language)  {{
  {{ BIND("{value}" as ?id)
  OPTIONAL {{ wd:{value} wdt:P98 ?editor}}
  OPTIONAL {{ wd:{value} wdt:P1476 ?title }} 
  OPTIONAL {{
    wd:{value} wdt:P577 ?date
    BIND(SUBSTR(str(?date), 0, 5) as ?pub_date)
  }}
  OPTIONAL {{ wd:{value} wdt:P1433 ?venue_id .
    ?venue_id rdfs:label ?venue}}
  OPTIONAL {{ wd:{value} wdt:P478 ?volume }}
  OPTIONAL {{ wd:{value} wdt:P433 ?issue }}
  OPTIONAL {{ wd:{value} wdt:P304 ?page }}
  OPTIONAL {{ wd:{value} wdt:P31 ?type_id . }}
  OPTIONAL {{
            wd:{value} wdt:P50 ?author_res .
            ?author_res wdt:P735/wdt:P1705 ?g_name ;
                        wdt:P734/wdt:P1705 ?f_name .
            BIND(CONCAT(?f_name, ", ",?g_name) as ?a) }}
 }} }} group by ?author ?venue ?pub_date ?title ?volume ?issue ?page ?publisher ?type_id ?editor ?id

        """}
# 'edited book', 'monograph', 'reference book', 'report', 'standard' 'book series', 'book set', 'journal', 'proceedings series', 'series', 'standard series' 
TYPE_DENOMINATIONS_WD = {'Q13442814':'journal article', 'Q18918145': 'journal article', 'Q1266946': 'dissertation', 'Q7318358': 'journal article', 'Q215028':'journal article', 'Q193495':'monograph', 'Q13136':'reference book', 'Q23927052':'proceedings article', 'Q1980247':'book chapter', 'Q1172284':'dataset', 'Q1711593':'edited book', 'Q1980247':'book section','Q277759':'book series','Q121769':'reference entry', 'Q28062188':'book set', 'Q317623':'standard', 'Q265158':'peer review', 'Q3331189':'book', 'Q7725634':'book'}

TYPE_DENOMINATIONS_DATACITE = {'article-journal':'journal article', 'article':'journal article', 'dataset': 'dataset','report':'report', 'peer-review':'peer review'} 

FIELDNAMES = ('id','title','author','pub_date','venue','volume','issue','page','type','publisher','editor')



def substitute_dict(old_dict, sub_dict):
        '''This method substitutes empty elements in dictionaries'''
        for el in sub_dict:
            if el in old_dict:
                if sub_dict[el] == '' or old_dict[el] != '':
                    continue
                else:
                    old_dict[el] = sub_dict[el]
        return old_dict

def clean_dict(old_dict):
        '''This method substitutes empty elements in dictionaries'''
        tmp = old_dict
        for el in tmp:
            if el not in FIELDNAMES:
                old_dict.pop(el)
        return old_dict

def wd_preprocessing(values, id):
    values['id'] = id
    if 'type_id' in values:
        if values['type_id'].split('entity/')[1] in TYPE_DENOMINATIONS_WD:
            values['type'] = TYPE_DENOMINATIONS_WD[values.pop('type_id').split('entity/')[1]]
        else:
            values['type'] = ''
            print('Resource type not recognised from Wikidata: %s' % values.pop('type_id'))
    else:
        values['type'] = ''
    return values

def datacite_preprocessing(values, id): #TODO: better preprocessing
    result = dict()
    result['id'] = id
    authors = list()
    for person in values['creators']:
        if 'givenName' in person and 'familyName' in person:
            name = '%s, %s' % (person['familyName'], person['givenName'])

            if 'nameIdentifier' in person:
                for identifier in values['nameIdentifiers']:
                    if identifier['identifierSchema'] == 'ORCID':
                        orcid = identifier['nameIdentifier'].split('orcid.org/')[1]
                        name += ' [orcid:%s]' % orcid

                authors.append(name)

    if values['types']['citeproc'] in TYPE_DENOMINATIONS_DATACITE:
        result['type'] = TYPE_DENOMINATIONS_DATACITE[values['types']['citeproc']]
    else:
        result['type'] = ''
        print('Type from Datacite not recognised: %s' % values['types']['citeproc'])
    
    result['publisher'] = values['publisher']
    result['author'] = '; '.join(authors)

    result['title'] = values['titles'][0]['title']

    result['pub_date'] = values['publicationYear']
    return result




class MetaFeeder:
    '''
    This class is used to query Crossref, Wikidata and Pubmed and to prepare the input files for meta.
    '''
    def __init__(self, meta_config = '..%smeta_config.yaml' % os.sep) -> None:
        requests_cache.install_cache('pop_cache')
        self.cr_finder = CrossrefResourceFinder()
        self.wd_finder = WikidataResourceFinder(queries = VALID_QUERIES)
        self.datacite_finder = DataCiteResourceFinder()
        self.id_populator = IDPopulator(self.wd_finder)
        self.citations = list()
        self.author_pop = AuthorPopulator()
        self.meta_process = MetaProcess(config = meta_config)
        self.crossref_processor = CrossrefProcessing()
        self.meta_folder = '..%soutput' % (os.sep)
        self.tmp_dir = '..%scroci_tmp' % os.sep
        if not os.path.isdir(self.tmp_dir):
            os.mkdir(self.tmp_dir)
        # if not os.path.isdir('%s%spointers' % (self.tmp_dir,os.sep)):
        #     os.mkdir('%s%spointers' % (self.tmp_dir,os.sep))
        if not os.path.isdir('%s%smeta' % (self.tmp_dir,os.sep)):
            os.mkdir('%s%smeta' % (self.tmp_dir,os.sep))
        self.clean_dir()        
        


    def choose_service(self,ids) -> dict: #TODO: Refactoring
        '''
        This method chooses which services to query and adds the authors
        :param ids: a dictionary containing ids
        :return: a dictionary containing the information retrieved
        '''

        result = dict()
        
        if 'doi' in ids:
            #this is a doi
            cr_result = self.cr_finder._call_api(ids['doi'])
            if cr_result is not None:
                result = self.crossref_processor.csv_creator(cr_result)
            elif cr_result is None:
                datacite_result = self.datacite_finder._call_api(ids['doi'])
                if datacite_result is not None:
                    result = datacite_preprocessing(datacite_result, ids)
            else:
                new_dict = wd_preprocessing(self.wd_finder._call_api('metadata', ids['wikidata']), ids)
                result = substitute_dict(result, new_dict)
            
        elif 'wikidata' in ids:
            result =  wd_preprocessing(self.wd_finder._call_api('metadata', ids['wikidata']), ids)
            if isinstance(result, list):
                for el in result:
                    if el['language'] == 'en':
                        result = el
                        break
            result.pop('language')
            
        for field in FIELDNAMES:
            if field not in result:
                result[field] = ''
        result['author'] = self.author_pop.get_author_info(ids,result)
        id_string = ""
        for identifier in ids:
            for id in ids[identifier].split(' '):
                id_string =  "%s%s:%s " % (id_string, identifier, id)
        result['id'] = id_string

        return result

    


    def run(self, row): 
        '''This method manages the entire process for the preprocessing to OC_Meta
        :params ids: list of ids to be processed'''
        found_ids = list()
        result = list()
        ids = {row.get('citing_id'): row.get('citing_publication_date'), row.get('cited_id'): row.get('cited_publication_date')}
        this_citation = list()
        for start_id in ids:
            id = self.id_populator.populate_ids(start_id)
        
            if id == None:
                return None # if one of the ids is invalid, break the process
            elif isinstance(id, int): # change
                this_citation.append(id)
                continue
            else:
                this_citation.append(id[1])
                id = id[0]
                id['start_id'] = start_id
                found_ids.append(id)

        for i in range(len(found_ids)): 
            id = found_ids[i]
            
            date = ids[id.pop('start_id')]

            pop_row = self.choose_service(id) # this chooses the pipeline
            # If the pipeline does not find a date, use the date given to give to Meta.

            if len(str(pop_row['pub_date'])) == 0:
                pop_row['pub_date'] = date
            # Else, use the date found in the pipeline. In the end, the date used will be the one validated by meta.
            result.append(pop_row)
        
        return result, this_citation

    def parse(self, file):
        to_meta = list()
        citations = list()
        with open(file, 'r') as input:
            reader = csv.DictReader(input)
            for row in reader:
                populated = self.run(row)
                
                if populated is not None:
                    to_meta.extend(populated[0])
                    citations.append(populated[1])
        file_to_meta = file
        while os.sep in file_to_meta:
            file_to_meta = file_to_meta.split(os.sep)[1]
        file_to_meta = "%s%s" % (file_to_meta[:-4], "_tometa.csv")
        with open('%s%smeta%s%s' %(self.tmp_dir, os.sep,os.sep, file_to_meta), 'w+', encoding='utf8') as w:
            writer = csv.DictWriter(w, fieldnames=FIELDNAMES)
            writer.writeheader()
            for row in to_meta:
                writer.writerow(row)
        run_meta_process(self.meta_process)
        os.remove('%s%smeta%s%s' %(self.tmp_dir, os.sep,os.sep, file_to_meta))
        meta_info = list()

        for dirpath, _, filenames in os.walk(os.path.join(self.meta_folder, 'csv')):
            for el in filenames:
                if file_to_meta[:-4] in el:
                    meta_info.extend(get_data(os.path.join(dirpath, el)))
                    break
        with open("%s%s%s" % (self.tmp_dir,os.sep,file_to_meta), 'w+') as output:
            writer = csv.DictWriter(output, fieldnames=('citing_id','citing_publication_date','cited_id','cited_publication_date'))
            writer.writeheader()
            for citation in citations:

                to_write = dict()
                to_write['citing_id'] = '%s%s' % ('meta:', meta_info[citation[0]]['id'].split('meta:')[1])
                to_write['citing_publication_date'] = meta_info[citation[0]]['pub_date']
                to_write['cited_id'] = '%s%s' % ('meta:', meta_info[citation[1]]['id'].split('meta:')[1])
                to_write['cited_publication_date'] = meta_info[citation[1]]['pub_date']
                writer.writerow(to_write)
        return "%s%s%s" % (self.tmp_dir,os.sep,file_to_meta)



    def clean_dir(self) -> None: 
        '''
        This method cleans the temporary directory with the results, as well as the cached ids.
        '''
        self.id_populator.seen_ids = dict()
        if os.path.exists('%s%smeta' % (self.tmp_dir,os.sep)):
            shutil.rmtree('%s%smeta' % (self.tmp_dir,os.sep))
            os.mkdir('%s%smeta' % (self.tmp_dir,os.sep))

class IDPopulator:
    '''This class is responsible for validating and populating a string of ids'''
    def __init__(self, wd_finder = None) -> None:
        self.doi = DOIManager()
        self.pmid = PMIDManager()
        self.isbn = ISBNManager()
        self.metaid = MetaIDManager()
        self.wikidata_id = WikiDataIDManager()
        self.wd_finder = wd_finder if wd_finder is not None else WikidataResourceFinder() #??? 
        self.validate_pipelines = {'doi': self.doi.normalise, 'pmid':self.pmid.normalise, 'wikidata':self.wikidata_id.normalise,'meta':self.metaid.normalise, 'isbn':self.isbn.normalise}
        self.seen_ids = dict()
        self.id_num = 0

    def validate_ids(self, ids, caching = True) -> dict: #TODO: refactoring
        '''This method transforms an id string into a dictionary with multiple ids and chooses the right pipeline
        :params ids: a string with the ids separated by ';'
        :params return_ids: a boolean that indicates if just the ids need to be cached
        :returns: a dictionary with the populated and validated ids
        '''
        
        identifiers = dict()
        try:
            for id in ids.split('; '): # first, we check the presence of ids. Is this for loop necessary? maybe there is an alternative
                if '\'' in id:
                    id = id.replace('\'', '')
                if '\"' in id:
                    id = id.replace('"', '')   
                prefix_idx = id.index(':')   

                prefix, id = id[:prefix_idx], id[prefix_idx + 1:]
                if prefix in self.validate_pipelines:
                    id = self.validate_pipelines[prefix](id)
                    if id != None:
                        if '%s:%s' % (prefix, id) in self.seen_ids:
                            return self.seen_ids['%s:%s' % (prefix, id)]
                    else:
                        continue
                    identifiers[prefix] = id
        except:
            return None
        if len(identifiers) == 0: # if no id is present, we return None
            return None
        else:
            return identifiers

    def complete_ids(self, identifiers:dict):
        missing_ids = list(el for el in self.validate_pipelines if el not in identifiers)
        if len(missing_ids) > 0:
            if 'wikidata' in missing_ids:
                for key in identifiers:

                    possible_wd = self.wd_finder._call_api('wdid', identifiers[key]) # here the assumption is that there is only one doi for each resource
                    
                    if possible_wd == None or len(possible_wd) == 0:
                        if any(char.islower() for char in identifiers[key]):
                            tmp = identifiers[key]
                            identifiers[key] = identifiers[key].upper()
                            completed, num = self.complete_ids(identifiers)
                            completed[key] = tmp
                            return completed, num
                        else:
                            continue
                    if len(possible_wd) > 1:
                        raise Warning("There is more than one wikidata id for %s" % identifiers[key])
                    # we get the wdid in the format {[{'wdid':{'value':'https://wikidata.org/entity/Q123'}}]}
                    possible_wd = self.wikidata_id.normalise(possible_wd['wdid'])

                    if possible_wd != None:
                        identifiers['wikidata'] = possible_wd
                        missing_ids.remove('wikidata')
                        break
            if 'wikidata' in identifiers:
                if 'doi' in missing_ids or 'pmid' in missing_ids or 'isbn' in missing_ids:
                    others = self.wd_finder._call_api('other_ids',identifiers['wikidata'])
                    for item in others:
                        if others[item] != '':
                            identifiers[item] = others[item]
                        
        for identifier in identifiers:
            for id in identifier.split(" "):
                id = '%s:%s' % (id, self.validate_pipelines[id](identifiers[id]))
                self.seen_ids[id] = self.id_num
        return identifiers, self.id_num # returns a tuple with the identifiers and the position of the id

    def populate_ids(self, ids):
        validated = self.validate_ids(ids)
        if isinstance(validated, int):
            return validated
        elif validated is not None:
            
            ids = self.complete_ids(validated)
            self.id_num += 1
            return ids
    
class AuthorPopulator:
    '''This class adds information about the authors'''
    def __init__(self) -> None:
        self.viaf_api = VIAF()
        self.orcid_api = ORCID()

    def get_author_info(self, ids, resource):
        authors_complete = list()
        for author in resource['author'].split('; '):
            author_list = list()
            orcid = ''
            viaf = ''
            if 'orcid' not in author:
                try:
                    author_list=[(author.split(', ')[1], author.split(', ')[0], None,None)]
                except IndexError:
                    authors_complete.append(author)
                    continue
                if 'doi' in ids:
                    id = ids['doi']
                    author_list = self.orcid_api.query(author_list, [(GraphEntity.iri_doi, id) ]) 
                elif 'pmid' in ids:
                    id = ids['pmid']
                    author_list = self.orcid_api.query(author_list, [(GraphEntity.iri_pmid, id) ])
                if author_list[0][2] != None: # If an orcid is found, add it
                    orcid = 'orcid:'+ author_list[0][2]
            else:
                author, orcid = author.split('[')
                orcid = orcid.split(']')[0]
            if 'viaf' not in author:
                possible_viaf = self.viaf_api.query(author.split(', ')[1], author.split(', ')[0], resource['title'])
                if possible_viaf != None:
                    viaf = 'viaf:'+ possible_viaf
                
            if orcid != '' and viaf != '':
                authors_complete.append(('%s [%s %s]' %(author, orcid, viaf)))
            elif orcid != '':
                authors_complete.append(('%s [%s]' %(author, orcid)))
            elif viaf != '':
                if 'orcid' in author:
                    authors_complete.append(('%s %s%s' % (author[:-1], viaf, ']')))
                else:
                    authors_complete.append(('%s [%s]' %(author, viaf)))
            else:
                authors_complete.append(author)
        return "; ".join(authors_complete)

if __name__ == '__main__':
    populator  = MetaFeeder(meta_config='..\\meta_config.yaml')
    populator.parse('..\\test.csv')
    