#!/usr/bin/python
# -*- coding: utf-8 -*-
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
from typing import Dict
from oc_graphenricher.APIs import *
from oc_meta.run.meta_process import MetaProcess, run_meta_process
import shutil
import csv
from oc_ocdm.graph.graph_entity import GraphEntity
import requests
import os
from os import sep
import json
import time
import random
import requests_cache
import backoff
from oc_meta.plugins.crossref.crossref_processing import CrossrefProcessing
from SPARQLWrapper import SPARQLWrapper, JSON
from index.identifier.doimanager import DOIManager
from index.identifier.metaidmanager import MetaIDManager
from index.identifier.wikidatamanager import WikiDataIDManager

VALID_WD = {'other_ids','wdid','metadata'}

class Populator:
    '''
    This class is used to query Crossref, Wikidata and Pubmed and to prepare the input files for meta.
    '''
    def __init__(self) -> None:
        requests_cache.install_cache('pop_cache')
        self.cr_api = "https://api.crossref.org/works/"
        self.wd_sparql_endpoint = "https://query.wikidata.org/sparql"
        self.headers = "OpenCitations - http://opencitations.net;  mailto:contact@opencitations.net"# chi metto?
        self.sparql = SPARQLWrapper(self.wd_sparql_endpoint , agent= self.headers)
        self.possible_ids = set(['doi','wdid','pmid','metaid'])
        self.seen_ids = dict()
        self.viaf = VIAF()
        self.orcid = ORCID()
        self.doi = DOIManager()
        self.metaid = MetaIDManager()
        self.crossref_processor = CrossrefProcessing()
        self.wikidata_id = WikiDataIDManager()
        self.ids_counter = 0
        self.tmp_dir = '..%scroci_tmp' % os.sep
        if not os.path.isdir(self.tmp_dir):
            os.mkdir(self.tmp_dir)
        if not os.path.isdir('%s%spointers' % (self.tmp_dir,os.sep)):
            os.mkdir('%s%spointers' % (self.tmp_dir,os.sep))
        if not os.path.isdir('%s%smeta' % (self.tmp_dir,os.sep)):
            os.mkdir('%s%smeta' % (self.tmp_dir,os.sep))
        self.clean_dir()
        
        
        

    
    @backoff.on_exception(backoff.expo, requests.exceptions.ReadTimeout, max_tries=20)
    def query_crossref(self, doi) -> dict:
        '''
        This method queries crossref by adding to the API url the DOI. It returns the result the request and the doi added. In order to avoid being blocked by the API.
        :params doi: the doi to query
        :return: the result of the query on Crossref
        '''
        query = self.cr_api + doi
        tentative = 0
        while tentative < 3:
            req = requests.get(query, timeout=60)
            if req.status_code == 404:
                return None
            elif req.status_code == 200:
                req = req.json()
                req = {'items':[req['message']]}
                return req
            else:
                print('Error in querying Crossref: %s' % req.status_code)
                tentative +=1
                continue
        
        

    def query_wd(self, value:str, to_search:str, starting_id = '') -> dict:
        '''This method queries wikidata
        :param value: the starting value to search e.g. a DOI that we can use to search wikidata
        :param to_search: the type of value to search e.g. wdid. It has to be one of the VALID_WD
        :param starting_id: the starting id to search.
        :return: the result of the query on wikidata in dict form'''
        if to_search not in VALID_WD:
            raise ValueError("WD argument must be one of %r." % VALID_WD)
        query = ""
        if to_search == 'other_ids': # Looking for other_ids. Check for each of them and return the ones that are found
            query1 = """SELECT ?doi
                WHERE {
                %s wdt:P356 ?doi.
                    
                }""" % value
            query2 = """SELECT ?pmid
                WHERE {
                %s wdt:P698 ?pmid.
                    
                }""" % value
            query3 = """SELECT ?ocid
                WHERE {
                %s wdt:P3181 ?ocid.
                    
                }""" % value
            self.sparql.setQuery(query1)
            self.sparql.setReturnFormat(JSON)
            res = dict()
            to_add = self.sparql.query().convert()
            if to_add['results']['bindings'] != []:
                res['doi'] = to_add['results']['bindings'][0]['doi']['value']
            self.sparql.setQuery(query2)
            self.sparql.setReturnFormat(JSON)
            to_add = self.sparql.query().convert()
            if to_add['results']['bindings'] != []:
                res['pmid'] =to_add['results']['bindings'][0]['pmid']['value']
            self.sparql.setQuery(query3)
            self.sparql.setReturnFormat(JSON)
            to_add = self.sparql.query().convert()
            if to_add['results']['bindings'] != []:
                res['metaid'] = 'br/' + to_add['results']['bindings'][0]['ocid']['value']
            return res
        elif to_search == 'wdid':
            if starting_id == 'doi':
                query = """SELECT ?wdid 
                    WHERE {
                    ?wdid wdt:P356 '%s'.
                        
                    }""" % value
                self.sparql.setQuery(query)
                self.sparql.setReturnFormat(JSON)
                if len(self.sparql.query().convert()['results']['bindings']) == 0:
                    query = """SELECT ?wdid 
                    WHERE {
                    ?wdid wdt:P356 '%s'.
                        
                    }""" % value.upper()
                    self.sparql.setQuery(query)
                    self.sparql.setReturnFormat(JSON)
                return self.sparql.query().convert()['results']['bindings']
            elif starting_id == 'pmid':
                query = """SELECT ?wdid 
                    WHERE {
                    ?wdid wdt:P698 '%s'.
                        
                    }""" % value
                self.sparql.setQuery(query)
                self.sparql.setReturnFormat(JSON)
                return self.sparql.query().convert()['results']['bindings']
        elif to_search == 'metadata': # da implementare
            pass
       
            



    def choose_service(self,ids) -> dict:
        '''
        This method chooses which services to query
        :param ids: a dictionary containing ids
        :return: a dictionary containing the information retrieved
        '''

        result = dict()
        if 'doi' in ids:
            #this is a doi
            result =  self.crossref_pipeline(ids['doi'])
            
        elif 'wdid' in ids:
            result =  self.wd_pipeline(ids['wdid'])
            
        elif 'pmid' in ids:
            result =  self.pm_pipeline(ids['pmid']) # TODO: implement pmid pipeline
        tmp = ids
        for id in tmp:
            if id != 'wdid':
                ids[id] = "%s:%s" % (id, ids[id])
        result['id'] = "; ".join(ids.values())
        return result

    def populate_ids(self, ids, caching = True) -> dict:
        '''This method transforms an id string into a dictionary with multiple ids and chooses the right pipeline
        :params ids: a string with the ids separated by ';'
        :params return_ids: a boolean that indicates if just the ids need to be cached
        :returns: a dictionary with the populated and validated ids
        '''
        
        identifiers = dict()
        for id in ids.split('; '): # first, we check the presence of ids. Is this for loop necessary? maybe there is an alternative
            if '\'' in id:
                id = id.replace('\'', '')
            if '"' in id:
                id = id.replace('"', '')                

            if 'doi:' in id:
                id = self.doi.normalise(id) 
                if id != None:
                    
                    if 'doi:%s' %id in self.seen_ids and caching: # TODO: caching che ha senso
                        
                        return self.seen_ids['doi:%s' %id]
                    identifiers['doi'] = id
                else:
                    break
            elif 'pmid:' in id:
                if id in self.seen_ids and caching: # TODO: caching che ha senso
                    return self.seen_ids[id]
                identifiers['pmid'] = id.split('pmid:')[1] # TODO normalise pmid
                
            elif 'wd:' in id:
                if len(ids.split('; '))==1:
                    raise Warning("We cannot accept just one wdid. Please provide more identifiers for the resource.")
                    break
                id = self.wikidata_id.normalise(id)
                if id in self.seen_ids and caching:
                    return self.seen_ids[id]
                identifiers['wdid'] = id
            elif 'meta:' in id:
                id = self.metaid.normalise(id, include_prefix=True)
                if id != None:
                    if id in self.seen_ids and caching: # da sistemare
                        return self.seen_ids[id]
                    identifiers['metaid'] = id.split('meta:')[1]
            
        if len(identifiers) == 0: # if no id is present, we return None
            return None
        else: # there are ids; so we try to complete the identifiers
            missing_ids = self.possible_ids - set(identifiers.keys()) 
            if len(missing_ids) > 0:
                if 'wdid' in missing_ids:
                    for key in identifiers:
                        possible_wd = self.query_wd(identifiers[key], 'wdid',key) # here the assumption is that there is only one doi for each resource
                        
                        if possible_wd == None or possible_wd == []:
                            continue
                        if len(possible_wd) > 1:
                            raise ValueError("There is more than one wd id for %s" % identifiers[key])
                        # we get the wdid in the format {[{'wdid':{'value':'https://wikidata.org/entity/Q123'}}]}
                        possible_wd = 'wd:'+ possible_wd[0]['wdid']['value'].split('entity/')[1] 
                        possible_wd = self.wikidata_id.normalise(possible_wd)

                        if possible_wd != None:
                            identifiers['wdid'] = possible_wd
                            missing_ids.remove('wdid')
                            break
                if 'wdid' in identifiers:
                    if 'doi' in missing_ids or 'pmid' in missing_ids or 'metaid' in missing_ids:
                        others = self.query_wd(identifiers['wdid'],'other_ids')
                        for item in others: #verificare qual è quello giusto?
                            if 'doi' in item: 
                                possible_doi = self.doi.normalise(others['doi'])
                                if self.doi.normalise(possible_doi) != None:
                                    identifiers['doi'] = self.doi.normalise(possible_doi)
                            if 'pmid' in item: 
                                identifiers['pmid'] = others['pmid']
                            '''if 'metaid' in item: #non è metaid ma identificativo interno di OC corpus
                                possible_metaid = self.metaid.normalise(others['metaid'])
                                print(possible_metaid)
                                if self.metaid.normalise(possible_metaid) != None:
                                    identifiers['metaid'] = self.metaid.normalise(possible_metaid)'''
                #if 'pmid' in missing_ids and 'wdid' in identifiers: # maybe insert some other service?
                #    possible_pmid = self.query_wd(identifiers['wdid'],'pmid')
                #    identifiers['pmid'] = possible_pmid # todo: normalise pmid
        for id in identifiers:
            if not id in identifiers[id] and id != 'wdid' and id != 'metaid':
                id = '%s:%s' % (id, identifiers[id])
            elif id == 'wdid':
                id = identifiers[id]
            elif id == 'metaid':
                id = '%s:%s' % ('meta', identifiers[id])
            self.seen_ids[id] = self.ids_counter
        pos = self.ids_counter
        self.ids_counter += 1
        return (identifiers, pos) # returns a tuple with the identifiers and the position of the id

            
    

    def crossref_pipeline(self, doi) -> dict:
        '''This method is the pipeline for Crossref'''
        info = self.query_crossref(doi)
        if info == None: # if the doi is not present in Crossref, we return an empty dictionary. should we search also on wikidata or other services?
            
            return {'id': doi, 'title': '', 'author': "", 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', \
                'page': '', 'type': '', 'publisher': '', 'editor': ''}
        info = self.crossref_processor.csv_creator(info)[0]
        authors = info['author'].split('; ')
        authors_complete = list()
        for author in authors:
            author_list = list()
            orcid = ''
            viaf = ''
            if 'orcid' not in author:
                try:
                    author_list=[(author.split(', ')[1], author.split(', ')[0], None,None)]
                except IndexError:
                    authors_complete.append(author)
                    continue
                author_list = self.orcid.query(author_list, [(GraphEntity.iri_doi,doi) ])
                if author_list[0][2] != None:
                    orcid = 'orcid:'+ author_list[0][2]
            else:
                author, orcid = author.split('[')
                orcid = orcid.split(']')[0]
            if 'viaf' not in author:
                possible_viaf = self.viaf.query(author.split(', ')[1], author.split(', ')[0], info['title'])
                if possible_viaf != None:
                    viaf = 'viaf:'+ possible_viaf
                
            if orcid != '' and viaf != '':
                authors_complete.append(('%s [%s; %s]' %(author, orcid, viaf)))
            elif orcid != '':
                authors_complete.append(('%s [%s]' %(author, orcid)))
            elif viaf != '':
                authors_complete.append(('%s [%s]' %(author, viaf)))
            else:
                authors_complete.append(author)

        info['author'] = '; '.join(authors_complete)


        return info

    def wd_pipeline(self, id): # TODO: da impleementare vedi come fa ramose
        '''This method is the pipeline for wikidata'''
        return {'id': id, 'title': '', 'author': "", 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}

    def pm_pipeline(self, id):
        '''This method is the pipeline for PubMed'''
        return {'id': id, 'title': '', 'author': "", 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}

    def run(self, row):
        '''This method manages the entire process for the preprocessing to OC_Meta
        :params ids: list of ids to be processed'''
        found_ids = list()
        pointers = list()
        ids = [row.get('citing_id'), row.get('cited_id')]
        dates = [row.get('citing_publication_date'), row.get('cited_publication_date')]
        for id in ids:
            id = self.populate_ids(id)
            if id == None:
                return None # if one of the ids is invalid, break the process
            if isinstance(id, int): # if it is a number, it means that the id has been already encountered.
                pointers.append(id)
            else:
                found_ids.append(id[0])
                pointers.append(id[1])
        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)
        for i in range(len(found_ids)):
            id = found_ids[i]
            date = dates[i]
            pop_row = self.choose_service(id) # this chooses the pipeline
            
            # If the pipeline does not find a date, use the date given to give to Meta.

            if len(str(pop_row['pub_date'])) == 0:
                pop_row['pub_date'] = date

            # Else, use the date found in the pipeline. In the end, the date used will be the one validated by meta.

            elif i == 0:
                row['citing_publication_date'] = pop_row['pub_date']
            elif i == 1:
                row['cited_publication_date'] = pop_row['pub_date']

            if os.path.isfile('%s%smeta%sto_meta.csv' %(self.tmp_dir, os.sep, os.sep)): # TODO: Come mantenere distinti i file per evitare che si sovrappongano?
                with open('%s%smeta%sto_meta.csv' %(self.tmp_dir, os.sep, os.sep), 'a', encoding='utf8') as w:
                    writer = csv.writer(w)
                    writer.writerow(pop_row.values())
            else:
                with open('%s%smeta%sto_meta.csv' %(self.tmp_dir, os.sep, os.sep), 'w+', encoding='utf8') as w:
                    writer = csv.writer(w)
                    writer.writerows([pop_row.keys(), pop_row.values()])

        if os.path.isfile('%s%spointers%sid_pointers.csv' %(self.tmp_dir, sep,sep)):
            with open('%s%spointers%sid_pointers.csv' %(self.tmp_dir, sep,sep), 'a', encoding='utf8') as w:
                writer = csv.writer(w)
            
                writer.writerow([pointers[0], \
                    row.get('citing_publication_date'), pointers[1], row.get('cited_publication_date')])
        else:
            with open('%s%spointers%sid_pointers.csv' %(self.tmp_dir, sep,sep), 'w+', encoding='utf8') as w:
                writer = csv.writer(w)
                
                writer.writerows([['citing_id','citing_publication_date','cited_id','cited_publication_date'], [pointers[0], \
                    row.get('citing_publication_date'), pointers[1], row.get('cited_publication_date')]])

    def clean_dir(self) -> None: 
        '''
        This method cleans the temporary directory with the results, as well as the cached ids.
        '''
        self.seen_ids = dict()
        self.ids_counter = 0
        if os.path.exists('%s%spointers' % (self.tmp_dir,os.sep)):
            shutil.rmtree('%s%spointers' % (self.tmp_dir,os.sep))
            os.mkdir('%s%spointers' % (self.tmp_dir,os.sep))
        if os.path.exists('%s%smeta' % (self.tmp_dir,os.sep)):
            shutil.rmtree('%s%smeta' % (self.tmp_dir,os.sep))
            os.mkdir('%s%smeta' % (self.tmp_dir,os.sep))
    
    

if __name__ == '__main__':
    pop = Populator()


    pop.run(['doi:10.1016/S0140-6736(08)60424-9','pmid:16635265'])
