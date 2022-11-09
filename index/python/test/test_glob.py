import unittest
from os import sep, remove, makedirs
import os
from os.path import exists, join
import shutil
from shutil import rmtree
import pandas as pd
import json

from oc.index.glob.csv import CSVDataSource
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.pmid import PMIDManager

from oc.index.scripts.glob_doci import (
    issn_data_recover_doci,
    issn_data_to_cache_doci,
    valid_date_doci,
    load_json_doci,
    process_doci,
)

from oc.index.scripts.glob_noci import (
    issn_data_recover_noci,
    issn_data_to_cache_noci,
    build_pubdate_noci,
    process_noci,
)


from oc.index.scripts.glob_crossref import (
    build_pubdate_coci,
    get_all_files_coci,
    load_json_coci,
    process_coci,
)


class GlobTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = join("index", "python", "test", "data")
        self.doi_manager = DOIManager()
        self.pmid_manager = PMIDManager()
        self.issn_manager = ISSNManager()
        self.orcid_manager = ORCIDManager()

        # Initialize datasource
        self.noci_datasource = None
        self.doci_datasource = None
        self.coci_datasource = None

        # COCI
        self.inp_coci = join(self.test_dir, "crossref_glob_dump_input")
        self.out_coci = self.__get_output_directory("crossref_glob_dump_output")
        self.dir_get_all_files_coci = join(self.test_dir, "crossref_glob_dump_input")
        self.sample_doi_coci = self.doi_manager.normalise("10.7717/peerj.4375", True)
        self.sample_reference_coci = self.doi_manager.normalise(
            "10.1016/j.joi.2016.08.002", True
        )
        self.sample_doi_coci_2 = self.doi_manager.normalise(
            "10.1016/j.websem.2017.06.001", True
        )
        self.obj_for_date = {"issued": {"date-parts": [[2017, 5]]}}
        self.obj_for_date_2 = {"issued": {"date-parts": [[2018, 2, 13]]}}
        self.obj_for_date_3 = {"issued": {"date-parts": [[2015, 3, 9]]}}
        self.load_json_c_inp = join(self.inp_coci, "crossref_dump.json")

        # DOCI
        self.inp_doci = join(self.test_dir, "doci_glob_dump_input")
        self.out_doci = self.__get_output_directory("doci_glob_dump_output")
        self.issn_journal_doci = {
            "european journal of organic chemistry": ["1434193X"],
            "drug delivery and translational research": ["2190-3948"],
            "the social science journal": ["1873-5355"],
        }
        self.n_doci = 3
        self.dir_issn_map_doci = join(self.test_dir, "recover_w_mapping_doci")
        self.dir_no_issn_map_doci = join(self.test_dir, "recover_wo_mapping_doci")
        self.dir_data_to_cache_doci = join(self.test_dir, "issn_data_to_cache_doci")
        self.dir_get_all_files_doci = self.__get_output_directory("doci_pp_dump_output")
        self.sample_reference_doci = self.doi_manager.normalise(
            "10.1002/anie.200504236", True
        )
        self.load_json_d_inp = join(self.inp_doci, "doci_dump.json")

        # NOCI
        self.inp_noci = join(self.test_dir, "noci_glob_dump_input")
        self.out_noci = self.__get_output_directory("noci_glob_dump_output")
        self.id_orcid_map = join(
            self.test_dir, "noci_id_orcid_mapping", "doi_orcid_index.zip"
        )
        self.n_noci = 3
        self.issn_journal_noci = {
            "N Biotechnol": ["1871-6784"],
            "Biochem Med": ["0006-2944"],
            "Magn Reson Chem": ["0749-1581"],
        }
        self.dir_issn_map_noci = join(self.test_dir, "recover_w_mapping_noci")
        self.dir_no_issn_map_noci = join(self.test_dir, "recover_wo_mapping_noci")
        self.dir_data_to_cache_noci = join(self.test_dir, "issn_data_to_cache_noci")
        self.dir_get_all_files_noci = self.__get_output_directory(
            "noci_md_pp_dump_output"
        )
        self.csv_sample = join(self.inp_noci, "CSVFile_1.csv")
        self.sample_reference_noci = self.pmid_manager.normalise("4150960", True)

    def __get_output_directory(self, directory):
        directory = join(".", "tmp", directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def test_build_pubdate_coci(self):
        self.assertEqual(build_pubdate_coci(self.obj_for_date), "2017-05")
        self.assertEqual(build_pubdate_coci(self.obj_for_date_2), "2018-02-13")
        self.assertEqual(build_pubdate_coci(self.obj_for_date_3), "2015-03-09")

    def test_load_json_coci(self):
        self.assertTrue(
            isinstance(load_json_coci(self.load_json_c_inp, None, 1, 1), dict)
        )

    def test_process_coci(self):
        process_coci(self.inp_coci, self.out_coci)
        self.coci_datasource = CSVDataSource("COCI")

        citing_doi = self.doi_manager.normalise(self.sample_doi_coci, True)
        citing_doi_2 = self.doi_manager.normalise(self.sample_doi_coci_2, True)
        self.assertEqual(
            self.coci_datasource.get(citing_doi_2)["orcid"],
            {"0000-0003-0530-4305", "0000-0002-7562-5203"},
        )
        self.assertEqual(self.coci_datasource.get(citing_doi)["valid"], {"v"})
        self.assertEqual(
            self.coci_datasource.get(self.sample_reference_coci)["valid"], {"v"}
        )
        self.assertEqual(self.coci_datasource.get(citing_doi)["date"], {"2018-02-13"})
        self.assertEqual(self.coci_datasource.get(citing_doi)["issn"], {"2167-8359"})

    # TEST DOCI GLOB
    def test_issn_data_recover_doci(self):
        self.assertTrue(True)
        if exists(self.dir_no_issn_map_doci):
            rmtree(self.dir_no_issn_map_doci)
        makedirs(self.dir_no_issn_map_doci)
        if exists(self.dir_issn_map_doci):
            rmtree(self.dir_issn_map_doci)
        makedirs(self.dir_issn_map_doci)
        with open(
            join(self.dir_no_issn_map_doci, "journal_issn.json"), "w", encoding="utf-8"
        ) as g:
            json.dump({}, g, ensure_ascii=False, indent=4)
        with open(
            join(self.dir_issn_map_doci, "journal_issn.json"), "w", encoding="utf-8"
        ) as f:
            json.dump(self.issn_journal_doci, f, ensure_ascii=False, indent=4)

        # Test the case in which there is no mapping file for journals - issn
        self.assertEqual(issn_data_recover_doci(self.dir_no_issn_map_doci), {})
        # Test the case in which there is a mapping file for journals - issn
        self.assertNotEqual(issn_data_recover_doci(self.dir_issn_map_doci), {})

        rmtree(self.dir_no_issn_map_doci)
        rmtree(self.dir_issn_map_doci)

    def test_issn_data_to_cache_doci(self):
        filename = join(self.dir_data_to_cache_doci, "journal_issn.json")
        if not exists(self.dir_data_to_cache_doci):
            makedirs(self.dir_data_to_cache_doci)
        if exists(filename):
            remove(filename)
        self.assertFalse(exists(filename))
        issn_data_to_cache_doci(self.issn_journal_doci, self.dir_data_to_cache_doci)
        self.assertTrue(exists(filename))
        rmtree(self.dir_data_to_cache_doci)

    def test_valid_date_doci(self):
        self.assertTrue(isinstance(valid_date_doci(2018), str))
        self.assertEqual(valid_date_doci("2018-11-25"), "2018-11-25")
        self.assertIsNone(valid_date_doci("11-25-2018"))

    def test_load_json_doci(self):
        self.assertTrue(
            isinstance(load_json_doci(self.load_json_d_inp, None, 1, 1), dict)
        )

    def test_process_doci(self):
        process_doci(self.inp_doci, self.out_doci, self.n_doci)
        self.doci_datasource = CSVDataSource("DOCI")

        citing_doi = "doi:10.1002/ejoc.201800947"
        self.assertEqual(
            self.doci_datasource.get(citing_doi)["orcid"], {"0000-0002-2397-9093"}
        )
        self.assertEqual(self.doci_datasource.get(citing_doi)["valid"], {"v"})
        self.assertEqual(
            self.doci_datasource.get(self.sample_reference_doci)["valid"], {"v"}
        )
        self.assertEqual(self.doci_datasource.get(citing_doi)["date"], {"2018-11-25"})
        self.assertEqual(self.doci_datasource.get(citing_doi)["issn"], {"1434-193X"})

    # TEST NOCI GLOB
    def test_issn_data_recover_noci(self):
        if exists(self.dir_no_issn_map_noci):
            rmtree(self.dir_no_issn_map_noci)
        makedirs(self.dir_no_issn_map_noci)
        if exists(self.dir_issn_map_noci):
            rmtree(self.dir_issn_map_noci)
        makedirs(self.dir_issn_map_noci)
        with open(
            join(self.dir_no_issn_map_noci, "journal_issn.json"), "w", encoding="utf-8"
        ) as g:
            json.dump({}, g, ensure_ascii=False, indent=4)
        with open(
            join(self.dir_issn_map_noci, "journal_issn.json"), "w", encoding="utf-8"
        ) as f:
            json.dump(self.issn_journal_noci, f, ensure_ascii=False, indent=4)

        # Test the case in which there is no mapping file for journals - issn
        self.assertEqual(issn_data_recover_noci(self.dir_no_issn_map_noci), {})
        # Test the case in which there is a mapping file for journals - issn
        self.assertNotEqual(issn_data_recover_noci(self.dir_issn_map_noci), {})

        rmtree(self.dir_no_issn_map_noci)
        rmtree(self.dir_issn_map_noci)

    def test_issn_data_to_cache_noci(self):
        filename = join(self.dir_data_to_cache_noci, "journal_issn.json")
        if not exists(self.dir_data_to_cache_noci):
            makedirs(self.dir_data_to_cache_noci)
        if exists(filename):
            remove(filename)
        self.assertFalse(exists(filename))
        issn_data_to_cache_noci(self.issn_journal_noci, self.dir_data_to_cache_noci)
        self.assertTrue(exists(filename))
        rmtree(self.dir_data_to_cache_noci)

    def test_build_pubdate_noci(self):
        df = pd.DataFrame()
        for chunk in pd.read_csv(self.csv_sample, chunksize=1000):
            f = pd.concat([df, chunk], ignore_index=True)
            f.fillna("", inplace=True)
            for index, row in f.iterrows():
                pub_date = build_pubdate_noci(row)
                self.assertTrue(isinstance(pub_date, str))
                self.assertTrue(isinstance(int(pub_date), int))
                self.assertEqual(len(pub_date), 4)

    def test_process_noci(self):
        process_noci(self.inp_noci, self.out_noci, self.n_noci)
        self.noci_datasource = CSVDataSource("NOCI")

        citing_pmid = "pmid:2"
        citing_pmid5 = "pmid:5"
        self.assertEqual(self.noci_datasource.get(citing_pmid)["orcid"], None)
        # self.assertEqual(self.noci_datasource.get(citing_pmid5)["orcid"], {"0000-0002-4762-5345"})
        # run the glob process with credetials to make this test assertion pass
        self.assertEqual(self.noci_datasource.get(citing_pmid)["valid"], {"v"})
        self.assertEqual(
            self.noci_datasource.get(self.sample_reference_noci)["valid"], {"v"}
        )
        self.assertEqual(self.noci_datasource.get(citing_pmid)["date"], {"1975"})
        self.assertEqual(self.noci_datasource.get(citing_pmid)["issn"], {"0006-291X"})

        # try again with doi_orcid mapping folder
        # for files in os.listdir(self.out_noci):
        #     path = os.path.join(self.out_noci, files)
        #     try:
        #         shutil.rmtree(path)
        #     except OSError:
        #         os.remove(path)
        # self.assertEqual(len(os.listdir(self.out_noci)),0)
        # process_noci(self.inp_noci, self.out_noci, self.n_noci, self.id_orcid_map)
        # self.assertEqual(len(os.listdir(self.out_noci)), 5)
        #
        # df = pd.DataFrame()
        # for chunk in pd.read_csv(self.csv_sample, chunksize=1000):
        #     f = pd.concat([df, chunk], ignore_index=True)
        #     f.fillna("", inplace=True)
        #     for index, row in f.iterrows():
        #         pmid = row["pmid"]
        #         citing_pmid = self.pmid_manager.normalise(pmid, include_prefix=True)
        #         if citing_pmid == "pmid:2":
        #             self.assertEqual(self.id_orcid.get_value(citing_pmid), {'0000-0003-0014-4963'})


if __name__ == "__main__":
    unittest.main()