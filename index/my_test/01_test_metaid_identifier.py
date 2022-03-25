import unittest
from os import sep, remove
import os
from os.path import exists
from index.identifier.metaidmanager import MetaIDManager
import shutil
import pandas as pd

class MyTestCase( unittest.TestCase ):
    def setUp(self):
        #self.input_dir = "index%croci_test%data%metaid.csv" % (sep,sep)
        self.metaid_manager = MetaIDManager()

    def test_recognise_metaid(self):
        self.assertTrue(self.metaid_manager.normalise('br/0601'))

    


if __name__ == '__main__':
    unittest.main()