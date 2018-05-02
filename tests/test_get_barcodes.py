import unittest
from get_barcodes import *
import os
import shutil
import glob
from test_functions import run_luigi_worker


class TestConnectionToRockMaker(unittest.TestCase):
    pass


class TestGetPlateTypes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.working_dir = os.getcwd()
        print(cls.working_dir)
        os.chdir('tests')
        os.mkdir('barcodes_2drop')
        os.mkdir('barcodes_3drop')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('barcodes_2drop')
        shutil.rmtree('barcodes_3drop')
        os.remove('plates.done')
        os.chdir(cls.working_dir)

    def test_get_plate_types(self):
        # test the GetPlateTypes task runs
        test = run_luigi_worker(GetPlateTypes())
        # test that task was successful
        self.assertTrue(test)

        # get csv files creates by task
        two_drops = glob.glob('barcodes_2drop/*.csv')
        three_drops = glob.glob('barcodes_3drop/*.csv')
        # check that files were created
        self.assertTrue(two_drops)
        self.assertTrue(three_drops)




