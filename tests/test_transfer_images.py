import unittest
import os
import shutil
import glob
from transfer_images import *
from test_functions import run_luigi_worker


class TestConnectionToRemote(unittest.TestCase):
    pass


class TestGetImages(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.working_dir = os.getcwd()
        print(cls.working_dir)
        os.chdir('tests')
        os.mkdir('barcodes_2drop')
        os.mkdir('SubwellImages')
        os.mkdir('transfers')
        # copy two example csv files over for testing (90jc is empty)
        shutil.copy('test_data/90j8.csv', 'barcodes_2drop/90j8.csv')
        shutil.copy('test_data/90jc.csv', 'barcodes_2drop/90jc.csv')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('barcodes_2drop')
        shutil.rmtree('transfers')
        shutil.rmtree('SubwellImages')
        os.chdir(cls.working_dir)

    def test_empty_transfer(self):
        # check that an error is not thrown for transfer of empty file
        transfer = run_luigi_worker(TransferImages(csv_file=os.path.join(os.getcwd(), 'barcodes_2drop', '90jc.csv'),
                                                   barcode='90jc',
                                                   plate_type='2_drop'))

        self.assertTrue(transfer)

    def test_transfer(self):
        # run transfer for a 2_drop plate
        transfer = run_luigi_worker(TransferImages(csv_file=os.path.join(os.getcwd(), 'barcodes_2drop', '90j8.csv'),
                                                   barcode='90j8',
                                                   plate_type='2_drop'))
        # check that the task ran successfully
        self.assertTrue(transfer)
        # check that the expected filepath was created
        self.assertTrue(os.path.isdir('SubwellImages/90j8_2018-04-11_RI1000-0081-2_drop/'))
        # check that the right number of images were transferred (2*96 well plate = 192)
        images = glob.glob('SubwellImages/90j8_2018-04-11_RI1000-0081-2_drop/*.jpg')
        print(images)
        # self.assertEqual(len(images), 192)


