import unittest
import os
import shutil
import glob
import subprocess
from run_ranker import *
from test_functions import run_luigi_worker


class TestRunRanker(unittest.TestCase):
    pass

    @classmethod
    def setUpClass(cls):
        cls.working_dir = os.getcwd()
        os.chdir('tests')
        print(os.getcwd())
        os.mkdir('SubwellImages')
        os.mkdir('transfers')
        os.mkdir('ranker_jobs')
        with open('90j8_2018-04-11_2drop.done', 'w') as f:
            f.write('')
        os.mkdir('barcodes_2drop')
        shutil.copy('test_data/90j8.csv', 'barcodes_2drop/90j8.csv')
        shutil.copytree('test_data/90j8_2018-04-11_RI1000-0081-2drop/',
                        'SubwellImages/90j8_2018-04-11_RI1000-0081-2drop/')
        mat_files = glob.glob('test_data/rcLeft-mrc2d*.mat')
        for file in mat_files:
            shutil.copy(file, str(file).replace('test_data/', ''))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('SubwellImages')
        shutil.rmtree('transfers')
        shutil.rmtree('ranker_jobs')
        shutil.rmtree('barcodes_2drop')
        mat_files = glob.glob('test_data/rcLeft-mrc2d*.mat')
        for file in mat_files:
            os.remove(str(file).replace('test_data/', ''))
        os.chdir(cls.working_dir)

    def test_run_ranker(self):
        subwell_directory = 'SubwellImages'
        plate_directories = [x[0] for x in os.walk(os.path.join(os.getcwd(), subwell_directory))
         if os.path.join(os.getcwd(), 'SubwellImages') not in x]

        imagers=[]
        plate_types=[]
        plates=[]

        for plate in plate_directories:
            components = plate.split('/')
            plate_components = components[-1].split('_')
            print(plate_components)

            imagers.append(str(plate_components[-1].split('-')[0]) + '-' + plate_components[-1].split('-')[1])
            plate_types.append(plate_components[-1].split('-')[2])
            plates.append(components[-1])

        print(imagers)
        print(plate_types)
        print(plates)

        ranker = run_luigi_worker(RunRanker(plate=plates[0], plate_type=plate_types[0], imager=imagers[0]))
        self.assertTrue(ranker)
        self.assertTrue(os.path.isfile('ranker_jobs/RANK_90j8_2018-04-11_RI1000-0081-2drop.sh'))

    def test_run_ranker_local(self):
        job_file = 'RANK_90j8_2018-04-11_RI1000-0081-2drop.sh'
        process = subprocess.Popen(str('cd ranker_jobs; chmod 777 ' + job_file + '; ./' + job_file),
                                   shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)