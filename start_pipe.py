import luigi
import shutil
import os

from run_ranker import FindPlates


class StartPipe(luigi.Task):

    def requires(self):
        yield FindPlates()

    def run(self):
        shutil.rmtree('barcodes_2drop')
        os.mkdir('barcodes_2drop')
        shutil.rmtree('barcodes_3drop')
        os.mkdir('barcodes_3drop')
        os.remove('barcodes.info.done')

