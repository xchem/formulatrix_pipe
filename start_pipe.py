import luigi
from run_ranker import FindPlates
from get_barcodes import GetPlateTypes


class StartPipe(luigi.Task):

    def requires(self):

        # remove all barcode info files

        yield GetPlateTypes()
        yield FindPlates()