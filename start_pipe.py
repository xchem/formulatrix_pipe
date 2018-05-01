import luigi
import shutil
import os
# from get_barcodes import GetPlateTypes
from run_ranker import FindPlates, BatchCheckRanker
import datetime as dt


class StartPipe(luigi.Task):

    def requires(self):
        now = dt.datetime.now()
        ago = now - dt.timedelta(minutes=30)
        directories = ['barcodes_2drop', 'barcodes_3drop']
        for directory in directories:
            for root, dirs, files in os.walk(os.path.join(os.getcwd(), directory)):
                for fname in files:
                    path = os.path.join(root, fname)
                    st = os.stat(path)
                    mtime = dt.datetime.fromtimestamp(st.st_mtime)
                    if mtime > ago:
                        print(os.path.join(root, fname))
                        os.remove(os.path.join(root, fname))
        try:
            os.remove('plates.done')
        except:
            pass
        try:
            os.remove('checkrank.done')
        except:
            pass
        try:
            os.remove('findplates.done')
        except:
            pass
        # yield GetPlateTypes()
        yield FindPlates()
        yield BatchCheckRanker()

    def run(self):
        pass
