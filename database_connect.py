import pytds
import luigi
import os
import csv
import pandas


class GetBarcodes(luigi.Task):
    server = luigi.Parameter(default='DIAMRISQL01\ROCKIMAGER')
    database = luigi.Parameter(default='RockMaker')
    username = luigi.Parameter(default='formulatrix')
    password = luigi.Parameter(default='fmlx216')

    def requires(self):
        pass

    def output(self):
        cwd = os.getcwd()
        return luigi.LocalTarget(os.path.join(cwd, 'barcodes.csv'))

    def run(self):
        barcodes = []
        conn = pytds.connect(self.server, self.database, self.username, self.password)
        c = conn.cursor()

        query = "SELECT Barcode FROM Plate " \
                "INNER JOIN TreeNode as TN1 ON Plate.TreeNodeID = TN1.ID " \
                "INNER JOIN TreeNode as TN2 ON TN1.ParentID = TN2.ID " \
                "INNER JOIN TreeNode as TN3 ON TN2.ParentID = TN3.ID " \
                "INNER JOIN TreeNode as TN4 ON TN3.ParentID = TN4.ID " \
                "where TN4.Name='Xchem'"

        c.execute(query)
        rows = c.fetchall()
        for row in rows:
            barcodes.append(str(row[0]))

        print(len(barcodes))

        with open(self.output().path, 'w') as f:
            wr = csv.writer(f)
            wr.writerow(barcodes)


class GetBarcodeInfo(luigi.Task):
    server = luigi.Parameter(default='DIAMRISQL01\ROCKIMAGER')
    database = luigi.Parameter(default='RockMaker')
    username = luigi.Parameter(default='formulatrix')
    password = luigi.Parameter(default='fmlx216')
    barcode = luigi.Parameter()

    def requires(self):
        return GetBarcodes()

    def output(self):
        cwd = os.getcwd()
        return luigi.LocalTarget(os.path.join(cwd, 'barcode_info', str(self.barcode + '.csv')))

    def run(self):
        results = {
            'PlateID': [],
            'BatchID': [],
            'WellNum': [],
            'ProfileID': [],
            'WellRowLetter': [],
            'WellColNum': [],
            'DropNum': [],
            'ImagerName': []
        }

        conn = pytds.connect(self.server, self.database, self.username, self.password)
        c = conn.cursor()
        c.execute(
            "SELECT DISTINCT "
            "w.PlateID, ib.ID, w.WellNumber, cpv.CaptureProfileID, "
            "w.RowLetter, w.ColumnNumber, wd.DropNumber, "
            "crp.Value " \
            "from Well w INNER JOIN Plate p ON w.PlateID = p.ID " \
            "INNER JOIN WellDrop wd ON wd.WellID = w.id " \
            "INNER JOIN ExperimentPlate ep ON ep.PlateID = p.ID " \
            "INNER JOIN ImagingTask it ON it.ExperimentPlateID = ep.ID " \
            "INNER JOIN ImageBatch ib ON ib.ImagingTaskID = it.ID " \
            "INNER JOIN CaptureResult cr ON cr.ImageBatchID = ib.ID " \
            "INNER JOIN CaptureProfileVersion cpv ON cpv.ID = cr.CaptureProfileVersionID " \
            "INNER JOIN CaptureResultProperty crp on CaptureResultID = cr.ID " \
            "WHERE p.Barcode=%s AND crp.Value like %s ", (self.barcode, '%RI1000%')
        )

        for row in c.fetchall():
            results['PlateID'].append(str(row[0]))
            results['BatchID'].append(str(row[1]))
            results['WellNum'].append(str(row[2]))
            results['ProfileID'].append(str(row[3]))
            results['WellRowLetter'].append(str(row[4]))
            results['WellColNum'].append(str(row[5]))
            results['DropNum'].append(str(row[6]))
            results['ImagerName'].append(str(row[7]))

        df = pandas.DataFrame.from_dict(results)
        df.to_csv(self.output().path, index=False)