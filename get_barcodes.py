import pytds
import luigi
import os
import pandas
from transfer_images import TransferImages
from config_classes import RockMakerDBConfig


class GetPlateTypes(luigi.Task):
    server = RockMakerDBConfig().server
    database = RockMakerDBConfig().database
    username = RockMakerDBConfig().username
    password = RockMakerDBConfig().password
    translate = {'SWISSci_2drop':'2drop', 'SWISSci_3Drop':'3drop'}

    def requires(self):
        plate_types = []
        conn = pytds.connect(self.server, self.database, self.username, self.password)
        c = conn.cursor()

        c.execute("SELECT TN3.Name as 'Name' From Plate " \
                  "INNER JOIN TreeNode TN1 ON Plate.TreeNodeID = TN1.ID " \
                  "INNER JOIN TreeNode TN2 ON TN1.ParentID = TN2.ID " \
                  "INNER JOIN TreeNode TN3 ON TN2.ParentID = TN3.ID " \
                  "INNER JOIN TreeNode TN4 ON TN3.ParentID = TN4.ID " \
                  "where TN4.Name='Xchem'")

        for row in c.fetchall():
            plate_types.append(str(row[0]))
        plate_types = list(set(plate_types))

        for plate in plate_types:
            barcodes = []
            plates = []
            conn = pytds.connect(self.server, self.database, self.username, self.password)
            c = conn.cursor()

            c.execute("SELECT Barcode FROM Plate " \
                      "INNER JOIN TreeNode as TN1 ON Plate.TreeNodeID = TN1.ID " \
                      "INNER JOIN TreeNode as TN2 ON TN1.ParentID = TN2.ID " \
                      "INNER JOIN TreeNode as TN3 ON TN2.ParentID = TN3.ID " \
                      "INNER JOIN TreeNode as TN4 ON TN3.ParentID = TN4.ID " \
                      "where TN4.Name='Xchem' AND TN3.Name like %s", (str('%' + plate + '%'),))

            rows = c.fetchall()
            for row in rows:
                barcodes.append(str(row[0]))
                if plate in self.translate.keys():
                    plates.append(self.translate[plate])
                else:
                    raise Exception(str(plate + ' definition not found in pipeline code or config file!'))

        return [TransferImages(barcode=barcode, plate_type=plate_type,
                               csv_file=os.path.join(os.getcwd(), str('barcodes_' + str(plate_type)), str(barcode + '.csv')))
                for (plate_type, barcode) in list(zip(plates, barcodes))]


class GetBarcodeInfo(luigi.Task):
    server = RockMakerDBConfig().server
    database = RockMakerDBConfig().database
    username = RockMakerDBConfig().username
    password = RockMakerDBConfig().password
    barcode = luigi.Parameter()
    plate_type = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        cwd = os.getcwd()
        return luigi.LocalTarget(os.path.join(cwd, str('barcodes_' + str(self.plate_type)), str(self.barcode + '.csv')))

    def run(self):
        results = {
            'PlateID': [],
            'BatchID': [],
            'WellNum': [],
            'ProfileID': [],
            'WellRowLetter': [],
            'WellColNum': [],
            'DropNum': [],
            'ImagerName': [],
            'DateImaged': []
        }

        conn = pytds.connect(self.server, self.database, self.username, self.password)
        c = conn.cursor()
        c.execute(
            "SELECT DISTINCT "
            "w.PlateID, ib.ID, w.WellNumber, cpv.CaptureProfileID,  "
            "w.RowLetter, w.ColumnNumber, wd.DropNumber, "
            "crp.Value, it.DateImaged " \
            "from Well w INNER JOIN Plate p ON w.PlateID = p.ID " \
            "INNER JOIN WellDrop wd ON wd.WellID = w.id " \
            "INNER JOIN ExperimentPlate ep ON ep.PlateID = p.ID " \
            "INNER JOIN ImagingTask it ON it.ExperimentPlateID = ep.ID " \
            "INNER JOIN ImageBatch ib ON ib.ImagingTaskID = it.ID " \
            "INNER JOIN CaptureResult cr ON cr.ImageBatchID = ib.ID " \
            "INNER JOIN CaptureProfileVersion cpv ON cpv.ID = cr.CaptureProfileVersionID " \
            "INNER JOIN CaptureResultProperty crp on CaptureResultID = cr.ID " \
            "WHERE p.Barcode=%s AND crp.Value like %s", (str(self.barcode), '%RI1000%')
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
            results['DateImaged'].append(str(row[8].date()))

        df = pandas.DataFrame.from_dict(results)
        df.to_csv(self.output().path, index=False)