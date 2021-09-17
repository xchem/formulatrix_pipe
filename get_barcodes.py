import pytds
import luigi
import os
import pandas
from config_classes import RockMakerDBConfig
from transfer_images import TransferImages


# This task kicks off the picking up of images, working backwards to find barcode info from TransferImages
class GetPlateTypes(luigi.Task):
    blacklist = luigi.Parameter(os.path.join(os.getcwd()), 'blacklist.txt')
    # these are all defined in luigi.cfg
    server = RockMakerDBConfig().server
    database = RockMakerDBConfig().database
    username = RockMakerDBConfig().username
    password = RockMakerDBConfig().password
    # this translates the directory names in RockMaker to names for the pipeline
    translate = {
        "SWISSci_2drop": "2drop",
        "SWISSci_3Drop": "3drop",
        "SWISSci_3drop": "3drop",
        "Mitegen_insitu1": "mitegen",
        "MiTInSitu": "mitegen",
    }

    def output(self):
        return luigi.LocalTarget("plates.done")

    def requires(self):

        if os.path.isfile(self.blacklist):
            blacklisted = [x.rstrip() for x in open(self.blacklist, 'r').readlines()]
        else:
            blacklisted = ['']

        plate_types = []
        # connect to the RockMaker database
        conn = pytds.connect(self.server, self.database, self.username, self.password)
        c = conn.cursor()

        # find the directory names corresponding to plate types for all entries in the XChem folder
        # XChem
        # |- Plate Type (e.g. SwissSci3D)
        #    |- Barcode
        #       |- Data
        c.execute(
            "SELECT TN3.Name as 'Name' From Plate "
            "INNER JOIN TreeNode TN1 ON Plate.TreeNodeID = TN1.ID "
            "INNER JOIN TreeNode TN2 ON TN1.ParentID = TN2.ID "
            "INNER JOIN TreeNode TN3 ON TN2.ParentID = TN3.ID "
            "INNER JOIN TreeNode TN4 ON TN3.ParentID = TN4.ID "
            "where TN4.Name='Xchem'"
        )

        for row in c.fetchall():
            plate_types.append(str(row[0]))
        # get all plate types
        plate_types = list(set(plate_types))
        # lists to hold barcodes and plate types
        barcodes = []
        plates = []

        for plate in plate_types:
            # connect to the RockMaker DB
            conn = pytds.connect(
                self.server, self.database, self.username, self.password
            )
            c = conn.cursor()

            # For each plate type, find all of the relevant barcodes
            c.execute(
                "SELECT Barcode FROM Plate "
                "INNER JOIN ExperimentPlate ep ON ep.PlateID = Plate.ID "
                "INNER JOIN ImagingTask it ON it.ExperimentPlateID = ep.ID "
                "INNER JOIN TreeNode as TN1 ON Plate.TreeNodeID = TN1.ID "
                "INNER JOIN TreeNode as TN2 ON TN1.ParentID = TN2.ID "
                "INNER JOIN TreeNode as TN3 ON TN2.ParentID = TN3.ID "
                "INNER JOIN TreeNode as TN4 ON TN3.ParentID = TN4.ID "
                "where TN4.Name='Xchem' AND TN3.Name like %s "
                "and it.DateImaged >= Convert(datetime, DATEADD(DD, -3, GETDATE()))",
                (str("%" + plate + "%"),),
            )

            rows = c.fetchall()
            for row in rows:

                # translate the name from RockMaker (UI) strange folders to 2drop or 3drop (in transfer parameter)
                if plate in self.translate.keys():
                    plates.append(self.translate[plate])
                    barcodes.append(str(row[0]))

                # else:
                # raise Exception(str(plate + ' definition not found in pipeline code or config file!'))
        # get all of the relevant info for every barcode (below)
        yield [
            GetBarcodeInfo(barcode=barcode, plate_type=plate)
            for (barcode, plate) in list(zip(barcodes, plates))
            if barcode not in blacklisted
        ]
        yield [
            TransferImages(
                barcode=barcode,
                plate_type=plate_type,
                csv_file=os.path.join(
                    os.getcwd(),
                    str("barcodes_" + str(plate_type)),
                    str(barcode + ".csv"),
                ),
            )
            for (plate_type, barcode) in list(zip(plates, barcodes))
            if barcode not in blacklisted
        ]

    def run(self):
        with self.output().open("w") as f:
            f.write("")


class GetBarcodeInfo(luigi.Task):
    blacklist = luigi.Parameter(os.path.join(os.getcwd()), 'blacklist.txt')
    # credentials to connect to RockMaker DB as defined in luigi.cfg
    server = RockMakerDBConfig().server
    database = RockMakerDBConfig().database
    username = RockMakerDBConfig().username
    password = RockMakerDBConfig().password
    barcode = luigi.Parameter()
    plate_type = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        if os.path.isfile(self.blacklist):
            blacklisted = [x.rstrip() for x in open(self.blacklist, 'r').readlines()]
        else:
            blacklisted = ['']

        if self.barcode not in blacklisted:
            cwd = os.getcwd()
            return luigi.LocalTarget(
                os.path.join(
                    cwd,
                    str("barcodes_" + str(self.plate_type)),
                    str(self.barcode + ".csv"),
                )
            )

    def run(self):

        if os.path.isfile(self.blacklist):
            blacklisted = [x.rstrip for x in open(self.blacklist, 'r').readlines()]
        else:
            blacklisted = ['']

        if self.barcode not in blacklisted:
            # dictionary to hold info to be written out to csv for each barcode
            results = {
                "PlateID": [],
                "BatchID": [],
                "WellNum": [],
                "ProfileID": [],
                "WellRowLetter": [],
                "WellColNum": [],
                "DropNum": [],
                "ImagerName": [],
                "DateImaged": [],
            }
            # connect to RockMaker DB
            conn = pytds.connect(
                self.server, self.database, self.username, self.password
            )
            c = conn.cursor()
            # select all data needed by inner joining a million tables on ID's - blame formulatrix
            c.execute(
                "SELECT DISTINCT "
                "w.PlateID, ib.ID, w.WellNumber, cpv.CaptureProfileID,  "
                "w.RowLetter, w.ColumnNumber, wd.DropNumber, "
                "crp.Value, it.DateImaged "
                "from Well w INNER JOIN Plate p ON w.PlateID = p.ID "
                "INNER JOIN WellDrop wd ON wd.WellID = w.id "
                "INNER JOIN ExperimentPlate ep ON ep.PlateID = p.ID "
                "INNER JOIN ImagingTask it ON it.ExperimentPlateID = ep.ID "
                "INNER JOIN ImageBatch ib ON ib.ImagingTaskID = it.ID "
                "INNER JOIN CaptureResult cr ON cr.ImageBatchID = ib.ID "
                "INNER JOIN CaptureProfileVersion cpv ON cpv.ID = cr.CaptureProfileVersionID "
                "INNER JOIN CaptureResultProperty crp on CaptureResultID = cr.ID "
                "WHERE p.Barcode=%s AND crp.Value like %s",
                (str(self.barcode), "%RI1000%"),
            )
            # add all info to results dict
            for row in c.fetchall():
                results["PlateID"].append(str(row[0]))
                results["BatchID"].append(str(row[1]))
                results["WellNum"].append(str(row[2]))
                results["ProfileID"].append(str(row[3]))
                results["WellRowLetter"].append(str(row[4]))
                results["WellColNum"].append(str(row[5]))
                results["DropNum"].append(str(row[6]))
                results["ImagerName"].append(str(row[7]))
                results["DateImaged"].append(str(row[8].date()))
            # write dict to output csv for image transfers
            df = pandas.DataFrame.from_dict(results)
            df.to_csv(self.output().path, index=False)
