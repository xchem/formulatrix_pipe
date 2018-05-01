import luigi
import os
import pandas
from smb import SmbOperations
from get_barcodes import *
from config_classes import ImageTransferConfig


class TransferImage(luigi.Task):
    password = ImageTransferConfig().password
    username = ImageTransferConfig().username
    machine = ImageTransferConfig().machine
    options = ImageTransferConfig().options
    # rf = luigi.Parameter()
    rd = luigi.Parameter()
    lf = luigi.Parameter()
    ld = luigi.Parameter()
    drop_num = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.ld, self.lf))

    def run(self):
        smbobj = SmbOperations(username=self.username, password=self.password, machine=self.machine,
                               options=self.options)
        connection = smbobj.test_connection()

        if connection:
            smbobj = SmbOperations(username=self.username, password=self.password, machine=self.machine,
                                   options=self.options)

            pattern = str('d' + self.drop_num)
            out_list = smbobj.list_files(self.rd)
            for file in out_list:
                if '_ef.jpg' in file and pattern in file:
                    remote_filename = file

                    success = smbobj.get_file(local_directory=self.ld, local_filename=self.lf, remote_directory=self.rd,
                                              remote_filename=remote_filename)

                    if success:
                        print(str('Remote File ' + remote_filename + ' copied to (local) '
                                  + os.path.join(self.ld, self.lf)))


class TransferImages(luigi.Task):
    password = ImageTransferConfig().password
    username = ImageTransferConfig().username
    machine = ImageTransferConfig().machine
    options = ImageTransferConfig().options
    barcode = luigi.Parameter()
    csv_file = luigi.Parameter()
    plate_type = luigi.Parameter()

    def output(self):
        results = pandas.DataFrame.from_csv(self.csv_file, index_col=None)
        self.dates = list(set(results['DateImaged']))
        if len(self.dates) == 0:
            self.dates = ['empty']
        for date in self.dates:
            yield luigi.LocalTarget(str('transfers/' + self.barcode + '_' + date + '.done'))

    def requires(self):
        print(self.csv_file)
        lf = []
        ld = []
        # rf = []
        drop_num = []
        rd = []
        results = pandas.DataFrame.from_csv(self.csv_file, index_col=None)

        out_list = {

        }

        for i in range(0, len(results['PlateID'])):
            remote_filepath = '\\'.join(['WellImages',
                                         str(results['PlateID'][i]),
                                         str('plateID_' + str(results['PlateID'][i])),
                                         str('batchID_' + str(results['BatchID'][i])),
                                         str('wellNum_' + str(results['WellNum'][i])),
                                         str('profileID_' + str(results['ProfileID'][i])), '\\'
                                         ])

            imager_name = results['ImagerName'][i]
            num = format(int(results['WellColNum'][[i]]), '02d')
            col = str(results['WellRowLetter'][i])
            drop = str(results['DropNum'][i])
            date = str(results['DateImaged'][i])

            local_filename = str(self.barcode + '_' + num + col + '_' + drop + '.jpg')
            local_filepath = os.path.join('SubwellImages', str(self.barcode + '_' + imager_name + '_' + date + '_'
                                                               + self.plate_type))

            if not os.path.isdir(os.path.join(os.getcwd(), local_filepath)):
                os.makedirs(local_filepath)

            ld.append(local_filepath)
            lf.append(local_filename)
            rd.append(remote_filepath)
            drop_num.append(drop)

        return [TransferImage(ld=ld, lf=lf, rd=rd, drop_num=drop) for (ld, lf, rd, drop) in list(zip(ld, lf, rd,
                                                                                                     drop_num))]

    def run(self):
        for date in self.dates:
            with open(str('transfers/' + self.barcode + '_' + date + '.done'), 'w') as f:
                f.write('')