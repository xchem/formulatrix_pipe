import luigi
import os
import pandas
from smb import SmbOperations
from get_barcodes import *
from config_classes import ImageTransferConfig


class TransferImage(luigi.Task):
    # options for connection defined in luigi.config
    password = ImageTransferConfig().password
    username = ImageTransferConfig().username
    machine = ImageTransferConfig().machine
    options = ImageTransferConfig().options
    # ld = local dir, lf = local file, rd = remote dir
    rd = luigi.Parameter()
    lf = luigi.Parameter()
    ld = luigi.Parameter()
    # drop number to find correct image
    drop_num = luigi.Parameter()

    def output(self):
        # output is simply the file that has been transferred
        return luigi.LocalTarget(os.path.join(self.ld, self.lf))

    def run(self):
        # connect to the smbclient
        smbobj = SmbOperations(username=self.username, password=self.password, machine=self.machine,
                               options=self.options)
        # make sure that the connection is working
        connection = smbobj.test_connection()

        if connection:
            # pattern for prefix of file name eg. drop 1 = d1_*
            pattern = str('d' + self.drop_num)
            # get a list of files in the remote target directory
            out_list = smbobj.list_files(self.rd)
            for file in out_list:
                # get the effective focus image (denoted by _ef) for the target drop (pattern from above)
                if '_ef.jpg' in file and pattern in file:
                    remote_filename = file
                    # get the remote file and check that it has transferred successfully
                    success = smbobj.get_file(local_directory=self.ld, local_filename=self.lf, remote_directory=self.rd,
                                              remote_filename=remote_filename)

                    if success:
                        print(str('Remote File ' + remote_filename + ' copied to (local) '
                                  + os.path.join(self.ld, self.lf)))
        # Throw an error if no connection made
        else:
            raise Exception('Unable to connect to smbclient, make sure you are running on the correct machine!')


class TransferImages(luigi.Task):
    password = ImageTransferConfig().password
    username = ImageTransferConfig().username
    machine = ImageTransferConfig().machine
    options = ImageTransferConfig().options
    barcode = luigi.Parameter()
    # csv file from GetBarcodeInfo
    csv_file = luigi.Parameter()
    # 2drop or 3drop
    plate_type = luigi.Parameter()

    def output(self):
        # read the csv file output from GetBarcodeInfo
        results = pandas.DataFrame.from_csv(self.csv_file, index_col=None)
        # separate the transfers by date - some plates may have been imaged on multiple days
        dates = results['DateImaged']
        imagers = results['ImagerName']

        self.dates_imagers = list(set(zip(dates, imagers)))
        if len(dates) == 0:
            self.dates_imagers = [('empty', '')]

        # catch plates which have not been imaged yet
        # produce a file for each transfer (should only differ by date imaged, not plate type obvs)
        for (date, imager) in self.dates_imagers:
            yield luigi.LocalTarget(str('transfers/' + self.barcode + '_' + date + '_' + imager + '_' +
                                        self.plate_type + '.done'))

    def requires(self):
        lf = []
        ld = []
        drop_num = []
        rd = []
        results = pandas.DataFrame.from_csv(self.csv_file, index_col=None)

        if len(results['PlateID'])/96 != int(len(results['PlateID'])/96):
            raise Exception('Number of images not divisible by 96... some images missing?')

        for i in range(0, len(results['PlateID'])):
            # construct expected filepath on remote storage from info gathered from RockMaker DB
            remote_filepath = '\\'.join(['WellImages',
                                         str(results['PlateID'][i])[-3:],
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
            # local filename: barcode_well-number_well-letter_drop-number.jpg
            local_filename = str(self.barcode + '_' + num + col + '_' + drop + '.jpg')
            # local filepath: SubwellImages/barcode_date_imager-platetype
            local_filepath = os.path.join('SubwellImages', str(self.barcode + '_' + date + '_' + imager_name + '-' +
                                                               self.plate_type))
            # make the directory if it doesn't exits
            if not os.path.isdir(os.path.join(os.getcwd(), local_filepath)):
                os.makedirs(local_filepath)

            ld.append(local_filepath)
            lf.append(local_filename)
            rd.append(remote_filepath)
            drop_num.append(drop)
        # run each image transfer as a separate task (above)
        return [TransferImage(ld=ld, lf=lf, rd=rd, drop_num=drop) for (ld, lf, rd, drop) in list(zip(ld, lf, rd,
                                                                                                     drop_num))]

    def run(self):
        for (date, imager) in self.dates_imagers:
            # write the output files to show all images have been transferred
            with open(str('transfers/' + self.barcode + '_' + date + '_' + imager + '_' +
                                        self.plate_type + '.done'), 'w') as f:
                f.write('')