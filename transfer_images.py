import luigi
import os, shutil
import pandas
from smb import SmbOperations
from get_barcodes import *
from config_classes import ImageTransferConfig
import glob
import warnings


class TransferImage(luigi.Task):
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
        file_list = glob.glob(str(self.rd + '/*'))
        print(file_list)
        pattern = str('d' + self.drop_num)

        for file in file_list:
            # get the effective focus image (denoted by _ef) for the target drop (pattern from above)
            if '_ef.jpg' in file and pattern in file:
                remote_filename = file
                if not os.path.isfile(remote_filename):
                    raise Exception(f'Expected image file {remote_filename} does not exist. Ask staff to check plate has imaged correctly')
                shutil.copy(os.path.join(self.rd, remote_filename), os.path.join(self.ld, self.lf))


class TransferImages(luigi.Task):
    barcode = luigi.Parameter()
    # csv file from GetBarcodeInfo
    csv_file = luigi.Parameter()
    # 2drop or 3drop
    plate_type = luigi.Parameter()
    mount_path = luigi.Parameter('/mnt/rockimager/rockimager/RockMakerStorage/WellImages')

    def output(self):
        if not os.path.isfile(self.csv_file):
            warnings.warn(f'CSV file for {self.barcode} not yet created...')
            return None
        # read the csv file output from GetBarcodeInfo
        results = pandas.read_csv(self.csv_file, index_col=None)
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
        if not os.path.isfile(self.csv_file):
            warnings.warn(f'CSV file for {self.barcode} not yet created...')
            return None
        results = pandas.read_csv(self.csv_file, index_col=None)


        # make sure the number of images detected is divisible by 96 (i.e. the whole plate has been imaged)
        if len(results['PlateID'])/96 != int(len(results['PlateID'])/96):
            raise Exception('Number of images not divisible by 96... some images missing?')

        for i in range(0, len(results['PlateID'])):
            # construct expected filepath on remote storage from info gathered from RockMaker DB

            mounted_path = os.path.join(self.mount_path,
                                        str(int(str(results['PlateID'][i])[-3:])),
                                        str('plateID_' + str(results['PlateID'][i])),
                                        str('batchID_' + str(results['BatchID'][i])),
                                        str('wellNum_' + str(results['WellNum'][i])),
                                        str('profileID_' + str(results['ProfileID'][i]))
                                        )

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
            rd.append(mounted_path)
            drop_num.append(drop)
        # run each image transfer as a separate task (above)
        return [TransferImage(ld=ld, lf=lf, rd=rd, drop_num=drop) for (ld, lf, rd, drop) in list(zip(ld, lf, rd,
                                                                                                     drop_num))]

    def run(self):
        if not os.path.isfile(self.csv_file):
            pass
        else:
            for (date, imager) in self.dates_imagers:
                # write the output files to show all images have been transferred
                with open(str('transfers/' + self.barcode + '_' + date + '_' + imager + '_' +
                                            self.plate_type + '.done'), 'w') as f:
                    f.write('')


# placeholder: check each directory and see if a new image has appeared in the last 20 minutes. If not, and you can't
# divide no. of images by 96, then skip everything for that plate (add to an exception list?)
class CheckImageDirs(luigi.Task):
    pass
