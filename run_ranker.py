import luigi
import os
import glob
import cluster_submission
from transfer_images import TransferImages

class RunRanker(luigi.Task):
    imager = luigi.Parameter()
    plate = luigi.Parameter()
    plate_type = luigi.Parameter()
    run_rankerE_script = luigi.Parameter(default=os.path.join(os.getcwd(), 'run_RankerE.sh'))
    MCR = luigi.Parameter(default='/dls/science/groups/i04-1/software/MCR/r2012a/v717/')

    def requires(self):
        pass

    def run(self):
        current_directory = os.getcwd()

        mat_files = []
        if 'RI1000-0080' in self.imager and '2drop' in self.plate_type:
            mat_files = glob.glob('rcMiddle-mrc2d*.mat')
        if 'RI1000-0080' in self.imager and '3drop' in self.plate_type:
            mat_files = glob.glob('rcMiddle-sci3d*.mat')

        if 'RI1000-0081' in self.imager and '2drop' in self.plate_type:
            mat_files = glob.glob('rcLeft-mrc2d*.mat')
        if 'RI1000-0081' in self.imager and '3drop' in self.plate_type:
            mat_files = glob.glob('rcLeft-sci3d*.mat')

        if 'RI1000-0276' in self.imager and '2drop' in self.plate_type:
            mat_files = glob.glob('rcRight-mrc2d*.mat')
        if 'RI1000-0276' in self.imager and '3drop' in self.plate_type:
            mat_files = glob.glob('rcRight-sci3d*.mat')

        if 'RI1000-0082' in self.imager and '2drop' in self.plate_type:
            mat_files = glob.glob('rcCold-mrc2d*.mat')
        if 'RI1000-0082' in self.imager and '3drop' in self.plate_type:
            mat_files = glob.glob('rcCold-sci3d*.mat')

        if not mat_files:
            raise Exception('Imager mat files not found!')

        run_ranker_command = ' '.join([self.run_rankerE_script, self.MCR, self.plate, ','.join(mat_files)])

        cluster_submission.write_job(job_directory=os.path.join(current_directory, 'ranker_jobs'),
                                     execute_directory=os.getcwd(),
                                     job_filename=str('RANK_' + self.plate + '.sh'),
                                     job_command=run_ranker_command, job_name=self.plate)

        cluster_submission.submit_job(job_directory=os.path.join(current_directory, 'ranker_jobs'),
                                      job_script=str('RANK_' + self.plate + '.sh'))


class FindPlates(luigi.Task):
    subwell_directory = luigi.Parameter(default='SubwellImages')

    def requires(self):
        if not os.path.isdir(os.path.join(os.getcwd(), self.subwell_directory)):
            print(os.getcwd())
            raise Exception('No Subwell Directory found!')
        plate_directories = [x[0] for x in os.walk(os.path.join(os.getcwd(), self.subwell_directory))
                  if os.path.join(os.getcwd(), 'SubwellImages') not in x]
        imagers = []
        plate_types=[]
        plates=[]
        for plate in plate_directories:
            components = plate.split('/')
            plate_components = components[-1].split('_')

            imagers.append(plate_components[1])
            plate_types.append(plate_components[-1])
            plates.append(components[-1])

        return[RunRanker(plate=plate, plate_type=plate_type, imager=imager) for (plate, plate_type, imager)
               in list(zip(plates, plate_types, imagers))]
