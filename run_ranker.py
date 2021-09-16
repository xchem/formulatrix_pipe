import luigi
import os
import glob
import cluster_submission
import subprocess
from get_barcodes import GetPlateTypes
from transfer_images import TransferImages
import smtplib
from email.mime.text import MIMEText
import time
import getpass


class BatchCheckRanker(luigi.Task):
    def output(self):
        return luigi.LocalTarget("checkrank.done")

    def requires(self):
        # get a list of all ranker jobs that have been produced by RunRanker
        files_list = glob.glob("ranker_jobs/*.sh")
        checklist = [x.replace("RANK_", "") for x in files_list]
        checklist = [x.replace(".sh", "") for x in checklist]
        checklist = [x.replace("ranker_jobs/", "") for x in checklist]
        # Check whether the .mat file expected has appeared
        return [CheckRanker(name=name) for name in checklist]

    def run(self):
        with self.output().open("w") as f:
            f.write("")


class CheckRanker(luigi.Task):
    name = luigi.Parameter()
    data_directory = luigi.Parameter(default="Data")
    extension = luigi.Parameter(default=".mat")
    # a list of people to email when a plate has been ranked
    emails = luigi.Parameter(
        default=[
            "rachael.skyner@diamond.ac.uk",
            "jose.brandao-neto@diamond.ac.uk",
            "daren.fearon@diamond.ac.uk",
            "ailsa.powell@diamond.ac.uk",
            "louise.dunnett@diamond.ac.uk",
            "tyler.gorrie-stone@diamond.ac.uk",
            "felicity.bertram@diamond.ac.uk",
        ]
    )

    def requires(self):
        pass

    def output(self):
        # a text version of the email sent is saved
        return luigi.LocalTarget(os.path.join("messages", str(self.name + ".txt")))

    def run(self):
        # what we expect the output from the ranker job to be
        expected_file = os.path.join(
            self.data_directory, str(self.name + self.extension)
        )
        # if it's not there, throw an error - might just not be finished... maybe change to distinguish(?)

        if not os.path.isfile(expected_file):
            time.sleep(5)
            if not os.path.isfile(expected_file):
                queue_jobs = []
                job = "ranker_jobs/RANK_" + self.name + ".sh"
                output = glob.glob(str(job + ".o*"))
                print(output)

                cuser = getpass.getuser()

                remote_sub_command = f"ssh -tt {cuser}@ssh.diamond.ac.uk"
                submission_string = " ".join(
                    [
                        # remote_sub_command,
                        '/bin/bash -c  "/dls_sw/cluster/GE/UGE8.6.7/bin/lx-amd64/qstat -r"',
                    ]
                )

                submission = subprocess.Popen(
                    submission_string,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                out, err = submission.communicate()

                output_queue = out.decode("ascii").split("\n")
                print(output_queue)
                for line in output_queue:
                    if "Full jobname" in line:
                        jobname = line.split()[-1]
                        queue_jobs.append(jobname)
                print(queue_jobs)
                if job.replace("ranker_jobs/", "") not in queue_jobs:
                    cluster_submission.submit_job(
                        job_directory=os.path.join(os.getcwd(), "ranker_jobs"),
                        job_script=job.replace("ranker_jobs/", ""),
                    )
                    print(
                        "The job had no output, and was not found to be running in the queue. The job has been "
                        "resubmitted. Will check again later!"
                    )
                if not queue_jobs:
                    raise Exception(
                        ".mat file not found for "
                        + str(self.name)
                        + "... something went wrong in ranker or job is still running"
                    )

        if os.path.isfile(expected_file):
            # message text for the email
            message_text = r"""This is an automated message from the formulatrix pipeline.
            
I'm just letting you know that the plate %s has been successfully ranked. You can now view the plate in TeXRank. To do this:

    1. Go to a windows machine or use tserver
    2. Open windows explorer or similar
    3. Point explorer to \\dc.diamond.ac.uk\dls\science\groups\i04-1\software\luigi_pipeline\formulatrix_pipe
    4. Click on TeXRankE
    5. In the dropdown menu on the right, look for %s
    6. Rank your plates
    """ % (
                " ".join(str(self.name).split("_")),
                " ".join(str(self.name).split("_")),
            )
            # write the message to a txt file
            with open(os.path.join("messages", str(self.name + ".txt")), "w") as f:
                f.write(message_text)
            # open the message as read
            fp = open(os.path.join("messages", str(self.name + ".txt")), "r")
            # read with email package
            msg = MIMEText(fp.read())
            fp.close()
            # set email subject, to, from
            msg["Subject"] = str("Ranker: " + self.name + " has been ranked!")
            msg["From"] = "formulatrix-pipe"
            msg["To"] = ",".join(self.emails)
            # use localhost as email server
            s = smtplib.SMTP("localhost")
            # send the email to everyone
            s.sendmail(msg["From"], self.emails, msg.as_string())
            s.quit()


class RunRanker(luigi.Task):
    imager = luigi.Parameter()
    plate = luigi.Parameter()
    plate_type = luigi.Parameter()
    run_rankerE_script = luigi.Parameter(
        default=os.path.join(os.getcwd(), "run_RankerE.sh")
    )
    MCR = luigi.Parameter(default="/dls/science/groups/i04-1/software/MCR/r2012a/v717/")

    def requires(self):
        return TransferImages(
            barcode=str(self.plate.split("_")[0]),
            plate_type=str(self.plate.split("_")[-1].split("-")[-1]),
            csv_file=os.path.join(
                os.getcwd(),
                str("barcodes_" + str(self.plate_type)),
                str(self.plate.split("_")[0] + ".csv"),
            ),
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join("ranker_jobs", str("RANK_" + self.plate + ".sh"))
        )

    def run(self):
        current_directory = os.getcwd()
        # dictionary to translate imager code and pipelines plate types to the right matlab file for ranker
        lookup = {
            "RI1000-0080_2drop": glob.glob("RI1000-0080-2drop*.mat"),
            "RI1000-0080_3drop": glob.glob("RI1000-0080-3drop*.mat"),
            "RI1000-0081_2drop": glob.glob("RI1000-0081-2drop*.mat"),
            "RI1000-0081_3drop": glob.glob("RI1000-0081-3drop*.mat"),
            "RI1000-0276_2drop": glob.glob("RI1000-0276-2drop*.mat"),
            "RI1000-0276_3drop": glob.glob("RI1000-0276-3drop*.mat"),
            "RI1000-0082_2drop": glob.glob("RI1000-0082-2drop*.mat"),
            "RI1000-0082_3drop": glob.glob("RI1000-0082-3drop*.mat"),
            "RI1000-0276_mitegen": glob.glob("RI1000-0276-MiTInSitu*.mat"),
            "RI1000-0082_mitegen": glob.glob("RI1000-0082-MiTInSitu*.mat"),
        }
        # define what we want to lookup in the above dict
        lookup_string = str(self.imager + "_" + self.plate_type)
        # get the right matlab files from the lookup dict (by key)
        if lookup_string in lookup.keys():
            mat_files = lookup[lookup_string]
        else:
            mat_files = (
                None  # raise exception if the lookup key is not defined (my fault)
            )
        # raise Exception('Either mat files do not exist, or are not defined in the lookup')
        if not mat_files:
            # raise exception if there are no mat files (your fault)
            raise Exception("Imager mat files not found!")
        # construct command to run ranker:
        # run_rankerE.sh <matlab runtime> <plate name in SubwellImages> <matfiles - separated by no spaces and commas>
        run_ranker_command = " ".join(
            [self.run_rankerE_script, self.MCR, self.plate, ",".join(mat_files)]
        )
        # write the job file to be run on the cluster
        cluster_submission.write_job(
            job_directory=os.path.join(current_directory, "ranker_jobs"),
            execute_directory=os.getcwd(),
            job_filename=str("RANK_" + self.plate + ".sh"),
            job_command=run_ranker_command,
            job_name=self.plate,
        )
        # submit the job to the cluster
        cluster_submission.submit_job(
            job_directory=os.path.join(current_directory, "ranker_jobs"),
            job_script=str("RANK_" + self.plate + ".sh"),
        )


class FindPlates(luigi.Task):
    subwell_directory = luigi.Parameter(default="SubwellImages")

    def output(self):
        return luigi.LocalTarget("findplates.done")

    def requires(self):
        # raise an error if there's no SubwellImages dir
        if not os.path.isdir(os.path.join(os.getcwd(), self.subwell_directory)):
            print(os.getcwd())
            os.mkdirs(os.path.join(os.getcwd(), self.subwell_directory))
            # raise Exception('No Subwell Directory found!')
        # Find the names of all plates - the subdirectories in SubwellImages
        plate_directories = [
            x[0]
            for x in os.walk(os.path.join(os.getcwd(), self.subwell_directory))
            if os.path.join(os.getcwd(), "SubwellImages") not in x
        ]
        imagers = []
        plate_types = []
        plates = []
        for plate in plate_directories:
            components = plate.split("/")
            plate_components = components[-1].split("_")

            imagers.append(
                str(plate_components[-1].split("-")[0])
                + "-"
                + plate_components[-1].split("-")[1]
            )
            plate_types.append(plate_components[-1].split("-")[2])
            plates.append(components[-1])

        yield GetPlateTypes()
        yield [
            RunRanker(plate=plate, plate_type=plate_type, imager=imager)
            for (plate, plate_type, imager) in list(zip(plates, plate_types, imagers))
        ]

    def run(self):
        with self.output().open("w") as f:
            f.write("")
