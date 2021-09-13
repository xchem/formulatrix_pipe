import luigi
import os
import subprocess
import getpass


def run_luigi_worker(task):
    # initialise a worker to run task
    w = luigi.worker.Worker()
    # schedule task
    w.add(task)
    # run task - returns True if job is run successfully by worker
    status = w.run()

    return status


def kill_job(directory, id_file):
    with open(os.path.join(directory, id_file), 'r') as f:
        job_id = int(f.readline())

    cuser = getpass.getuser()

    # cancel cluster job so that test of sh can be run locally
    process = subprocess.Popen(str('ssh -t ' + cuser + '@nx.diamond.ac.uk '
                                   '"module load global/cluster >>/dev/null 2>&1; qdel ' + str(job_id) + '"'),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    out, err = process.communicate()

    return out, job_id
