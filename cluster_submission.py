import subprocess
import os


def submit_job(job_directory, job_script, remote_sub_command='ssh -tt uzw12877@ssh.diamond.ac.uk', max_jobs=100):
    current = os.getcwd()
    # change = '/dls/science/groups/i04-1/software/luigi_pipeline/formulatrix_pipe/ranker_jobs/'
    os.chdir(job_directory)
    submission_string = str(f'module load global/cluster; qsub -q medium.q /dls/science/groups/i04-1/software/luigi_pipeline/formulatrix_pipe/ranker_jobs/{job_script}').split()

    print(submission_string)
    #os.system(submission_string)
    submission = subprocess.Popen(submission_string,
                                        stdout = subprocess.PIPE,
                                        stderr = subprocess.PIPE,
                                        )
    out, err = submission.communicate()

    out = out.decode('ascii')
    print('\n')
    print(out)
    print('\n')
    if err:
        err = err.decode('ascii')
        print('\n')
        print(err)
        print('\n')
    os.chdir(current)

def write_job(execute_directory, job_directory, job_filename, job_name, job_command):
    directory = os.getcwd()
    os.chdir(job_directory)
    job_script = '''#!/bin/bash
cd %s
export MCR_CACHE_ROOT=$( mktemp -d )
%s
    ''' % (execute_directory, job_command)

    output = os.path.join(job_directory, job_filename)

    f = open(output, 'w')
    f.write(job_script)
    os.chdir(directory)
