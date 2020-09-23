import subprocess
import os


def submit_job(job_directory, job_script, remote_sub_command='ssh -tt uzw12877@ssh.diamond.ac.uk', max_jobs=100):
    current = os.getcwd()
    os.chdir(job_directory)
    submission_string = f'qsub -q medium.q {job_directory}/{job_script}'

    print(submission_string)
    proc = subprocess.run(submission_string.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = proc.stdout
    err = proc.stderr

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
