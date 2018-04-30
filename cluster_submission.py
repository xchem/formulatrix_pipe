import subprocess
import os


def submit_job(job_directory, job_script, remote_sub_command='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk', max_jobs=100):

    submission_string = ' '.join([
        remote_sub_command,
        '"',
        'cd',
        job_directory,
        '; module load global/cluster >>/dev/null 2>&1; qsub',
        job_script,
        '"'
    ])

    print(submission_string)

    submission = subprocess.Popen(submission_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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


def write_job(execute_directory, job_directory, job_filename, job_name, job_command):
    directory = os.getcwd()
    os.chdir(job_directory)
    job_script = '''#!/bin/bash
    cd %s
    %s
    ''' % (execute_directory, job_command)

    output = os.path.join(job_directory, job_filename)

    f = open(output, 'w')
    f.write(job_script)
    os.chdir(directory)
