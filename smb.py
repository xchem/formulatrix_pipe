import subprocess
import os


class SmbOperations:
    def __init__(self, username, password, machine, options):
        # set up config for connection
        self.username = username
        self.password = password
        self.machine = machine
        self.options = options
        # test connection string - connect with smb client and issue quit command
        self.connect_string = ' '.join(['smbclient', self.options, '-U', self.username, self.machine, self.password,
                                        '-c', '"quit"'])

    def test_connection(self):
        # use subprocess call to test connection string in __init__
        process = subprocess.Popen(self.connect_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()

        # if there's any error at all, throw back a failed connection
        if 'ERR' in out.decode('ascii') or 'ERR' in err.decode('ascii'):
            return False
        else:
            return True

    def list_files(self, remote_directory):
        # list the files in a remote directory (specified)
        retrieve_command = ' '.join(['-c', '"', 'ls', remote_directory, '"'])
        print(' '.join([self.connect_string, retrieve_command]))
        process = subprocess.Popen(' '.join([self.connect_string, retrieve_command]),
                                   shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = process.communicate()

        # return a list of files to search
        file_list = out.decode('ascii').split()

        return file_list

    def get_file(self, local_directory, remote_directory, local_filename, remote_filename):

        # copy a file with given name to a local location specified
        retrieve_command = ' '.join(['-c', '"', 'lcd', local_directory, ';', 'cd', remote_directory, ';', 'get',
                                                                   remote_filename, local_filename, '"'])

        process = subprocess.Popen(' '.join([self.connect_string, retrieve_command]),
                                   shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = process.communicate()
        print(out)
        print(err)

        # check that the file has copied successfully, and return true
        if os.path.isfile(os.path.join(local_directory, local_filename)):
            return True
        else:
            return False

