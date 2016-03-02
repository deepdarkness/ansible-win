import os

import paramiko


class Connection(object):
    def __init__(self, runner, transport):
        self.runner = runner
        self.transport = transport

    def connect(self, host):
        conn = None
        if self.transport == 'paramiko':
            conn = ParamikoConnection(self.runner, host)
        if conn is None:
            raise Exception("Unsupported connection type")
        return conn.connect()


class ParamikoConnection(object):
    def __init__(self, runner, host):
        self.ssh = None
        self.runner = runner
        self.host = host

    def connect(self):

        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(
                    self.host,
                    username=self.runner.remote_user,
                    allow_agent=True,
                    look_for_keys=True,
                    password=self.runner.remote_pass,
                    timeout=self.runner.timeout
            )
        except Exception as e:
            raise AnsibleConnectionException(str(e))
        return self

    def exec_command(self, cmd):
        """
        run command on the remote host
        :param cmd: cmd string
        :return: (stdin,stdout,stderr)
        """
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdin, stdout, stderr

    def put_file(self,in_path,out_path):
        if not os.path.exists(in_path):
            raise AnsibleConnectionException("file or module dose not exist: %s" % in_path)
        sftp=self.ssh.open_sftp()
        try:
            sftp.put(in_path,out_path)
        except IOError:
            raise AnsibleConnectionException("failed to transfer file to %s" % out_path)
        sftp.close()


    def close(self):
        self.ssh.close()


class AnsibleConnectionException(Exception):
    ''' Subclass of exception for catching in Runner() code '''

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
