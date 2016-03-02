import fnmatch
import json
import os
import random

import multiprocessing

import signal

import ans.connection
import ans.constants as C
from ans.errors import AnsibleInventoryNotFoundError


def _execute_hook(job_queue, result_queue):
    import Queue
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    while not job_queue.empty():
        try:
            job = job_queue.get(block=False)
            runner, host = job
            result_queue.put(runner.executor(host))
        except Queue.Empty:
            pass


class Runner(object):
    def __init__(self,
                 host_list=C.DEFAULT_HOST_LIST,
                 module_path=C.DEFAULT_MODULE_PATH,
                 module_name=C.DEFAULT_MODULE_NAME,
                 module_args=C.DEFAULT_MODULE_ARGS,
                 forks=5,
                 timeout=10,
                 pattern=C.DEFAULT_PATTERN,
                 remote_user=C.DEFAULT_REMOTE_USER,
                 remote_pass=C.DEFAULT_REMOTE_PASS,
                 background=0,
                 basedir=None,
                 setup_cache=None,
                 transport="paramiko",
                 verbose=False):
        if setup_cache is None:
            setup_cache = {}
        self.setup_cache = setup_cache
        self.host_list, self.groups = self.parse_hosts(host_list)
        self.module_path = module_path
        self.module_name = module_name
        self.forks = int(forks)
        self.pattern = pattern
        self.module_args = module_args
        self.timeout = timeout
        self.verbose = verbose
        self.remote_user = remote_user
        self.remote_pass = remote_pass
        self.background = background

        if basedir is None:
            basedir = os.getcwd()
        self.basedir = basedir

        self._tmp_paths = {}

        random.seed()
        self.generated_jid = str(random.randint(0, 999999999999))
        self.connector = ans.connection.Connection(self, transport)

    @classmethod
    def parse_hosts(cls, host_list):
        """
        parse the host inventory file ,exp:
        [groupname]
        host1
        host2
        :param host_list: the path of host name file
        :return: hosts,groups
        """
        host_list = os.path.expanduser(host_list)
        if not os.path.exists(host_list):
            raise AnsibleInventoryNotFoundError(host_list)

        lines = file(host_list).read().split("\n")
        groups = {"ungrouped": []}
        group_name = "ungrouped"
        results = []
        for item in lines:
            item = item.strip()
            if item.startswith("#"):
                continue
            elif item.startswith("["):
                group_name = item.replace("[", "").replace("]", "").strip()
                groups[group_name] = []
            else:
                groups[group_name].append(item)
                results.append(item)
        return results, groups

    def run(self):
        hosts = self.match_hosts()
        if len(hosts) == 0:
            return {
                "contacted": {},
                "dark": {}
            }
        hostsp = [(self, x) for x in hosts]
        if self.forks > 1:
            job_queue = multiprocessing.Queue()
            result_queue = multiprocessing.Queue()
            for i in hostsp:
                job_queue.put(i)
            workers = []
            for i in range(self.forks):
                tmp = multiprocessing.Process(target=_execute_hook, args=(job_queue, result_queue))
                tmp.start()
                workers.append(tmp)
            try:
                for worker in workers:
                    worker.join()
            except KeyboardInterrupt:
                for worker in workers:
                    worker.terminate()
                    worker.join()
            results = []
            while not result_queue.empty():
                results.append(result_queue.get(block=False))
        else:
            results = [self.executor(h) for h in hosts]

        # handle output
        results2 = {
            "contacted": {},
            "dark": {}
        }
        hosts_with_result = []  # ok host 
        for x in results:
            host, is_ok, result = x
            hosts_with_result.append(host)
            if not is_ok:
                results2["dark"][host] = result
            else:
                results2["contacted"][host] = result
        return results2

    def match_hosts(self):
        return [h for h in self.host_list if self.__matches(h, self.pattern)]

    @staticmethod
    def remote_log(conn, msg):
        conn.exec_command("/usr/bin/logger -t ansible -p auth.info \"%s\"" % msg)

    def __matches(self, host_name, pattern):
        if host_name == '':
            return False
        pattern = pattern.replace(";", ":")
        subpatterns = pattern.split(":")
        for subpattern in subpatterns:
            if subpattern == "all":
                return True
            if fnmatch.fnmatch(host_name, subpattern):
                return True
            if subpattern in self.groups:
                if host_name in self.groups[subpattern]:
                    return True
        return False

    def executor(self, host):
        """
        :return: (hostname,connected_ok,extra)
        where extra is the result of a successful connect
        or a traceback string
        """
        ok, conn = self.__connect(host)
        if not ok:
            return [host, False, conn]
        tmp = self.__get_tmp_path(conn)
        # TODO :: some other module are not supported
        if self.background == 0:
            result = self.__execute_normal_module(conn, host, tmp)
        else:
            # result = self.__execute_async_module(conn, host, tmp)
            raise AssertionError("anync unsupported.")
        self.__delete_remote_files(conn, tmp)
        conn.close()

        return result

    def __connect(self, host):
        try:
            return [True, self.connector.connect(host)]
        except ans.connection.AnsibleConnectionException as e:
            return [False, "FAILED: %s" % str(e)]

    def __get_tmp_path(self, conn):
        result = self.__exec_command(conn, "mktemp -d /tmp/ansible.XXXXXXXX")
        return result.split("\n")[0] + "/"

    def __exec_command(self, conn, cmd):
        msg = "%s:%s" % (self.module_name, cmd)
        self.remote_log(conn, msg)
        stdin, stdout, stderr = conn.exec_command(cmd)
        results = "\n".join(stdout.readlines())
        return results

    def execute_cmd_for_debug(self,conn,cmd):
        return self.__exec_command(conn,cmd)

    def __delete_remote_files(self, conn, tmp):
        self.__exec_command(conn, "rm -rf %s " % tmp)

    def __execute_normal_module(self, conn, host, tmp):
        # module return a exec module file path to execute
        file_path = self.__transfer_module(conn, tmp)
        result = self.__execute_module(conn, tmp, file_path)
        return self.__return_from_module(host, result)

    def __transfer_module(self, conn, tmp):
        outpath = self.__copy_module(conn, tmp)
        self.__exec_command(conn, "chmod +x %s" % outpath)
        return outpath

    def __execute_module(self, conn, tmp, exec_file_path):
        args = self.module_args
        if type(args) == list:
            args = [str(x) for x in self.module_args]
            args = ' '.join(args)

        # todo :: for the args of module file [ignore]
        cmd = "%s %s " % (exec_file_path, args)
        result = self.__exec_command(conn, cmd)
        return result

    def __copy_module(self, conn, tmp):
        if self.module_name.startswith("/"):
            raise Exception("%s is not a module" % self.module_name)
        in_path = self.module_path + self.module_name
        if not os.path.exists(in_path):
            raise Exception("module not found: %s" % in_path)
        out_path = tmp + self.module_name
        conn.put_file(in_path, out_path)
        return out_path

    @staticmethod
    def __return_from_module(host, result):
        """
        :return [host,is-json-type,string]
        """
        try:
            # try to parse the JSON response
            return [host, True, json.loads(result)]
        except Exception, e:
            # it failed, say so, but return the string anyway
            return [host, False, "%s/%s" % (str(e), result)]
