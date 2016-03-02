import optparse

import sys

import ans.errors
import ans.runner
import ans.constants as C
from ans.utils import *


class Client(object):
    def __init__(self):
        pass

    def parse(self):
        parser = optparse.OptionParser(usage="ans <host-pattern> [options]")
        parser.add_option("-a", "--args", dest="module_args",
                          help="module arguments",
                          default=C.DEFAULT_MODULE_ARGS)
        parser.add_option("-M", "--module-path", dest="module_path",
                          help="path to module library",
                          default=C.DEFAULT_MODULE_PATH)
        parser.add_option("-m", "--module-name", dest="module_name",
                          help="module name to execute",
                          default=C.DEFAULT_MODULE_NAME)
        parser.add_option("-u", "--user", dest="remote_user",
                          help="connect as this user",
                          default=C.DEFAULT_REMOTE_USER)
        options, args = parser.parse_args()
        if len(args) == 0 or len(args) > 1:
            parser.print_help()
            exit(C.EXIT_FAILURE)
        return options, args

    def run(self, options, args):

        pattern = args[0]
        runner = ans.runner.Runner(
                module_name=options.module_name,
                module_path=options.module_path,
                module_args=options.module_args.split(),
                remote_user=options.remote_user,
                pattern=pattern,
                verbose=True
        )
        return runner, runner.run()

    def output(self, runner, results, options, args):

        if results is None:
            exit("No hosts matched")

        buf = ''
        for hostname in contacted_hosts(results):
            msg = host_report_msg(
                    hostname,
                    options.module_name,
                    contacted_host_result(results, hostname),
                    False
            )
            # if options.tree:
            #     write_tree_file(options.tree, hostname, bigjson(results))
            buf += msg

        if has_dark_hosts(results):
            buf += dark_hosts_msg(results)

        print buf


if __name__ == '__main__':
    client = Client()
    options, args = client.parse()
    try:
        runner, results = client.run(options, args)
    except ans.errors.AnsibleError as e:
        print e
        sys.exit(C.EXIT_FAILURE)
    else:
        client.output(runner, results, options, args)
