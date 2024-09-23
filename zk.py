import os
import sys
import cmd
import random

from argparse import ArgumentParser
from os.path import normpath
from pathlib import PurePosixPath

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NotEmptyError


def parse_arg(arg: str):
    if not arg:
        return []

    return arg.split(' ')


def set_prompt(path):
    cmd.Cmd.prompt = 'zk: {}> '.format(path)


class ZookeeperCLI(cmd.Cmd):
    def __init__(self, zk: KazooClient, current_path: PurePosixPath):
        super().__init__(completekey='tab')
        if not zk: raise ValueError('zk must not be none')
        if not current_path: raise ValueError('current_path must not be none')
        self.zk = zk
        self.current_path = current_path
        self.editor = os.getenv('EDITOR')
        set_prompt(current_path.as_posix())

    def do_mkdir(self, arg):
        args = parse_arg(arg)

        if not args:
            print('At least one argument must be give')
            return

        for path in args:
            self.zk.ensure_path(self._realpath(path))

    def do_rm(self, arg):
        args = parse_arg(arg)
        recursive = args.count('-r')
        paths = list(filter(lambda _: not _.startswith('-'), args))
        if not args:
            print('At least one argument must be give')

        for path in paths:
            try:
                self.zk.delete(self._realpath(path), recursive=bool(recursive))
            except NotEmptyError:
                print(f"'{path}' is not empty, use -r for recursive deletions")

    def do_ls(self, arg):
        for child in self._ls(arg):
            print(child)

    def _ls(self, arg):
        paths = parse_arg(arg)
        children = []
        try:
            if not paths:
                return self.zk.get_children(self._pwd())

            for path in paths:
                relative = self._realpath(path)
                children.extend(self.zk.get_children(relative))
        except NoNodeError:
            pass

        return children

    def do_edit(self, arg):
        args = parse_arg(arg)
        if len(args) != 1:
            print('Exactly one node must be given')
            return

        file = args[0]
        file = self._realpath(file)

        try:
            data, _ = self.zk.get(file)

            tmp_file = f"/tmp/zk-{random.randrange(sys.maxsize)}-{file.split('/')[-1]}"
            with open(tmp_file, 'w+') as tmp_fp:
                if data:
                    data = data.decode('utf-8')
                    tmp_fp.write(data)
                    tmp_fp.flush()

                self._open_editor(tmp_file)
                tmp_fp.seek(0)
                changed_data = tmp_fp.read()
                if changed_data != data:
                    self.zk.set(file, changed_data.encode())
                else:
                    print(f'No changes made on {file}')

                os.remove(tmp_file)

        except NoNodeError:
            print(f'Node not found: {file}')
            return

    def do_cat(self, arg):
        args = parse_arg(arg)
        if not args:
            print('At least a node must me given')
            return

        for file in args:
            file = self._realpath(file)

            try:
                data, _ = self.zk.get(file)
            except NoNodeError:
                print(f'Node not found: {file}')
                return

            if data:
                print(data.decode('utf-8'))

    def do_cd(self, arg):
        args = parse_arg(arg)

        if not args or len(args) > 1:
            print('cd exactly one argument')
            return

        realpath = self._realpath(args[0])
        self.current_path = PurePosixPath(realpath)
        set_prompt(realpath)

    def do_pwd(self, _):
        print(self._pwd())

    def do_exit(self, _):
        self.zk.stop()
        exit()

    def do_editor(self, _):
        print(self.editor)

    def do_set_editor(self, arg):
        args = parse_arg(arg)
        if len(args) != 1:
            print('Exactly one argument must be give')
            return

        self.editor = args[0]

    def completedefault(self, text, *args):
        return [child for child in self._ls(None) if child.startswith(text)]

    def cmdloop(self, intro=None):
        if intro: print(intro)

        while True:
            try:
                super(ZookeeperCLI, self).cmdloop()
                break
            except KeyboardInterrupt:
                print()

    def _open_editor(self, file):
        if not self.editor:
            print('No default edit found, use set_editor to choose your default editor first')
            return

        os.system(f'{self.editor} {file}')

    def _realpath(self, path):
        path = PurePosixPath(path) if path.startswith('/') else self.current_path.joinpath(path)
        return normpath(path.as_posix())

    def _pwd(self):
        return self.current_path.as_posix()


def main():
    parser = ArgumentParser()
    parser.add_argument('hosts', default='')
    parser.add_argument('cmd', nargs='?', default=None)
    parser.add_argument('--default_path', required=False, default='/')

    args, sys_args = parser.parse_known_args()

    current_path = PurePosixPath(args.default_path)
    zk = KazooClient(hosts=args.hosts)
    zk.start()

    cli = ZookeeperCLI(zk, current_path)

    if args.cmd:
        line = f"{args.cmd} {' '.join(sys_args)}"
        return cli.onecmd(line)

    cli.cmdloop()


if __name__ == "__main__":
    main()
