"""anykap client that creates and manages workloads

Intended as a kubectl plugin
"""

import subprocess
import os
import re
from pathlib import Path
from importlib import resources as impresources
from collections import namedtuple
import logging

logger = logging.getLogger('kubectl-anykap')

NAME_PARSER = re.compile('[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?', flags=re.ASCII)
def name_type(data):
    if not NAME_PARSER.fullmatch(data):
        raise ValueError(f'name {data!r} is not conforming to requirement')
    return data


MANIFEST = {
    #(local name,               workspace,              fmt,    in_cm,  comment),
    ('kustomization.yaml.fmt',  'kustomization.yaml',   True,   False,  False),
    ('daemonset.yaml',          'daemonset.yaml',       False,  False,  False),
    ('capture.py.fmt',          'capture.py',           True,   True,   False),
    ('__init__.py',             'PYMODULE__ANYKAP',     False,  True,   False),
    ('envimporter.py',          'envimporter.py',       False,  True,   False),
    ('README.md.fmt',           'README.md',            True,   False,  False),
}


def generate_workspace(path, config):
    resources = impresources.files(__package__)
    config['cmfiles'] = '\n'.join(
        (('  # - ' if comment else '  - ') + repr(outname))
        for (_, outname, _, in_cm, comment) in MANIFEST if in_cm)
    for (localname, outname, fmt, in_cm, comment) in MANIFEST:
        data = (resources / localname).read_text()
        if fmt:
            data = data.format(**config)
        (path / outname).write_text(data)


def cmd_init(parser, args):
    initpath = Path(args.path)
    if initpath.exists():
        if initpath.is_dir():
            if any(initpath.iterdir()):
                parser.exit(f'the path specified {initpath} is not empty')
        else:
            parser.exit(
                f'the path specified {initpath} exists and not a directory')
    else:
        try:
            initpath.mkdir(mode=0o755)
        except OSError as e:
            parser.exit(f'failed making directory {initpath} due to {e}')
    config = vars(args)
    print(f'config: {config}')
    generate_workspace(initpath, config)


def main():
    import argparse
    parser = argparse.ArgumentParser('kubectl-anykap')
    parser.add_argument('--verbosity',
                        help='verbosity of kubectl-anykap. '
                             'for verbosity of kubectl, use "-v"')
    commands = parser.add_subparsers(title='command', required=True)
    init = commands.add_parser('init', help='generates a kustomization directory')
    init.set_defaults(func=cmd_init)
    init.add_argument('name', type=name_type, help='name of the capture')
    init.add_argument('path', nargs='?', default='.',
                      help='path to initialize, defaults to current directory')
    tasks = commands.add_parser('tasks', aliases=['t', 'task'],
                                help='working with tasks')
    artifacts = commands.add_parser('artifacts', aliases=['a', 'artifact'],
                                    help='working with artifacts')
    send = commands.add_parser('send', help='send event to nodes')
    # we only parse known args, all unknown args are forwarded to kubectl
    # unless someone spot a reason we shouldn't do this
    args = parser.parse_args()
    args.func(parser, args)


if __name__ == '__main__':
    main()
