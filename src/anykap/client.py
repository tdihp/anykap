"""anykap client that creates and manages workloads

Intended as a kubectl plugin
"""

import subprocess
import os
import re
import yaml  # we favor json whenever we can
import json
import shlex
from pathlib import Path, PosixPath
from importlib import resources as impresources
import warnings
import pprint
import logging
import tarfile
from anykap.envimporter import EnvImporter
import anykap

logger = logging.getLogger('kubectl-anykap')

NAME_PARSER = re.compile('[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?', flags=re.ASCII)
def name_type(data):
    if not NAME_PARSER.fullmatch(data):
        raise ValueError(f'name {data!r} is not conforming to requirement')
    return data

_env = EnvImporter().envname

MANIFEST = {
    #(local name,               workspace,              fmt,    in_cm,  comment),
    ('kustomization.yaml.fmt',  'kustomization.yaml',   True,   False,  False),
    ('daemonset.yaml.fmt',      'daemonset.yaml',       True,   False,  False),
    ('capture.py.fmt',          'capture.py',           True,   True,   False),
    ('__init__.py',             _env('anykap'),         False,  True,   False),
    ('azure.py',                _env('anykap.azure'),   False,  True,   True),
    ('envimporter.py',          'envimporter.py',       False,  True,   False),
    ('README.md.fmt',           'README.md',            True,   False,  False),
}


def find_kustomization_file(parser, args):
    path = args.kustomize
    if not path.is_dir():
        parser.exit(f'kustomize directory specified {path} is not a directory')
    for fname in ('kustomization.yaml', 'kustomization.yml', 'kustomization'):
        fpath = (path / fname)
        if fpath.is_file():
            return fpath

    parser.exit(f'kustomization file no found')

def kubectl(*args):
    command = ('kubectl',) + args
    logger.debug('executing command %r', command)
    result = subprocess.run(command,
                            env=os.environb, check=True, text=True,
                            stdout=subprocess.PIPE)
    return result.stdout


def repl_req(pod_locator, args, sockpath, chroot='/host'):
    """communicate with anykap.REPLServer"""
    nc_cmd = ('nc', '-CUN', sockpath)
    sh_cmd = ('/bin/sh', '-c',
              ('echo ' + shlex.quote(shlex.join(args)) + ' | '\
               + shlex.join(nc_cmd)))
    
    command = ('exec',) + pod_locator + ('--',)
    if chroot:
        command += ('chroot', chroot)

    command += sh_cmd  
    result = kubectl(*command)
    lines = result.rstrip().splitlines()
    if lines[0] != 'OK':
        raise RuntimeError(f'got non-ok: {result}')
    return lines[1:]


def tar_copy_artifact(pod_locator, path, outpath, chroot='/host'):
    """kubectl command to send a artifact path to stdout as gzipped tar"""
    command = ('kubectl', 'exec',) + pod_locator + ('--',)
    if chroot:
        command += ('chroot', chroot)
    command += ('tar', 'zc', '-C', str(path.parent), path.name)
    logger.debug('running tar command %r', command)
    p = subprocess.Popen(command, env=os.environb, stdout=subprocess.PIPE)
    with tarfile.open(fileobj=p.stdout, mode='r|*') as tf:
        tf.extractall(path=outpath)
    if p.wait() != 0:
        raise RuntimeError(f'tar copy failed, exit code {p.returncode}')


def generate_workspace(path, config):
    resources = impresources.files(__package__)
    config['cmfiles'] = '\n'.join(
        (('  # - ' if comment else '  - ') + repr(outname))
        for (_, outname, _, in_cm, comment) in MANIFEST if in_cm)
    config['version'] = anykap.__version__
    for (localname, outname, fmt, in_cm, comment) in MANIFEST:
        data = (resources / localname).read_text()
        if fmt:
            data = data.format(**config)
        (path / outname).write_text(data)


def cmd_init(parser, args, kubectl_args):
    initpath = Path(args.kustomize)
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


def get_metadata(parser, args):
    kfpath = find_kustomization_file(parser, args)
    with kfpath.open('r') as f:
        kustomize = yaml.safe_load(f)

    try:
        annotations = kustomize['metadata']['annotations']
        name = annotations['anykap/name']
        if not NAME_PARSER.fullmatch(name):
            parser.exit(f'invalid name {name!r} '
                        'specified in metadata.annotations')
    except KeyError:
        parser.exit(f'unable to find annotation anykap/name in metadata')

    result = dict((k.removeprefix('anykap/'), v)
                for k, v in annotations.items() if k.startswith('anykap/'))
    version = result.get('version')
    if (not version or version != anykap.__version__):
        warnings.warn('workspace created with different anykap version '
                      f'{version}, current anykap version {anykap.__version__}')
    return result


def get_covered(parser, args, kubectl_args):
    # return namespace, pods
    """get all covered nodes/pods with kubectl"""
    kfpath = find_kustomization_file(parser, args)
    path = kfpath.parent
    result = kubectl('get', '-k', str(path), '-o', 'json', *kubectl_args)
    output = json.loads(result)
    found = [d for d in output['items']
             if d['apiVersion'] == 'apps/v1'
             and d['kind'] == 'DaemonSet'
             and any(c['name'] == 'anykap'
                     for c in d['spec']['template']['spec']['containers'])]
    try:
        found_ds, = found
    except ValueError:
        parser.exit("expecting exactly 1 daemonset with a container 'anykap,'"
                    "found %d" % len(found))

    # we only support matchLabels for now
    namespace = found_ds['metadata']['namespace']
    selector = ','.join(
        map('='.join,found_ds['spec']['selector']['matchLabels'].items()))
    output = json.loads(kubectl(
        'get', 'pods', '-o', 'json', '-n', namespace, '-l', selector,
        *kubectl_args))
    return namespace, output['items']


def prep_repl_req(config):
    name = config['name']
    return {
        'chroot': config.get('chroot', ''),
        'sockpath': config.get('serverpath', f'/var/run/anykap-{name}.sock')
    }


def iter_nodes(parser, args, pods):
    node2pod = dict((pod['spec']['nodeName'], pod) for pod in pods)

    if args.nodes:
        nodes = set(node.strip() for node in args.nodes.split(','))
        notfound = nodes - set(node2pod.keys())
        if notfound:
            parser.exit('nodes specified not found: %r' % notfound)
        nodes = sorted(nodes)
    else:
        nodes = sorted(node2pod.keys())

    for node in nodes:
        yield node, node2pod[node]['metadata']['name']


def cmd_tasks(parser, args, kubectl_args):
    config = get_metadata(parser, args)
    req_kw = prep_repl_req(config)
    namespace, pods = get_covered(parser, args, kubectl_args)
    query = ('-ojson', 'tasks',)
    if args.regex:
        query += ('-r',)
    if args.stop:
        if not args.name:
            parser.exit('task name must be specified for stop')
        query += ('-s',)
    if args.all:
        query += ('-a',)
    if args.name:
        query += tuple(args.name)
    results = {}
    for node, pod in iter_nodes(parser, args, pods):
        try:
            result = repl_req(('-n', namespace, pod) + tuple(kubectl_args),
                              query, **req_kw)
            assert len(result) == 1
            results[node] = json.loads(result[0])['items']
        except Exception:
            logger.exception('failed querying for node %s (pod %s)', node, pod)
            continue
    pprint.pprint(results)


def copy_artifacts(parser, args, kubectl_args):
    # config = get_metadata(parser, args)
    # req_kw = prep_repl_req(config)
    # namespace, pods = get_covered(parser, args, kubectl_args)
    artifacts_path = (args.kustomize / 'artifacts')
    artifacts_path.mkdir(exist_ok=True)


def cmd_artifacts(parser, args, kubectl_args):
    config = get_metadata(parser, args)
    req_kw = prep_repl_req(config)
    namespace, pods = get_covered(parser, args, kubectl_args)
    query = ('-ojson', 'artifacts',)
    if args.regex:
        query += ('-r',)
    if args.all and not args.mark_uploaded and not args.copy:
        query += ('-a',)
    if args.mark_uploaded and not args.copy:
        query += ('--mark-uploaded',)
        if not args.name:
            parser.exit('artifact name must be specified for mark-uploaded')
    if args.copy:
        artifacts_path = (args.kustomize / 'artifacts')
        artifacts_path.mkdir(exist_ok=True)

    results = {}
    for node, pod in iter_nodes(parser, args, pods):
        try:
            result = repl_req(('-n', namespace, pod) + tuple(kubectl_args),
                              query, **req_kw)
            assert len(result) == 1
            artifacts = json.loads(result[0])['items']
            results[node] = artifacts
        except Exception:
            logger.exception('failed querying for node %s (pod %s)', node, pod)
            continue
        if args.copy:
            outcome = []
            results[node] = outcome
            for a in artifacts:
                if a['upload_state'] == 'completed':
                    logger.debug('skipping already uploaded artifact %s',
                                 a['name'])
                    continue
                try:

                    logger.info('copying artifact %s', a['name'])
                    tar_copy_artifact(
                        ('-n', namespace, pod) + tuple(kubectl_args),
                        PosixPath(a['path']), artifacts_path,
                        chroot=config['chroot'],)
                    response, = repl_req(
                        ('-n', namespace, pod) + tuple(kubectl_args),
                        ('-ojson', 'artifacts', '--mark-uploaded', a['name']),
                        **req_kw)
                    outcome.extend(json.loads(response)['items'])
                except Exception:
                    logger.exception('failed when copying node %s artifact %s',
                                     node, a['name'])
                    continue
    pprint.pprint(results)

def main():
    import argparse
    parser = argparse.ArgumentParser('kubectl-anykap')
    parser.add_argument('--verbosity', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='verbosity of kubectl-anykap. '
                             'for verbosity of kubectl, use "-v"')
    parser.add_argument('-n', '--namespace',
                        help='namespace specified for the command, ignored')
    parser.add_argument('-k', '--kustomize', type=Path, default='.', 
        help='path to initialize, defaults to current directory')
    nodes = argparse.ArgumentParser(add_help=False)
    nodes.add_argument('--nodes',
                       help='comma separated list of nodes, '
                            'defaults to all nodes')
    subpersers = parser.add_subparsers(title='command', required=True)
    init = subpersers.add_parser('init',
                               help='generates a kustomization directory')
    init.set_defaults(func=cmd_init)
    init.add_argument('name', type=name_type, help='name of the capture')
    commands = anykap.make_hq_replserver_parser(subpersers, parents=[nodes])
    commands['tasks'].set_defaults(func=cmd_tasks)
    commands['artifacts'].set_defaults(func=cmd_artifacts)
    commands['artifacts'].add_argument(
        '--copy', action='store_true',
        help='copy all completed and unuploaded artifacts to '
             '<kustomize>/artifacts/. this option marks all successful copies '
             'with "--mark-uploaded"')
    # we only parse known args, all unknown args are forwarded to kubectl
    # unless someone spot a reason we shouldn't do this
    args, kubectl_args = parser.parse_known_args()
    logging.basicConfig(level=getattr(logging, args.verbosity))
    logger.debug('kubectl args: %r', kubectl_args)
    if args.namespace:
        logger.warning('namespace %r is specified in commandline, ignored',
                       args.namespace)
    args.func(parser, args, kubectl_args)


if __name__ == '__main__':
    main()
