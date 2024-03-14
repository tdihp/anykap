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
import argparse
from anykap.envimporter import EnvImporter
import anykap

logger = logging.getLogger("kubectl-anykap")

NAME_PARSER = re.compile("[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?", flags=re.ASCII)


def name_type(data):
    if not NAME_PARSER.fullmatch(data):
        raise ValueError(f"name {data!r} is not conforming to requirement")
    return data


_env = EnvImporter().envname

# fmt: off
MANIFEST = {
    #(local name,               workspace,              fmt,    in_cm,  comment),
    ("kustomization.yaml.fmt",  "kustomization.yaml",   True,   False,  False),
    ("daemonset.yaml.fmt",      "daemonset.yaml",       True,   False,  False),
    ("capture.py",              "capture.py",           False,  True,   False),
    ("__init__.py",             _env("anykap"),         False,  True,   False),
    ("azure.py",                _env("anykap.azure"),   False,  True,   True),
    ("envimporter.py",          "envimporter.py",       False,  True,   False),
    ("README.md.fmt",           "README.md",            True,   False,  False),
}
# fmt: on


def generate_workspace(path, config):
    resources = impresources.files(__package__)
    config["cmfiles"] = "\n".join(
        (("  # - " if comment else "  - ") + repr(outname))
        for (_, outname, _, in_cm, comment) in MANIFEST
        if in_cm
    )
    config["version"] = anykap.__version__
    for localname, outname, fmt, in_cm, comment in MANIFEST:
        data = (resources / localname).read_text()
        if fmt:
            data = data.format(**config)
        (path / outname).write_text(data)


def kubectl(*args: str):
    command = ("kubectl",) + args
    logger.debug("executing command %r", command)
    result = subprocess.run(
        command, env=os.environb, check=True, text=True, stdout=subprocess.PIPE
    )
    return result.stdout


def repl_req(pod_locator, args, sockpath, chroot="/host"):
    """communicate with anykap.REPLServer"""
    nc_cmd = ("nc", "-CUN", sockpath)
    sh_cmd = (
        "/bin/sh",
        "-c",
        ("echo " + shlex.quote(shlex.join(args)) + " | " + shlex.join(nc_cmd)),
    )

    command = ("exec",) + pod_locator + ("--",)
    if chroot:
        command += ("chroot", chroot)

    command += sh_cmd
    result = kubectl(*command)
    lines = result.rstrip().splitlines()
    if lines[0] != "OK":
        raise RuntimeError(f"got non-ok: {result}")
    return lines[1:]


def tar_copy_artifact(pod_locator, path, outpath, chroot="/host"):
    """kubectl command to send a artifact path to stdout as gzipped tar"""
    command = (
        (
            "kubectl",
            "exec",
        )
        + pod_locator
        + ("--",)
    )
    if chroot:
        command += ("chroot", chroot)
    command += ("tar", "zc", "-C", str(path.parent), path.name)
    logger.debug("running tar command %r", command)
    p = subprocess.Popen(command, env=os.environb, stdout=subprocess.PIPE)
    with tarfile.open(fileobj=p.stdout, mode="r|*") as tf:
        tf.extractall(path=outpath)
    if p.wait() != 0:
        raise RuntimeError(f"tar copy failed, exit code {p.returncode}")


class Client:
    def __init__(self):
        parser = argparse.ArgumentParser("kubectl-anykap")
        parser.add_argument(
            "--verbosity",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            help="verbosity of kubectl-anykap. " 'for verbosity of kubectl, use "-v"',
        )
        parser.add_argument(
            "-n", "--namespace", help="namespace specified for the command, ignored"
        )
        parser.add_argument(
            "-k",
            "--kustomize",
            type=Path,
            default=".",
            help="path to initialize, defaults to current directory",
        )
        nodes = argparse.ArgumentParser(add_help=False)
        nodes.add_argument(
            "--nodes", help="comma separated list of nodes, " "defaults to all nodes"
        )
        subpersers = parser.add_subparsers(title="command", required=True)
        init = subpersers.add_parser("init", help="generates a kustomization directory")
        init.set_defaults(func=self.cmd_init)
        init.add_argument("name", type=name_type, help="name of the capture")
        commands = anykap.make_hq_replserver_parser(subpersers, parents=[nodes])
        commands["info"].set_defaults(func=self.cmd_info)
        commands["tasks"].set_defaults(func=self.cmd_tasks)
        commands["artifacts"].set_defaults(func=self.cmd_artifacts)
        commands["artifacts"].add_argument(
            "--copy",
            action="store_true",
            help="copy all completed and unuploaded artifacts to "
            "<kustomize>/artifacts/. this option marks all successful copies "
            'with "--mark-uploaded"',
        )
        commands["send"].set_defaults(func=self.cmd_send)
        self.parser = parser

    def main(self, *args):
        # we only parse known args, all unknown args are forwarded to kubectl
        # unless someone spot a reason we shouldn't do this
        args, kubectl_args = self.parser.parse_known_args()
        logging.basicConfig(level=getattr(logging, args.verbosity))
        logger.debug("args: %s, kubectl_args: %s", args, kubectl_args)
        if args.namespace:
            logger.warning(
                "namespace %r is specified in commandline, ignored", args.namespace
            )
        self.args = args
        self.kubectl_args = tuple(kubectl_args)
        args.func()

    def find_kustomization_file(self):
        path = self.args.kustomize
        if not path.is_dir():
            self.parser.exit(f"kustomize directory specified {path} is not a directory")
        for fname in ("kustomization.yaml", "kustomization.yml", "kustomization"):
            fpath = path / fname
            if fpath.is_file():
                return fpath

        self.parser.exit("kustomization file no found")

    def get_metadata(self):
        """get anykap metadata from kustomization.yaml"""
        kfpath = self.find_kustomization_file()
        with kfpath.open("r") as f:
            kustomize = yaml.safe_load(f)

        try:
            annotations = kustomize["metadata"]["annotations"]
            name = annotations["anykap/name"]
            if not NAME_PARSER.fullmatch(name):
                self.parser.exit(
                    f"invalid name {name!r} specified in metadata.annotations"
                )
        except KeyError:
            self.parser.exit("unable to find annotation anykap/name in metadata")

        result = dict(
            (k.removeprefix("anykap/"), v)
            for k, v in annotations.items()
            if k.startswith("anykap/")
        )
        version = result.get("version")
        if not version or version != anykap.__version__:
            warnings.warn(
                "workspace created with different anykap version "
                f"{version}, current anykap version {anykap.__version__}"
            )
        return result

    @property
    def metadata(self):
        if not hasattr(self, "_metadata"):
            self._metadata = self.get_metadata()
        return self._metadata

    def discover_nodes(self):
        kubectl_args = self.kubectl_args
        path = self.args.kustomize
        result = kubectl("get", "-k", str(path), "-o", "json", *kubectl_args)
        output = json.loads(result)
        found = [
            d
            for d in output["items"]
            if d["apiVersion"] == "apps/v1"
            and d["kind"] == "DaemonSet"
            and any(
                c["name"] == "anykap"
                for c in d["spec"]["template"]["spec"]["containers"]
            )
        ]
        try:
            (found_ds,) = found
        except ValueError:
            self.parser.exit(
                "expecting exactly 1 daemonset with a container 'anykap,'"
                "found %d" % len(found)
            )

        # we only support matchLabels for now
        namespace = found_ds["metadata"]["namespace"]
        selector = ",".join(
            map("=".join, found_ds["spec"]["selector"]["matchLabels"].items())
        )
        output = json.loads(
            kubectl(  # XXX: what about pods not running?
                "get",
                "pods",
                "-o",
                "json",
                "-n",
                namespace,
                "-l",
                selector,
                *kubectl_args,
            )
        )
        return namespace, output["items"]

    def repl_req(self, pod_locator, args):
        kubectl_args = self.kubectl_args
        metadata = self.metadata
        name = metadata["name"]
        req_kw = {
            "chroot": metadata.get("chroot", ""),
            "sockpath": metadata.get("serverpath", f"/var/run/anykap-{name}.sock"),
        }
        return repl_req(pod_locator + kubectl_args, args, **req_kw)

    def iter_nodes(self):
        args = self.args
        namespace, pods = self.discover_nodes()
        node2pod = dict((pod["spec"]["nodeName"], pod) for pod in pods)

        if args.nodes:
            nodes = set(node.strip() for node in args.nodes.split(","))
            notfound = nodes - set(node2pod.keys())
            if notfound:
                self.parser.exit("nodes specified not found: %r" % notfound)
            nodes = sorted(nodes)
        else:
            nodes = sorted(node2pod.keys())

        for node in nodes:
            yield node, namespace, node2pod[node]["metadata"]["name"]

    def query_nodes(self, repl_query):
        for node, namespace, pod in self.iter_nodes():
            pod_locator = ("-n", namespace, pod)
            try:
                result = self.repl_req(pod_locator, repl_query)
            except Exception:
                logger.exception("failed when querying node %s", node)
                continue
            yield node, result, pod_locator

    def cmd_init(self):
        parser = self.parser
        args = self.args
        initpath = Path(args.kustomize)
        if initpath.exists():
            if initpath.is_dir():
                if any(initpath.iterdir()):
                    parser.exit(f"the path specified {initpath} is not empty")
            else:
                parser.exit(f"the path specified {initpath} exists and not a directory")
        else:
            try:
                initpath.mkdir(mode=0o755)
            except OSError as e:
                parser.exit(f"failed making directory {initpath} due to {e}")
        config = vars(args)
        generate_workspace(initpath, config)

    def cmd_info(self):
        results = {}
        for node, result, pod_locator in self.query_nodes(("-ojson", "info")):
            (result0,) = result
            results[node] = json.loads(result0)
        pprint.pprint(results)

    def cmd_tasks(self):
        args = self.args
        parser = self.parser
        results = {}
        query = ("-ojson", "tasks")
        if args.regex:
            query += ("-r",)
        if args.stop:
            if not args.name:
                parser.exit("task name must be specified for stop")
            query += ("-s",)
        if args.all:
            query += ("-a",)
        if args.name:
            query += tuple(args.name)
        for node, result, pod_locator in self.query_nodes(query):
            (result0,) = result
            results[node] = json.loads(result0)
        pprint.pprint(results)

    def cmd_artifacts(self):
        args = self.args
        kubectl_args = self.kubectl_args
        parser = self.parser
        metadata = self.metadata
        query = (
            "-ojson",
            "artifacts",
        )
        if args.regex:
            query += ("-r",)
        if args.all and not args.mark_uploaded and not args.copy:
            query += ("-a",)
        if args.mark_uploaded and not args.copy:
            query += ("--mark-uploaded",)
            if not args.name:
                parser.exit("artifact name must be specified for mark-uploaded")
        if args.copy:
            artifacts_path = args.kustomize / "artifacts"
            artifacts_path.mkdir(exist_ok=True)

        results = {}
        for node, result, pod_locator in self.query_nodes(query):
            (result0,) = result
            artifacts = json.loads(result0)["items"]
            results[node] = artifacts
            if args.copy:
                outcome = []
                results[node] = outcome
                for a in artifacts:
                    if a["upload_state"] == "completed":
                        logger.info("skipping already uploaded artifact %s", a["name"])
                        continue
                    try:
                        logger.info("copying artifact %s", a["name"])
                        tar_copy_artifact(
                            pod_locator + kubectl_args,
                            PosixPath(a["path"]),
                            artifacts_path,
                            chroot=metadata.get("chroot"),
                        )
                        (response,) = self.repl_req(
                            pod_locator,
                            ("-ojson", "artifacts", "--mark-uploaded", a["name"]),
                        )
                        outcome.extend(json.loads(response)["items"])
                    except Exception:
                        logger.exception(
                            "failed when copying node %s artifact %s", node, a["name"]
                        )
                        continue
        pprint.pprint(results)

    def cmd_send(self):
        args = self.args
        query = ("send", json.dumps(args.event, separators=(",", ":")))
        for node, result, pod_locator in self.query_nodes(query):
            assert not result
            logger.info("sent to node %s", node)


main = Client().main


if __name__ == "__main__":
    main()
