import sys
import os
from importlib.machinery import ModuleSpec
from importlib.abc import MetaPathFinder, InspectLoader


class EnvImporter(MetaPathFinder, InspectLoader):
    """Imports source inside env variables as Python modules"""

    def __init__(self, replace_dot="__", valid_prefix="", env_prefix="PYMODULE__"):
        self._replace_dot = replace_dot
        self._valid_prefix = valid_prefix
        self._env_prefix = env_prefix

    def envname(self, fullname):
        if self._valid_prefix and not fullname.startswith(self._valid_prefix):
            return None
        return self._env_prefix + fullname.upper().replace(".", self._replace_dot)

    def get_source(self, fullname):
        envname = self.envname(fullname)
        if not envname or envname not in os.environ:
            raise ImportError
        return os.environ[envname]

    def find_spec(self, fullname, path, target=None):
        envname = self.envname(fullname)
        if not envname or envname not in os.environ:
            return None
        is_package = any(k.startswith(envname + self._replace_dot) for k in os.environ)
        return ModuleSpec(fullname, loader=self, is_package=is_package)


def install_envimporter(**kwargs):
    sys.meta_path.append(EnvImporter(**kwargs))
