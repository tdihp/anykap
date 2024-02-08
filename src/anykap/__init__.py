from collections.abc import Callable
from typing import Optional, Any, Type, Union, Literal
import threading
import queue
import subprocess
import time
import datetime
from pathlib import Path
from shutil import rmtree
import json
import logging
import itertools as it
import contextlib
import shutil
import os
import re
import shlex
import asyncio
import selectors
import tempfile
import io
import math

logger = logging.getLogger('anykap')


NAME_PARSER = re.compile('^[a-z][a-z0-9_-]{0,254}$', flags=re.ASCII)


def valicate_name(name:str):
    """
    a general name parser for all named components
    basically:
    always lowercase, max 255 bytes
    start with alphabetic, ends with alphanumeric, mixes "-", "_" in the middle.
    consideration for 255 byte: we likely need full container id hash (64bytes)
    """
    return bool(NAME_PARSER.match(name))


def sanitize_name(name:str):
    """
    Try to sanitize the given name so it can be a valid one.
    """
    return re.sub(r'^[a-z0-9_-]', '',
                  re.sub(r'\s+', '_', name.strip().lower())).strip('_-')[:255]


class HQ(object):
    """
    rules: a list of functions to react on events. Rules can be added,
           not removed
    tasks: a list of Task objects
    """
    CADENCE = 0.1
    def __init__(self, datapath=None):
        """by default we use cwd as datapath"""
        self.tasks = []
        self.rules = []
        datapath = datapath or os.environ.get('ANYKAP_PATH', os.getcwd())
        self.datapath = Path(datapath).absolute()
        self.datapath.mkdir(parents=True, exist_ok=True)
        self.running = False
        self._lock = threading.RLock()
        self.queue = queue.SimpleQueue()
        self.artifact_counter = it.count()
        self.artifacts = set()
        self.logger = logger.getChild('HQ')

    def run(self):
        with self._lock:
            assert not self.running
            self.running = True
            for task in self.tasks:
                self.logger.debug('starting task %s', task)
                task.start()

        while self.running:
            self.logger.debug('running loop')
            self.event_loop()

    def event_loop(self):
        """execute event processing logic for one loop"""
        # we block only the the first one
        get_args = {'block': True, 'timeout': self.CADENCE}  
        logger = self.logger
        while True:
            try:
                item = self.queue.get(**get_args)
                logger.debug('got item %s', item)
            except queue.Empty:
                break

            for task in self.tasks:
                for f in task.receptors.values():
                    try:
                        f(item)
                    except Exception:
                        logger.exception(
                            'failed when calling receptor for task %s on item %r',
                            task, item)

            for f in self.rules:
                try:
                    f(item)
                except Exception:
                    logger.exception(
                        'failed when calling rule %r on item %r', f, item
                    )

            get_args = {'block': False}

    def add_task(self, task):
        """ add a new task to hq
        """
        # we are assuming tasks are not yet running.
        with self._lock:
            task.hq = self
            # assert task.hq == self
            # task.receptors['exit'].add_filter()
            self.tasks.append(task)
            if self.running:
                task.start()

    def add_rule(self, rule):
        self.rules.append(rule)

    def send_event(self, event:dict):
        """tasks should call this to transmit new events"""
        self.queue.put(event)

    def new_artifact(self, name, keep=True, context=None):
        artifact_name = self.gen_artifact_name(name)
        path = self.datapath / name
        if path.exists():
            raise RuntimeError('artifact path %s already exists' % path)

        path.mkdir(parents=True)
        artifact = Artifact(artifact_name, path, keep)
        with self._lock:
            self.artifacts.add(artifact)
        artifact.hq = self
        return artifact

    def gen_artifact_name(self, name):
        now = datetime.datetime.utcnow().strftime('%Y%m%d%H%M')
        cnt = next(self.artifact_counter)
        return f'{now}-{name}-{cnt}'

    def forget_artifact(self, artifact):
        with self._lock:
            self.artifacts.discard(artifact)

class Receptor(object):
    """
    Receptor is a part of the event driven framework that defines subscription
    to the event stream.
    
    Filters can be added to decide whether the testing event is subscribed.
    """
    def __init__(self):
        self.conditions = []
        self.logger = logger.getChild(self.__class__.__name__)

    def add_condition(self,
                      filter:Callable[["dict"], bool],
                      mutator:Optional[Callable[["dict"], Any]]=None,
                      ):
        self.conditions.append((filter, mutator))

    def __call__(self, event:dict):
        for filter, mutator in self.conditions:
            try:
                if not filter(event):
                    return
            except Exception:
                self.logger.exception(
                    'failed when calling filter %r on event %r',
                    filter, event)
                return

            mutation_result = event
            if mutator:
                try:
                    mutation_result = mutator(event)
                except Exception:
                    self.logger.exception(
                        'failed when calling mutator %r on event %r',
                        mutator, event)
                    return

            self.send(mutation_result)
            break  # first match wins

    def send(self, event:Any):
        raise NotImplementedError

    def get(self, timeout=None):
        """get everything of the receptor"""
        raise NotImplementedError


class QueueReceptor(Receptor):
    def __init__(self):
        super().__init__()
        self.queue = queue.SimpleQueue()

    def send(self, event):
        self.queue.put(event)

    def get(self, timeout=None):
        """get one item from queue with optional timeout. Return None if
        nothing available.
        """
        if timeout is None:
            kwargs = {'block': False}
        else:
            kwargs = {'block': True, 'timeout': timeout}
        
        try:
            return self.queue.get(**kwargs)
        except queue.Empty:
            return None


class TEventReceptor(Receptor):
    def __init__(self):
        super().__init__()
        self.event = threading.Event()

    def send(self, event):
        if bool(event):
            self.event.set()
        else:
            self.event.clear()

    def get(self, timeout=None):
        if timeout is None:
            return self.event.is_set()

        return self.event.wait(timeout=timeout)


class Task(threading.Thread):
    # each task can have fixed number of receptors. They should be defined like
    # this:
    # Noting a exit receptor always exists and HQ expects that
    receptors:dict[str, Receptor]

    # The exit receptor should be checked by run function to check whether to
    # terminate itself immediately
    receptor_exit = TEventReceptor

    def __init__(self, name:str):
        super().__init__(name=name)
        if not valicate_name(name):
            raise ValueError(
                f"given name {name} is not valid, please sanitize first")
        self.name = name
        self.receptors = dict((k.split('_', 1)[1], getattr(self, k)())
                              for k in dir(self)
                              if k.startswith('receptor_'))
        # XXX: we are assuming name is unique
        self.logger = logger.getChild(self.__class__.__name__).getChild(self.name)
        self.local = threading.local()

    def exit(self):
        """intended method to directly terminate this task"""
        self.receptors['exit'].send(1)

    def need_exit(self):
        """should be tested in run"""
        return self.receptors['exit'].get()

    def send_event(self, event):
        self.hq.send_event(event)

    def run(self):
        self.local.logger = self.logger.getChild(str(threading.get_native_id()))
        try:
            self.run_task()
        except BaseException:
            self.logger.exception('task encountered error')
        finally:
            pass

    def run_task(self):
        """tasks should implement this"""
        raise NotImplementedError

    def new_artifact(self, name=None, keep=True):
        return self.hq.new_artifact(name or self.name, keep)


class Compressor(object):
    """"""
    pass


class CRICtlData(object):
    OBJTYPE:str
    LIST_KEY:str
    INSPECT_KEY:str
    DATA_LIST_KEY:str
    RUN_TIMEOUT:int = 3
    DATA_KEYS:set[str] = set()
    INSPECT_KEYS:set[str] = set()

    @classmethod
    def list(cls, **kwargs):
        cp = subprocess.run(['crictl', cls.LIST_KEY, '-o', 'json']
                            + cls.query_args(**kwargs),
                            stdout=subprocess.PIPE, text=True, check=True,
                            timeout=cls.RUN_TIMEOUT)
        data = json.loads(cp.stdout)
        return [cls(item) for item in data[cls.DATA_LIST_KEY]]

    @classmethod
    def query_args(cls, **kwargs):
        return []

    def __init__(self, data, inspect=None):
        self._data = data
        self._inspect = inspect

    @property
    def inspect(self):
        # lazily inspect things
        if self._inspect is None:
            self._inspect = self.run_inspect()

        return self._inspect

    def __hash__(self):
        return self.id

    def run_inspect(self):
        cp = subprocess.run(['crictl', self.INSPECT_KEY, self.id, '-o', 'json'],
                            stdout=subprocess.PIPE, text=True, check=True,
                            timeout=self.RUN_TIMEOUT)
        return json.loads(cp.stdout)

    def __getattr__(self, name):
        if name in self.DATA_KEYS:
            return self._data[name]

        if name in self.INSPECT_KEYS:
            return self.inspect[name]

        raise AttributeError("attribute %s is not known" % name)


class CRIPodSandbox(CRICtlData):
    OBJTYPE = 'pod'
    LIST_KEY = 'pods'
    INSPECT_KEY = 'inspectp'
    DATA_LIST_KEY = 'items'
    DATA_KEYS = {
        'id',
        'metadata',
        'state',
        'createdAt',
        'labels',
        'annotations',
        'runtimeHandler',
    }
    INSPECT_KEYS = {
        'status',
        'info',
    }

    @classmethod
    def query_args(cls, namespace=None, name=None, labels=None):
        args = []
        if namespace:
            args += ['--namespace', namespace]

        if name:
            args += ['--name', name]
        
        if labels:
            for label in labels:
                args += ['--label', label]

        return args


class CRIContainer(CRICtlData):
    OBJTYPE = 'container'
    LIST_KEY = 'ps'
    INSPECT_KEY = 'inspect'
    DATA_LIST_KEY = 'containers'
    POD_NAME_LABEL = "io.kubernetes.pod.name"
    POD_NAMESPACE_LABEL = "io.kubernetes.pod.namespace"
    POD_UID_LABEL = "io.kubernetes.pod.uid"
    DATA_KEYS = {
        'id',
        'podSandboxId',
        'metadata',
        'image',
        'imageRef',
        'state',
        'createdAt',
        'labels',
        'annotations',
    }
    INSPECT_KEYS = {
        'status',
        'info',
    }


    @classmethod
    def query_args(cls, name=None, image=None, labels=None,
                 pod_name=None, pod_namespace=None, pod_uid=None):
        args = []

        if name:
            args += ['--name', name]

        if image:
            args += ['--image', image]

        labels = list(labels) if labels else []

        if pod_name:
            labels.append(cls.POD_NAME_LABEL + '=' + pod_name)

        if pod_namespace:
            labels.append(cls.POD_NAMESPACE_LABEL + '=' + pod_namespace)

        if pod_uid:
            labels.append(cls.POD_UID_LABEL + '=' + pod_uid)

        for label in labels:
            args += ['--label', label]

        return args


class CRIImage(CRICtlData):
    OBJTYPE = 'image'
    LIST_KEY = 'img'
    INSPECT_KEY = 'inspecti'
    DATA_LIST_KEY = 'images'
    DATA_KEYS = {
        'id',
        'repoTags',
        'repoDigests',
        'size',
        'uid',
        'username',
        'spec',
        'pinned',
    }
    INSPECT_KEYS = {
        'status',
        'info',
    }


class Artifact(object):
    """
    Claims a path for saving data. Task shouldn't write any data to disk without
    requesting an Artifact.

    HQ tracks artifact's progress so data can be presented and/or uploaded.
    Artifact is strictly speaking not bound to Tasks, while in reality it should
    be.

    regular use case:

        with hq.new_artifact(self, 'name') as artifact:
            (artifact.path / 'out.file').write('hello')
    
    """
    
    def __init__(self, name:str, path:Path, keep=True, context:Any=None):
        """keep is a hint on whether we intend to keep it.
        Some commands might just need a working dir for artifact.
        """
        self.name = name
        self.path = path
        self.hq = None
        self._state = 'created'
        self._start_time = None
        self._completion_time = None
        self.loggers = []
        self.handler = None
        self.lock = threading.RLock()
        self.keep = True
        self.context = context
        self.upload_state = None  # None / 'uploading' / complete
        self.upload_attempts = 0
        self.last_upload_failure = None

    def start(self):
        with self.lock:
            assert self._state == 'created'
            self._state = 'started'
            self._start_time = datetime.datetime.utcnow()

            for logger in self.loggers:
                logger.addHandler(self.handler)

    def complete(self):
        with self.lock:
            assert self._state == 'started', "got state %s" % self._state
            self._state = 'completed'
            self._completion_time = datetime.datetime.utcnow()
            for logger in self.loggers:
                logger.removeHandler(self.handler)

        self.hq.send_event({'kind': 'artifact',
                            'topic': 'complete',
                            'artifact': self})
        # we don't handle keep here, but other components might decide on how
        # to deal with it

    def destroy(self):
        with self.lock:
            assert self._state == 'completed'
            shutil.rmtree(self.path)

    @property
    def state(self):
        with self.lock:
            return self._state

    @property
    def start_time(self):
        with self.lock:
            return self._start_time

    @property
    def completion_time(self):
        with self.lock:
            return self._completion_time

    def handle_logger(self, logger:logging.Logger):
        with self.lock:
            if not self.handler:
                self.handler = logging.FileHandler(self.path / 'anykap.log')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.complete()


class ShellTask(Task):
    """run shell scripts
    
    stdout/stderr will be routed to "sh.stdout" "sh.stderr", result will be
    routed to "sh.result". No exception raised for error exit codes.

    On script exit, the exit code will be sent through events.

    ShellTask can be configured to send the stdout/stderr received into events.
    User's discretion is needed, preferably leverage tee and grep so only
    meaningful lines are popped up.

    Since we should be dealing with text here, all subprocess.Popen parameters
    are set to prefer text parsing, and regex only supports text.
    """
    STDOUT_FILE = 'sh.stdout'
    STDERR_FILE = 'sh.stderr'
    RESULT_FILE = 'sh.result'
    SIGTERM_TIMEOUT = 10
    SIGKILL_TIMEOUT = 3
    CADENCE = 0.2

    def __init__(self,
                 name,
                 command:str,
                 nsenter_args:Optional[list[str]]=None,
                 shell:str='bash',
                 shell_args:Optional[list[str]]=None,
                 timeout:Optional[float]=None,
                 keep_artifact:bool=True,
                 encoding='utf-8',
                 errors='replace',
                 stdout_mode:Literal['artifact','notify','null']='artifact',
                 stderr_mode:Literal['artifact','notify','null','stdout']='artifact',
                 stdout_filter:Optional[Union[str,re.Pattern[str]]]=None,
                 stderr_filter:Optional[Union[str,re.Pattern[str]]]=None,
                 **kwargs,
                 ):
        super().__init__(name)
        self.command = command
        self.shell = shell
        self.shell_args = shell_args or []
        self.nsenter_args = nsenter_args
        self.timeout = timeout
        self.keep_artifact = keep_artifact
        self.encoding = encoding
        self.errors = errors
        self.stdout_mode = stdout_mode
        self.stderr_mode = stderr_mode

        if stdout_filter:
            if isinstance(stdout_filter, str):
                logger.debug('compiling stdout filter %r', stdout_filter)
                stdout_filter = re.compile(stdout_filter)
            elif not isinstance(stdout_filter, re.Pattern):
                raise ValueError('provided stdout_filter %r is not valid'
                                 % stdout_filter)
        self.stdout_filter = stdout_filter
        if stderr_filter:
            if isinstance(stderr_filter, str):
                logger.debug('compiling stderr filter %r', stderr_filter)
                stderr_filter = re.compile(stderr_filter)
            elif not isinstance(stdout_filter, re.Pattern):
                raise ValueError('provided stderr_filter %r is not valid'
                                 % stderr_filter)
        self.stderr_filter = stderr_filter

    def run_task(self):
        logger = self.local.logger
        args = [self.shell]
        if self.shell_args:
            args.extend(self.shell_args)

        args += ['-c', self.command]

        if self.nsenter_args:
            args[0:0] = ['nsenter'] + self.nsenter_args

        start_time = time.monotonic()

        # by default, we simply write to artifact directory
        with contextlib.ExitStack() as stack:
            artifact = stack.enter_context(
                self.new_artifact(keep=self.keep_artifact))

            def prepare_file(mode, filename):
                if mode == 'artifact':
                    return stack.enter_context(
                        # seems to work even when subprocess is test mode
                        open(artifact.path / filename, 'wb'))  
                elif mode == 'null':
                    return subprocess.DEVNULL
                elif mode == 'stdout':
                    return subprocess.STDOUT
                elif mode == 'notify':
                    return subprocess.PIPE
                raise ValueError('unrecognized')

            stdout = prepare_file(self.stdout_mode, self.STDOUT_FILE)
            stderr = prepare_file(self.stderr_mode, self.STDERR_FILE)
            p = subprocess.Popen(
                args, cwd=str(artifact.path),
                encoding=self.encoding, errors=self.errors,
                stdout=stdout, stderr=stderr,
            )
            selector = selectors.DefaultSelector()
            if self.stdout_mode == 'notify':
                selector.register(p.stdout.fileno(),
                                  selectors.EVENT_READ,
                                  (p.stdout, self.stdout_filter, 'stdout'))
            if self.stderr_mode == 'notify':
                selector.register(p.stderr.fileno(),
                                  selectors.EVENT_READ,
                                  (p.stderr, self.stderr_filter, 'stderr'))

            while True:
                for key, mask in selector.select(self.CADENCE):
                    f, pattern, name = key.data
                    for line in f.readlines():
                        logger.debug('processing line %r', line)
                        extra = {}
                        if pattern:
                            logger.debug('pattern: %r, line: %r', pattern, line)
                            m = pattern.search(line)
                            if not m:
                                continue
                            extra = {
                                'groups': m.groups(),
                                'groupdict': m.groupdict(),
                            }
                        event = {
                            'kind': 'shell',
                            'topic': 'line',
                            'output': name,
                            'line': line,
                            'task_name': self.name,
                        }
                        event.update(extra)
                        self.send_event(event)

                result = p.poll()

                is_timeout = self.timeout and (
                    time.monotonic() - start_time > self.timeout)

                if result is not None:  # completed
                    logger.info('script exited with code %d', result)
                    (artifact.path / self.RESULT_FILE).write_text(str(result))
                elif self.need_exit() or is_timeout:
                    if is_timeout:
                        logger.info('timeout')
                    if self.need_exit():
                        logger.info('received exit')
                    logger.info('terminating')
                    try:
                        result = p.wait(self.SIGTERM_TIMEOUT)
                    except subprocess.TimeoutExpired:
                        logger.warning('terinating timed out, killing')
                        p.kill()
                        try:
                            result = p.wait(self.SIGKILL_TIMEOUT)
                        except subprocess.TimeoutExpired:
                            logger.error('kill timed out. Giving up')
                            raise

                if result is not None:
                    logger.info('exit code: %d', result)
                    self.send_event(
                        {
                            'kind': 'shell',
                            'topic': 'complete',
                            'status': result,
                            'task_name': self.name,
                        })
                    return


class CRIDiscovery(Task):
    CADENCE = 5
    def __init__(self, datacls:Type[CRICtlData], name=None, send_list=False,
                 **query):
        super().__init__(name=None or datacls.OBJTYPE + '_discovery')
        self.datacls = datacls
        self.watching = set()
        self.send_list = send_list  # send list on cadence if needed
        self.query = query

    def run_task(self):
        logger = self.local.logger
        self.datacls.list()
        while not self.need_exit():
            result = set(self.datacls.list(**self.query))
            added = result - self.watching
            removed = self.watching - result
            for obj in added:
                self.send_event({
                    'kind': 'discovery',
                    'topic': 'new',
                    'objtype': self.datacls.OBJTYPE,
                    'task': self.name,
                    'object': obj,
                })

            for obj in removed:
                self.send_event({
                    'kind': 'discovery',
                    'topic': 'lost',
                    'objtype': self.datacls.OBJTYPE,
                    'task': self.name,
                    'object': obj,
                })
            
            self.watching -= removed
            self.watching += added
            if self.send_list:
                self.send_event({
                    'kind': 'discovery',
                    'topic': 'list',
                    'objtype': self.datacls.OBJTYPE,
                    'task': self.name,
                    'object': list(self.watching),
                })
            time.sleep(self.CADENCE)

        logger.info('exiting')


class PODDiscovery(CRIDiscovery):
    def __init__(self, name=None, **kwargs):
        super().__init__(CRIPodSandbox, name=name, **kwargs)


class ContainerDiscovery(CRIDiscovery):
    def __init__(self, name=None, **kwargs):
        super().__init__(CRIContainer, name=name, **kwargs)


class ImageDiscovery(CRIDiscovery):
    def __init__(self, name=None, **kwargs):
        super().__init__(CRIImage, name=name, **kwargs)


class Fission(object):
    """
    templating method for creating & adding new tasks
    """
    def __init__(self, hq, task_factory, name=None,
                 filter=None, mutator=None,
                 **kwargs):
        self.hq = hq

        if not filter and not hasattr(self, 'filter'):
            raise ValueError('filter must be provided')

        if filter:
            self.filter = filter

        if not mutator and not hasattr(self, 'mutator'):
            raise ValueError('mutator must be provided')
    
        if mutator:
            self.mutator = mutator

        self.name = name
        self.task_factory = task_factory
        self.kwargs = kwargs
        self.counter = it.count(1)
        self.logger = logger.getChild(self.__class__.__name__)

    def next_task_name(self):
        return '-'.join(self.name + next(self.counter))

    def __call__(self, event):
        try:
            if not self.filter(event):
                return
        except Exception:
            self.logger.exception(
                'calling filter %r fails on event %r, ignoring',
                self.filter, event)
            return

        try:
            mutating_result = self.mutator(event)
        except Exception:
            self.logger.exception('calling mutator %r fail on event %r, ignoring',
                self.mutator, event)
            return

        params = self.kwargs.copy()
        params.update(mutating_result)
        if 'name' not in mutating_result:
            # we make the task name if not supplied
            params['name'] = self.next_task_name()

        try:
            new_task = self.task_factory(**params)
        except Exception:
            self.logger.exception('calling factory %r fails on event %r',
                                  self.task_factory, event)
            return

        if not isinstance(new_task, Task):
            self.logger.error('factory %r generated non-task %r, ignored',
                              self.task_factory, new_task)
            return

        self.hq.add_task(new_task)


class REPLServerProtocol(asyncio.Protocol):
    r"""A way user can interact with HQ for some simple actions
    It is also a "task" because why not?

    Protocol:
        request:
            always single line ends with os.linesep with pattern:
                <cmd> [<param1> <param2> ...]
            Both cmd and param goes through shlex, so they can be quoted.
            exit/quit is a protocol level command that causes the server to
            directly hang up without sending a response.
        response:
            starts with a status line of OK/ERR, several content lines, ends
            with a empty line:
                <STATUS>
                <non-empty-data>
                <non-empty-data>
                ...
                <empty new line>
            For ERR, the following lines will show error specific data,
            Server will disconnect after sending the full ERR message.
            for OK, meaningful content.
        Noting that newline is the only control charater that should always be
        honored. (Yes this is a limited protocol, not for binary/arbitary
        transfer)

    User can use `nc -CU <file>` to access the unix socket for plain text
    interaction.
    """
    def __init__(self, server,
                 newline=b'\r\n', idle_timeout=300, max_request=1024):
        self.newline = newline
        self.idle_timeout = idle_timeout
        self.max_request = max_request
        self.server = server
        self.logger = logger.getChild(self.__class__.__name__)
        self.logger.debug('protocol created')


    def on_timeout(self, progress):
        logger.debug('progress timing out: %d, current progress: %d',
                     progress, self.progress)
        if self.progress > progress: 
            return  # client made progress, not timeout
        else:
            return self.fail('session timed out')

    def on_progress(self):
        self.progress += 1
        loop = asyncio.get_running_loop()
        logger.debug('loop.call_later(%s,%s,%s)', self.idle_timeout, self.on_timeout, self.progress)
        loop.call_later(self.idle_timeout, self.on_timeout, self.progress)

    def listening(self):
        self.buffer = bytearray()
        self.state = 'LISTEN'
        self.on_progress()

    def connection_made(self, transport):
        self.transport = transport
        self.logger.info('new connection')
        self.progress = 0
        self.listening()

    def data_received(self, data):
        self.logger.debug('received data %r', data)
        self.on_progress()
        if self.state != 'LISTEN':
            return self.fail('received data when in state %s',  self.state)

        buffer = self.buffer
        buffer.extend(data)
        if len(buffer) > self.max_request:
            return self.fail('input too large (%d)', len(buffer))

        if not buffer.endswith(self.newline):
            return

        self.state = "PROCESS"
        if buffer.count(self.newline) > 1:
            return self.fail('request has multiple newlines')

        try:
            query = buffer.decode('ascii')
        except Exception as e:  # yup we only support ascii
            return self.fail('request cannot be decoded with ascii, %s', str(e))

        try:
            cmdline = shlex.split(query)
            if not cmdline:
                return self.fail('no command found')

            if cmdline[0] in {'exit', 'quit'}:
                self.transport.close()
                return

            response = self.server.call_cmd(*cmdline)
        except Exception as e:
            logger.exception('got exception when calling cmd')
            self.transport.write(b'ERR' + self.newline)
            self.transport.write(self.sanitize(repr(e)) + self.newline * 2)
            self.transport.close()
            return

        self.transport.write(b'OK' + self.newline)
        for row in response:
            self.transport.write(self.sanitize(row) + self.newline)

        self.transport.write(self.newline)
        self.listening()

    def sanitize(self, data):
        """we make sure there's no empty new lines in data"""
        return re.sub(
            br'(%s)+' % re.escape(self.newline), self.newline,
            data.encode('ascii', errors='replace').strip(self.newline))

    def fail(self, reason, *args):
        self.logger.info('connection failed: ' + reason, *args)
        self.transport.close()


class REPLServer(Task):
    CADENCE = 1
    def __init__(self, path, name='replserver', **kwargs):
        super().__init__(name=name)
        self.path = path
        self.kwargs = kwargs  # for protocol factory

    def run_task(self):
        logger = self.local.logger
        # asyncio is used so we don't need to worry about concurrency on this
        loop = asyncio.new_event_loop()
        self.server = None
        async def run():
            # logging.basicConfig(level=logging.DEBUG)
            server = await loop.create_unix_server(
                lambda: REPLServerProtocol(self, **self.kwargs), self.path)
            self.server = server
            logger.info('server started')
            async with server:
                logger.info('starting server')
                try:
                    await server.serve_forever()
                except asyncio.CancelledError:
                    return

        async def check_exit():
            while not self.need_exit():
                # print('waiting')
                await asyncio.sleep(self.CADENCE)
            if self.server:
                self.server.close()
                await self.server.wait_closed()

        server_task = loop.create_task(run())
        loop.create_task(check_exit())
        loop.run_until_complete(server_task)  # check_exit terminates the loop
        logger.info('loop stopped')

    def call_cmd(self, cmd, *args):
        cmdfunc = self.commands[cmd]  # fine to raise KeyError
        return cmdfunc(*args)


class HQREPLServer(REPLServer):
    HELP = '''
    Commands available:
        h/help [<command>] - prints help
        tasks - list all tasks
        artifacts - list all artifacts
        stop-tasks <pattern> - stop task(s) of the given pattern.
                              prints tasks issued stop
        send-event <event-json>
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commands = {
            'h': self.cmd_help,
            'help': self.cmd_help,
            'tasks': self.cmd_tasks,
            'stop-tasks': self.cmd_stop_tasks,
        }

    def cmd_help(self, cmd=None):
        ''' Prints help
        '''
        return [self.HELP.strip()]

    def cmd_tasks(self):
        """
        """
        result = ['\t'.join(['name', 'tid', 'running', 'stopping'])]
        for task in list(self.hq.tasks):
            running = task.is_alive()
            tid = None
            if running:
                tid = task.get_native_id()
                stopping = task.need_exit()
            result.append('\t'.join([task.name, str(tid), str(running), str(stopping)]))

        return result

    def cmd_stop_tasks(self, pattern):
        """
        input:
            task_query: regular expression for task name
        output:
            task_name (for every task with stop sent)
            ...
        """
        compiled = re.compile(pattern)
        result = []
        for task in self.hq.tasks:
            if not compiled.search(task.name):
                continue
            
            if task.is_alive and not task.need_exit():
                task.exit()
                result.append(task.name)
        return result


def archive_tar(datapath:str, outpath:str,
                compressor:Literal[None,'gz','bz2','xz']='gz'):
    import tarfile
    suffix = '.tar'
    tarmode = 'w'
    if compressor:
        suffix += '.' + compressor
        tarmode += ':' + compressor

    fd, fpath = tempfile.mkstemp(dir=outpath, prefix='archive-', suffix=suffix)
    stack = contextlib.ExitStack()
    try:
        with stack:
            f = stack.enter_context(open(fd, 'wb'))
            tf = stack.enter_context(
                tarfile.open(fileobj=f, mode=tarmode)
            )
            tf.add(datapath)
    except Exception:
        os.remove(fpath)
        raise


def archive_zip(datapath:str, outpath:str, ):
    import zipfile
    fd, fpath = tempfile.mkstemp(dir=outpath, prefix='archive-', suffix='.zip')
    stack = contextlib.ExitStack()
    try:
        with stack:
            f = stack.enter_context(open(fd, 'wb'))
            zf = stack.enter_context(
                zipfile.ZipFile(f, mode='w', compression=zipfile.ZIP_DEFLATED))
            
            for dirpath, dirnames, filenames in os.walk(datapath):
                print(f'walking {dirpath}')
                for fn in filenames:
                    print(fn)
                    zf.write(os.path.join(dirpath, fn))
    except Exception:
        os.remove(fpath)
        raise


class ArtifactRule(object):
    """The defult rule for processing artifacts"""
    def __init__(self, hq, upload_attempts=3, max_backoff=60):
        self.logger = logger.getChild(self.__class__.__name__)
        self.hq = hq
        self.upload_attempts = upload_attempts
        # delay = e^(k * n) - 1 --> k = log(delay - 1) / n
        k = math.log(max_backoff - 1) / (upload_attempts - 1)
        self.retry_ladder = [math.exp(k * i) for i in range(1, upload_attempts)]
        self.timers = []

    def __call__(self, event):
        hq = self.hq
        uploader = hq.uploader

        if event.get('kind') != 'artifact':
            return

        logger = self.logger
        artifact = event['artifact']

        if not artifact.keep:
            if not event['topic'] == 'complete':
                logger.warning('not sure what this event is: %r', event)
                return
            # we clean up and forget the thing if not meant to be kept
            try:
                shutil.rmtree(str(artifact.path))
            except Exception:
                self.logger.exception(
                    'failed removing files for artifact %r', artifact)
            hq.forget_artifact(artifact)

        if not uploader:
            logger.debug('uploader not configured, not uploading %r', artifact)
            return

        if event['topic'] not in ['complete', 'upload_failure']:
            logger.warning('not sure what this event is: %r', event)
            return

        if event['topic'] == 'complete':
            uploader.upload(artifact)
        elif event['topic'] == 'upload_failure':
            if artifact.upload_attempts >= self.upload_attempts:
                logger.warning('upload retries exhausted for artifact %r',
                               artifact)
                return
            assert 0 <= artifact.upload_attempts < self.upload_attempts
            self.timers.append(
                threading.Timer(self.retry_ladder[artifact.upload_attempts], uploader.upload, [artifact]))
        else:
            logger.warning('not sure what this event is: %r', event)
            return

        if artifact.upload_attempts > self.upload_attempts:
            logger.warning('upload retries exhausted for artifact %r', artifact)
            return
        


        hq.uploader()

class Uploader(object):
    """uploader is a retryable interface for uploading a single artifact
    """
    def __init__(self):
        self.logger = logger.getChild(self.__class__.__name__)

    def upload(self, artifact:Artifact):
        # no error means upload started. Noting it doesn't guarantee upload
        # success
        with artifact.lock:
            if artifact.state != 'complete':
                raise RuntimeError(
                    'uploading artifact %r not in complete state' % artifact)
            assert artifact.upload_state == None
            artifact.upload_state = 'uploading'
            artifact.upload_attempts += 1

        try:
            self.try_upload(artifact)
        except Exception:
            self.logger.exception('when attempting upload %r', artifact)
            self.fail(artifact)

    def try_upload(self, artifact):
        """concrete implementation of the uploading logic"""
        raise NotImplementedError

    def upload_complete(self, artifact):
        with artifact.lock:
            assert artifact.state == 'complete'
            assert artifact.upload_state == 'uploading'
            artifact.upload_state = 'complete'
        self.hq.send_event({
            'kind': 'artifact',
            'topic': 'upload_complete',
            'artifact': artifact})

    def fail(self, artifact):
        with artifact.lock:
            assert artifact.upload_state == 'uploading'
            artifact.upload_state = None

        self.hq.send_event({
            'kind': 'artifact',
            'topic': 'upload_failure',
            'artifact': artifact,
        })


class QueuedUploader(Task):
    CADENCE = 0.2
    def __init__(self, name, uploader:Uploader):
        super().__init__()
        self.queue = queue.SimpleQueue()
        self.uploader = uploader

    def upload_attempt(self, artifact):
        self.queue.put(artifact)

    def run_task(self):
        while not self.need_exit():
            try:
                artifact = self.queue.get(timeout=self.CADENCE)
            except queue.Empty:
                continue

            try:
                self.uploader.try_upload()  # make sure uploader is sync
            except Exception:
                self.logger.exception('when processing queued %r', artifact)
                continue            

            self.upload_complete(self, artifact)


if __name__ == '__main__':
    archive_zip('src', '.bash')
