from collections.abc import Callable
from typing import Optional, Any, Type, Union, Literal
import os
from pathlib import Path
import itertools as it
import asyncio
import asyncio.queues as queue
import asyncio.subprocess as async_subproc
import datetime
import re
import contextlib
import platform
import shutil
import time
import shlex
import argparse
import io
import json
import urllib.request
import urllib.parse
import ipaddress
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import logging

# XXX: make __all__
__version__ = '0.1.0'
USER_AGENT = f'anykap/{__version__}'

logger = logging.getLogger('anykap')
thread_pool = ThreadPoolExecutor(max_workers=2)

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

NAME_PARSER = re.compile('^[a-z][a-z0-9_-]{0,254}$', flags=re.ASCII)

def validate_name(name:str):
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


# -------------
# Core Concepts
# -------------

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
                      filter_:Callable[["dict"], bool],
                      mutator:Optional[Callable[["dict"], Any]]=None,
                      ):
        if not callable(filter_):
            raise ValueError(f'provided filter {filter_!r} not callable')
        if mutator and not callable(mutator):
            raise ValueError(f'provided mutator {mutator!r} not callable')
        self.conditions.append((filter_, mutator))

    def __call__(self, event:dict):
        for filter_, mutator in self.conditions:
            try:
                if not filter_(event):
                    return
            except Exception:
                self.logger.exception(
                    'failed when calling filter %r on event %r',
                    filter_, event)
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
        # send should never block
        raise NotImplementedError

    def get_nowait(self):
        """get everything of the receptor"""
        raise NotImplementedError

    async def get(self, timeout=None):
        """returns None if times out"""
        return self.get_nowait()

class FutureReceptor(Receptor):
    """ a future -> value receptor, only wait once, typical for exit receptor """
    def __init__(self, initial_value=False):
        super().__init__()
        # self.future = asyncio.Future()
        self._waiters = set()
        self.value = initial_value
    
    def send(self, event):
        self.value = event
        while self._waiters:
            waiter = self._waiters.pop()
            if not waiter.done():
                waiter.set_result(event)

    def get_nowait(self):
        return self.value

    async def get(self, ):
        # done, pending = await asyncio.wait([self.future], timeout=timeout)
        # regardless of timeout or not, we return the value.
        loop = asyncio.get_running_loop()
        waiter = asyncio.Future(loop=loop)
        self._waiters.add(waiter)
        try:
            await waiter
        except:
            waiter.cancel()
            self._waiters.discard(waiter)
            raise
        return self.value


class QueueReceptor(Receptor):
    def __init__(self):
        super().__init__()
        self.queue = queue.Queue()

    def send(self, event):
        self.queue.put_nowait(event)

    def get_nowait(self):
        """get one item from queue with optional timeout. Return None if
        nothing available.
        """
        try:
            event = self.queue.get_nowait()
        except queue.QueueEmpty:
            return None
        self.queue.task_done()
        return event

    async def get(self):
        return self.queue.get()


class Artifact(object):
    """metadata for artifact"""
    hq:"HQ" = None
    def __init__(self, name:str, path:Path, keep=True, context:Any=None):
        """keep is a hint on whether we intend to keep it.
        Some commands might just need a working dir for artifact.
        """
        self.name = name
        self.path = path
        self.loggers = []
        self.handler = None
        self.keep = True
        self.context = context
        self.upload_state = None  # None / 'uploading' / 'complete'
        self.upload_attempts = 0
        self.last_upload_failure = None
        self.upload_url = None # a path metadata for human comsumption
        self.milestones = {}
        self.mark_state('created')

    def mark_state(self, state):
        self.state = state
        self.milestones[state] = datetime.datetime.utcnow()

    def start(self):
        assert self.state == 'created'
        self.mark_state('started')

        for logger in self.loggers:
            logger.addHandler(self.handler)

    def complete(self):
        assert self.state == 'started', "got state %s" % self.state
        self.mark_state('completed')
        for logger in self.loggers:
            logger.removeHandler(self.handler)

        self.hq.send_event({'kind': 'artifact',
                            'topic': 'complete',
                            'artifact': self})  # for user to find context
        # we don't handle keep here, but other components might decide on how
        # to deal with it

    def destroy(self):
        assert self.state == 'completed'
        shutil.rmtree(self.path)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.complete()


class Task(object):
    # each task can have fixed number of receptors. They should be defined like
    # this:
    # Noting a exit receptor always exists and HQ expects that
    receptors:dict[str, Receptor]

    # The exit receptor should be checked by run function to check whether to
    # terminate itself immediately
    receptor_exit = FutureReceptor

    def __init__(self, name:str):
        if not validate_name(name):
            raise ValueError(
                f"given name {name} is not valid, please sanitize first")
        self.name = name
        # every attr with prefix "receptor_" will be taken as a receptor
        self.receptors = dict((k.split('_', 1)[1], getattr(self, k)())
                              for k in dir(self)
                              if k.startswith('receptor_'))
        # XXX: we are assuming name is unique
        self.logger = logger.getChild(
            self.__class__.__name__).getChild(self.name)
        self.task = None

    def exit(self):
        """intended method to directly terminate this task"""
        self.receptors['exit'].send(True)

    def need_exit(self):
        """should be tested in run"""
        return self.receptors['exit'].get_nowait()

    async def wait_exit(self):
        try:
            return await self.receptors['exit'].get()
        except asyncio.CancelledError:
            return

    async def run(self, hq):
        try:
            await self.run_task(hq)
        except Exception:
            self.logger.exception('task encountered error')

    async def run_task(self, hq):
        raise NotImplementedError

    def send_event(self, hq, event):
        return hq.send_event(event)

    def new_artifact(self, hq, name=None, keep=True):
        return hq.new_artifact(name or self.name, keep) 

    def start(self, hq):
        # copying from threading.Thread
        self.task = asyncio.create_task(self.run(hq), name=self.name)

    async def join(self):
        # only call this after start
        if self.task and not self.task.done():
            await self.task

    def is_alive(self):
        if self.task:
            return not (self.task.done() or self.task.cancelled())

    def __hash__(self):
        return hash(self.name)  # Task names should be unique

    def __eq__(self, other):
        if not isinstance(other, Task):
            raise ValueError(f'task comparing with nontask {other!r}')
        return self.name == other.name

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.name})'


class HQ(object):
    """
    A one for all manager for Tasks, Rules, Artifacts
    rules: a list of functions to react on events. Rules can be added,
           not removed
    tasks: a list of Task objects
    """
    def __init__(self, name=None, datapath=None, loop=None):
        """by default we use cwd as datapath"""
        # self.tasks = set()
        self.tasks = []
        self.done_tasks = []  # to accelerate
        self.rules = []
        datapath = datapath or os.environ.get('ANYKAP_PATH', os.getcwd())
        self.datapath = Path(datapath).absolute()
        # self.datapath.mkdir(parents=True, exist_ok=True)
        if not self.datapath.is_dir():
            logger.warning('supplied datapath for hq %s not a directory',
                           self.datapath)
        self.running = False
        self._quit = None # external trigger for test
        self.queue = queue.Queue()
        self.artifact_counter = it.count()
        self.artifacts = []
        self.name = name or sanitize_name(platform.node())
        self.logger = logger.getChild('HQ')

    async def run(self):
        assert not self.running
        self.running = True
        for task in self.tasks:
            self.logger.debug('starting task %s', task)
            task.start(self)

        looptask = asyncio.create_task(self.loop(), name='hqloop')
        self._quit = asyncio.Future()
        await self._quit
        waits = []
        for task in self.tasks:
            if task.is_alive():
                task.exit()
                waits.append(asyncio.create_task(task.join(), name=f'wait-for-task-{task.name}'))
        # raise Exception('oops')
        if waits:
            await asyncio.wait(waits)
        self.running = False
        await self.queue.join()
        looptask.cancel()
        await asyncio.wait([looptask])

    def quit(self):
        if self._quit:
            self._quit.set_result(None)

    def quit_threadsafe(self):
        if self._quit:
            self._quit.get_loop().call_soon_threadsafe(self.quit)

    async def loop(self):
        """execute event processing logic for one loop"""
        # we block only the the first one
        # get_args = {'block': True, 'timeout': self.CADENCE}  
        logger = self.logger
        while self.running:
            try:
                event = await self.queue.get()
            except asyncio.CancelledError:
                logger.debug('loop canceled')
                if self.queue.qsize:
                    logger.warning('loop canceled when items in queue')
                raise
            self.queue.task_done()
            logger.debug('got event %s', event)
            self.process_event(event)

    def process_event(self, event):
        for task in self.tasks:
            if not task.is_alive():
                self.tasks.remove(task)
                self.done_tasks.append(task)
                continue

            for f in task.receptors.values():
                try:
                    f(event)
                except Exception:
                    logger.exception(
                        'failed when calling receptor for task %s on event %r',
                        task, event)

        for f in self.rules:
            try:
                f(event)
            except Exception:
                logger.exception(
                    'failed when calling rule %r on item %r', f, event)

    def add_task(self, task):
        """ add a new task to hq
        """
        # we are assuming tasks are not yet running.
        # assert task.hq == self
        # task.receptors['exit'].add_filter()
        self.logger.debug('add task %r', task)
        if not isinstance(task, Task):
            raise ValueError(f'expecting task, got {task!r}') 
        if task in self.tasks:
            raise RuntimeError(f'task {task!r} already in hq, maybe name dup?')

        # self.tasks.add(task)
        self.tasks.append(task)
        if self.running:
            task.start(self)

    def add_rule(self, rule):
        self.rules.append(rule)

    def send_event(self, event:dict):
        """tasks should call this to transmit new events"""
        self.queue.put_nowait(event)

    def new_artifact(self, name, keep=True, context=None):
        artifact_name = self.gen_artifact_name(name)
        path = self.datapath / name
        if path.exists():
            raise RuntimeError('artifact path %s already exists' % path)

        path.mkdir(parents=True)
        artifact = Artifact(artifact_name, path, keep)
        self.artifacts.append(artifact)
        artifact.hq = self
        return artifact

    def gen_artifact_name(self, name):
        now = datetime.datetime.utcnow().strftime('%Y%m%d%H%M')
        cnt = next(self.artifact_counter)
        node_name = self.name
        return f'{now}-{node_name}-{name}-{cnt}'

    def forget_artifact(self, artifact):
        self.artifacts.remove(artifact)


# ------------------------------------------------------------------------------
# Scenarios
# ------------------------------------------------------------------------------

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
    # SIGTERM_TIMEOUT = 5
    SIGKILL_TIMEOUT = 1
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
                 terminate_timeout=5,
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
        self.terminate_timeout = terminate_timeout

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

    async def run_task(self, hq):
        logger = self.logger
        args = [self.shell]
        if self.shell_args:
            args.extend(self.shell_args)

        args += ['-c', self.command]

        if self.nsenter_args:
            args[0:0] = ['nsenter'] + self.nsenter_args

        # by default, we simply write to artifact directory
        async with contextlib.AsyncExitStack() as stack:
            artifact = stack.enter_context(
                self.new_artifact(hq, keep=self.keep_artifact))

            def prepare_file(mode, filename):
                if mode == 'artifact':
                    return stack.enter_context(
                        # seems to work even when subprocess is test mode
                        open(artifact.path / filename, 'wb'))  
                elif mode == 'null':
                    return async_subproc.DEVNULL
                elif mode == 'stdout':
                    return async_subproc.STDOUT
                elif mode == 'notify':
                    return async_subproc.PIPE
                raise ValueError(f'unrecognized mode {mode!r}')

            stdout = prepare_file(self.stdout_mode, self.STDOUT_FILE)
            stderr = prepare_file(self.stderr_mode, self.STDERR_FILE)
            p = await async_subproc.create_subprocess_exec(
                *args, cwd=str(artifact.path),
                stdout=stdout, stderr=stderr,
            )
            notify_tasks = []
            if self.stdout_mode == 'notify':
                notify_tasks.append(asyncio.create_task(self.notify_output(
                    hq, p.stdout, self.stdout_filter, 'stdout'
                ), name=f'{self.name}-notify-stdout'))
            if self.stderr_mode == 'notify':
                notify_tasks.append(asyncio.create_task(self.notify_output(
                    hq, p.stderr, self.stderr_filter, 'stderr'
                ), name=f'{self.name}-notify-stderr'))

            wait_exit_task = asyncio.create_task(self.wait_exit(), name=f'{self.name}-wait-exit')
            wait_p_task = asyncio.create_task(p.wait(), name=f'{self.name}-wait-p')
            done, pending = await asyncio.wait(
                [wait_exit_task, wait_p_task],
                timeout=self.timeout,
                return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.wait(pending)
            need_cancel = True
            if wait_p_task in done:
                need_cancel = False
                logger.info('script completed')
            elif wait_exit_task in done:
                logger.info('task exited externally')
            else:
                logger.info('reached timeout')

            if need_cancel:
                await self.cancel_process(p)
            # any way, we try to get return code
            result = p.returncode
            logger.info(f'script exit code: {result!r}')
            if notify_tasks:
                await asyncio.wait(notify_tasks)
            (artifact.path / self.RESULT_FILE).write_text(str(result))
            self.send_event(hq, 
                {
                    'kind': 'shell',
                    'topic': 'complete',
                    'status': result,
                    'task_name': self.name,
                })

    async def notify_output(self, hq, stream, pattern, name):
        while not stream.at_eof():
            line = await stream.readline()
            if not line:  # potential EOF emptiness
                continue
            data = line.decode(self.encoding, errors=self.errors)
            extra = {}
            if pattern:
                m = pattern.search(data)
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
                'line': data,
                'task_name': self.name,
            }
            event.update(extra)
            self.send_event(hq, event)

    async def cancel_process(self, p):
        p.terminate()
        try:
            await asyncio.wait_for(p.wait(), timeout=self.terminate_timeout)
            return
        except asyncio.TimeoutError:
            self.logger.warning('terminating process timed out')

        p.kill()
        try:
            await asyncio.wait_for(p.wait(), timeout=self.SIGKILL_TIMEOUT)
            return
        except asyncio.TimeoutError:
            self.logger.error('killing process timed out, giving up')


class REPLHelp(BaseException):
    def __init__(self, message):
        super().__init__(message)

    @property
    def message(self):
        return self.args[0]


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
        logger.debug('call_later(%s,%s,%s)', self.idle_timeout, self.on_timeout, self.progress)
        # asyncio.call_later(self.idle_timeout, self.on_timeout, self.progress)
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

        verdict = b'OK'
        body = []
        try:
            cmdline = shlex.split(query)
            # if not cmdline:
            #     return self.fail('no command found')

            if cmdline and cmdline[0] in {'exit', 'quit'}:
                self.transport.close()
                return

            body = self.server.call_cmd(*cmdline)
        except REPLHelp as e:
            logger.debug('help raised when calling cmd')
            body = list(filter(
                None,
                (line.rstrip() for line in e.message.splitlines())))
        except Exception as e:
            logger.exception('got exception when calling cmd')
            verdict = b'ERR'
            body = [str(e)]

        self.transport.write(verdict + self.newline)
        for row in body:
            self.transport.write(self.sanitize(row) + self.newline)

        self.transport.write(self.newline)
        if verdict == b'ERR':
            self.transport.close()
            return

        self.listening()

    def sanitize(self, data):
        """we make sure there's no empty new lines in data"""
        return re.sub(
            br'(%s)+' % re.escape(self.newline), self.newline,
            data.encode('ascii', errors='replace').rstrip(self.newline))

    def fail(self, reason, *args):
        self.logger.info('connection failed: ' + reason, *args)
        self.transport.close()


class REPLHelpAction(argparse.Action):
    def __init__(self, option_strings, dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS, help=None):
        super().__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        raise REPLHelp(parser.format_help())


class REPLArgumentParser(argparse.ArgumentParser):
    """customize argparse to get help"""
    def __init__(self, *args, add_help=True, **kwargs):
        super().__init__(*args, exit_on_error=False, add_help=False, **kwargs)
        self.register('action', 'replhelp', REPLHelpAction)
        if add_help:
            self.add_argument('-h', '--help', action='replhelp',
                              help='show this help message')

    def exit(self, *args, **kwargs):
        raise RuntimeError('ArgumentParser.exit is accedentially triggered')

    def error(self, message):
        args = {'prog': self.prog, 'message': message}
        detail = '%(prog)s: error: %(message)s\n' % args
        raise REPLHelp(self.format_usage() + detail)


class REPLServer(Task):
    parser = argparse.ArgumentParser('replserver')
    def __init__(self, path, name='replserver', **kwargs):
        """
        path is relative to hq.path
        """
        super().__init__(name=name)
        self.path = path
        self.kwargs = kwargs  # for protocol factory

    async def run_task(self, hq):
        # logging.basicConfig(level=logging.DEBUG)
        server = await asyncio.get_running_loop().create_unix_server(
            lambda: REPLServerProtocol(self, **self.kwargs),
            hq.datapath / self.path)
        async with server:
            await self.wait_exit()

    def call_cmd(self, *args):
        raise NotImplementedError()


class HQREPLServer(REPLServer):
    def __init__(self, path='repl.sock', *args, **kwargs):
        super().__init__(path, *args, **kwargs)
        parser = REPLArgumentParser(prog=self.name)
        subparsers = parser.add_subparsers(dest='command', required=True)
        info = subparsers.add_parser('info', aliases=['i'])
        info.set_defaults(func=self.cmd_info)
        tasks = subparsers.add_parser(
            'tasks', aliases=['t', 'task'], help='task management')
        tasks.set_defaults(func=self.cmd_tasks)
        tasks.add_argument('-s', '--stop', default='list',
                           dest='verb', action='store_const', const='stop',
                           help='stop tasks if provided, otherwise list tasks')
        tasks.add_argument('-r', '--regex', action='store_true',
                           help='filter task name with regular expression')
        tasks.add_argument('-a', '--all', action='store_true',
                           help='show all tasks, including stopped')
        tasks.add_argument('name', nargs='*',
                           help='task name or pattern, must exist for stop')
        artifacts = subparsers.add_parser(
            'artifacts', aliases=['a', 'artifact'], help='artifact management')
        artifacts.set_defaults(func=self.cmd_artifacts)
        send = subparsers.add_parser('send', help='send a event to hq')
        send.add_argument('event', type=json.loads, help='event in json')
        send.set_defaults(func=self.cmd_send)
        self.parser = parser

    async def run_task(self, hq):
        self.hq = hq
        return await super().run_task(hq)

    def call_cmd(self, *args):
        parser = self.parser
        args = parser.parse_args(args)
        self.logger.debug('args: %r', args)
        return args.func(args)

    def cmd_tasks(self, args):
        verb = args.verb
        name = args.name
        regex = args.regex
        if not name and verb == 'stop':
            self.parser.error('task name must exist for stop')
        tasks = list(self.hq.tasks)
        if all and verb == 'list':
            tasks += self.hq.done_tasks
        else:
            tasks = list(task for task in tasks if task.is_alive())

        if name:
            if regex:
                patterns = list(map(re.compile, name))
                tasks = list(task for task in tasks
                             if any(pattern.search(task.name)
                                    for pattern in patterns))
            else:
                tasks = [task for task in tasks if task.name in name]
        if verb == 'list':
            return list(map(self.format_task_tsv, tasks))
        else:
            assert verb == 'stop'
            tasks = list(task for task in tasks if not task.need_exit())
            for task in tasks:
                task.exit()
            return list(map(self.format_task_tsv, tasks))

    def format_task_tsv(self, task):
        running = task.is_alive()
        stopping = False
        if running:
            stopping = task.need_exit()
        return '\t'.join([task.name, str(running), str(stopping)])

    def cmd_artifacts(self, args):
        return []

    def cmd_send(self, args):
        event = args.event
        self.send_event(self.hq, event)
        return []

    def cmd_info(self, args):
        return []


VALID_DOMAINS = [
    '.blob.core.windows.net',
    '.blob.core.chinacloudapi.cn',
    '.blob.core.usgovcloudapi.net',
    '.blob.core.cloudapi.de'
]

AZUREBLOB_ACCOUNT_PATTERN = r'[a-z0-9]{3,24}'
# https://stackoverflow.com/a/35130142/1000290
AZUREBLOB_CONTAINER_PATTERN = r'[a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]'
AZUREBLOB_FQDN_PATTERN = (
    r'(?P<account>' + AZUREBLOB_ACCOUNT_PATTERN + r')'
    + '|'.join(map(re.escape, VALID_DOMAINS)))
# a best-effort IP addr matching
IPPORT_PATTERN = r'(?P<addr>.*?)(:(?P<port>\d+))?$'

def azureblob_make_url(filename:str, url=None, scheme=None, account=None,
                       container=None, directory=None, sas=None, ):
    """
    netloc can be FQDN or ip:port, ip:port should only be for local testing
    account can be either account name or FQDN
    """
    defaults = { # the default setting
        'scheme': 'https',
        'directory': '',
        'sas': '',
    }
    config = {}
    def set_config(k, v):
        if v:
            if config.get(k, v) != v:
                raise ValueError(f'inconsistent setting for {k}: '
                        f'{config[k]!r} != {v!r}')
            config[k] = v

    set_config('scheme', scheme)
    if account:
        if re.fullmatch(AZUREBLOB_ACCOUNT_PATTERN, account):
            set_config('account', account)
        else:
            m = re.fullmatch(AZUREBLOB_FQDN_PATTERN, account)
            if m:
                d = m.groupdict()
                set_config('account', d['account'])
                set_config('fqdn', account)
            else:
                raise ValueError(
                    f'account {account} is not valid Azure storage account')

    set_config('container', container)
    set_config('directory', directory)
    if sas:
        sas = sas.lstrip('?')
    set_config('sas', sas)

    if url:
        use_ipaddr=False
        parsed = urllib.parse.urlparse(url)
        set_config('scheme', parsed.scheme)
        if parsed.netloc:
            m = re.fullmatch(AZUREBLOB_FQDN_PATTERN, parsed.netloc)
            if m:
                d = m.groupdict()
                set_config('account', d['account'])
                set_config('fqdn', parsed.netloc)
            else:
                m = re.fullmatch(IPPORT_PATTERN, parsed.netloc)
                if not m:
                    raise ValueError(
                        f'failed to parse netloc {parsed.netloc} as ip:port')
                d = m.groupdict()
                try:
                    ipaddress.ip_address(d['addr'])
                except ValueError as e:
                    raise ValueError(f'unable to parse addr {d["addr"]}') from e
                set_config('ipport',  parsed.netloc)

        if parsed.fragment or parsed.params:
            raise ValueError(f'unrecognizable url {url}')
        path = [None] * 2
        if parsed.path:
            path = parsed.path.strip('/').split('/')
            if 'ipport' in config:
                if path:
                    set_config('account', path.pop(0))
            if path:
                set_config('container', path.pop(0))
            if path:
                set_config('directory', '/'.join(path))

    config = defaults | config
    try:
        if not re.fullmatch(AZUREBLOB_ACCOUNT_PATTERN, config['account']):
            raise ValueError(f'invalid account {account}')
        if not re.fullmatch(AZUREBLOB_CONTAINER_PATTERN, config['container']):
            raise ValueError(f'invalid container {container}')
        path = '/' + config['container'] + '/'
        if config['directory']:
            path += config['directory'] + '/'
        path += filename
        if 'ipport' in config:
            path = '/' + config['account'] + path
            return urllib.parse.urlunparse((
                config['scheme'], config['ipport'], path, '', config['sas'], '')
            )
        elif 'fqdn' not in config:
            config['fqdn'] = config['account'] + '.blob.core.windows.net'
        return urllib.parse.urlunparse(
            (config['scheme'], config['fqdn'], path, '', config['sas'], '')
        )
    except KeyError as e:
        key, = e.args
        raise ValueError(f'key {key} not provided in upload setting')


def urlopen_worker(request, options):
    logger.debug('sending request with headers: %s', request.headers)
    with urllib.request.urlopen(request, **options) as f:
        logger.debug('url %s got status: %s, headers: %s',
                     f.url, f.status, f.headers)
        logger.debug('body: %r', f.read())


class Uploader(object):
    """uploader interface"""
    async def upload(path:Path):
        """path given is a file, not supposed to be changing"""
        raise NotImplementedError 


class AzureBlobUploader(Uploader):
    def __init__(self, urlopen_options=None, **kwargs):
        super().__init__()
        self.urlopen_options = urlopen_options
        # validate the storage option just to be sure
        try:
            azureblob_make_url('contoso', **kwargs)
        except Exception as e:
            raise ValueError('Invalid azure blob configuration') from e
        self.blob_options = kwargs
        self.urlopen_options = urlopen_options or {}

    async def upload(self, path):
        path = Path(path)
        if not path.is_file():
            raise RuntimeError(f'path {path} not a file')

        upload_url = azureblob_make_url(path.name, **self.blob_options)
        logger.debug('uploading using url %s', upload_url)
        # caller should guarantee sure no further change to the file
        size = path.stat().st_size  
        with path.open('rb') as f:
            # https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob
            request = urllib.request.Request(
                url=upload_url, method="PUT", data=f, headers={
                    'Content-Length': str(size),
                    'Content-Type': 'application/octet-stream',
                    'User-Agent': USER_AGENT,
                    'Date': datetime.datetime.utcnow().strftime(
                        '%a, %d %b %Y %H:%M:%S GMT'),
                    'x-ms-version': '2023-11-03',
                    'x-ms-blob-type': 'BlockBlob',
                })
            # we don't want loop to be blocked, therefore using thread pool.
            await asyncio.get_running_loop().run_in_executor(
                thread_pool, urlopen_worker, request, self.urlopen_options)


class CopyUploader(Uploader):
    def __init__(self, destdir):
        destdir = Path(destdir)
        if not destdir.is_dir():
            raise RuntimeError(f'dest {destdir} not a directory')
        self.destdir = destdir

    async def upload(self, path):
        if not path.is_file():
            raise RuntimeError(f'path {path} is not a file')

        await asyncio.get_running_loop().run_in_executor(
            thread_pool, shutil.copyfile, path, self.destdir/path.name
        )

class Archiver(object):
    """archives a directory into a file"""
    async def archive(self, path):
        pass



class ArtifactManager(Task):
    """upload manager is a optional rule that handles archiving and uploading
    basically, manager picks up artifact completion events, then
    archive -> upload the artifact. Marks artifact uploaded / failed accordingly
    """
    def __init__(self, archiver, uploader, name='artifact-manager'):
        super().__init__(self, name)
        self.receptors['']
    # XXX: handle exceptions and retry. currently we mark artifact as failed
    # def filter_event()

