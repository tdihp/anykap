from collections.abc import Callable
from typing import Optional, Any, Type, Union, Literal, NewType
from dataclasses import dataclass, field, make_dataclass, asdict
from collections import defaultdict
import os
from pathlib import Path
import itertools as it
import asyncio
import asyncio.queues as queue
import asyncio.subprocess
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
import tempfile
from concurrent.futures import ThreadPoolExecutor
import logging

# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------
# XXX: make __all__
# make sure we follow https://packaging.python.org/en/latest/specifications/version-specifiers/#version-scheme
__version__ = '0.1.0-dev'
USER_AGENT = f'anykap/{__version__}'
logger = logging.getLogger('anykap')

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
    return re.sub(r'[^a-z0-9_-]', '',
                  re.sub(r'\s+', '_', name.strip().lower())).strip('_-')[:255]

@contextlib.asynccontextmanager
async def subprocess_teardown(p, terminate_timeout, kill_timeout,
                              logger=logger):
    """this context manager tears down a asyncio.Process when any exception
    occurs and the subprocess is not complete. Users should wait with timeout
    in the context block"""
    try:
        yield p
    finally:
        if p.returncode != None:
            return

        logger.warning('tearing down subprocess %r with timeouts %s and %s',
                       p, terminate_timeout, kill_timeout)
        logger.info('sending terminate')
        p.terminate()
        try:
            await asyncio.wait_for(p.wait(), timeout=terminate_timeout)
        except asyncio.TimeoutError:
            logger.warning('terminate timed out')
        else:
            if p.returncode != None:
                logger.info('terminate complete')
            else:
                logger.warning('terminate interrupted')
            return

        logger.info('sending kill')
        p.kill()
        try:
            await asyncio.wait_for(p.wait(), timeout=terminate_timeout)
        except asyncio.TimeoutError:
            logger.warning('kill timed out')
        else:
            if p.returncode != None:
                logger.info('kill complete')
            else:
                logger.warning('kill interrupted')

# -------------
# Core Concepts
# -------------
Event=NewType('Event', dict)
EventProcessor=NewType('EventProcessor', Callable[[Event], None])
EventFilter=NewType('EventFilter', Callable[[Event], bool])
EventMutator=NewType('EventMutator', Callable[[Event], Any])

@dataclass(init=False, eq=False)
class Receptor:
    """
    Receptor is a EventProcessor with a list of conditions.
    Conditions can be added to decide whether the testing event is subscribed.
    """
    conditions:list[tuple[EventFilter, Optional[EventMutator]]]
    def __init__(self):
        self.conditions = []
        self.logger = logger.getChild(self.__class__.__name__)

    def add_condition(self,
                      filter_:EventFilter,
                      mutator:Optional[EventMutator]=None,
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
    """ a future -> value receptor, only wait once,
    typical for exit receptor """
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

    async def get(self,):
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
        return await self.queue.get()


@dataclass
class Artifact:
    """metadata for artifact"""
    # metadata
    name:str
    path:Path
    keep:bool = True
    context:Any = None
    milestones:dict[str, datetime.datetime] = field(default_factory=dict)
    # lifecycle
    state:str = 'created'
    upload_state:Optional[str] = None
    upload_attempts:int = 0
    last_upload_failure:Optional[Exception] = None
    upload_url:Optional[str] = None

    def __post_init__(self):
        self.hq = None
        self.mark_state('created')

    def mark_state(self, state):
        self.state = state
        self.milestones[state] = datetime.datetime.utcnow()

    def mark_upload_state(self, state):
        self.upload_state = state
        self.milestones[state] = datetime.datetime.utcnow()

    def upload_complete(self, upload_url):
        self.upload_url = upload_url
        self.mark_upload_state('completed')
        self.hq.send_event({'kind': 'artifact',
                            'topic': 'uploaded',
                            'artifact': self})  # for user to find context

    def start(self):
        assert self.state == 'created'
        self.mark_state('started')
        # for logger in self.loggers:
        #     logger.addHandler(self.handler)

    def complete(self):
        assert self.state == 'started', "got state %s" % self.state
        self.mark_state('completed')
        # for logger in self.loggers:
        #     logger.removeHandler(self.handler)
        self.hq.send_event({'kind': 'artifact',
                            'topic': 'complete',
                            'artifact': self,
                            })  # for user to find context

    def destroy(self):
        assert self.state == 'completed'
        shutil.rmtree(self.path)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.complete()


task_dataclass = dataclass(eq=False, unsafe_hash=False)

_TaskBase = make_dataclass('Task', [
        ('name', Optional[str], None),
        ('receptors', dict[str, Receptor], field(init=False)),
        ('running', bool, field(init=False)),
        ('exiting', bool, field(init=False)),
    ], eq=False, unsafe_hash=False,
    namespace={'__post_init__': lambda self: None},
)

class Task(_TaskBase):
    counterdict = defaultdict(it.count)
    def __post_init__(self):
        super().__post_init__()
        if self.name is None:
            clsname = sanitize_name(self.__class__.__name__)
            id_ = next(self.counterdict[clsname])
            self.name = f'{clsname}-{id_}'

        if not validate_name(self.name):
            raise ValueError(
                f"given name {self.name} is not valid, please sanitize first")

        # each task can have fixed number of receptors to subscribe events
        self.receptors = {'exit': FutureReceptor()}

        self.logger = logger.getChild(
            self.__class__.__name__).getChild(self.name)

        self._task = None

    @property
    def running(self):
        return bool(self._task) and not self._task.done()

    @property
    def exiting(self):
        return self.running and self.need_exit()

    def exit(self):
        """method to directly terminate this task
        """
        self.receptors['exit'].send(True)

    def need_exit(self):
        """should be tested in run"""
        return self.receptors['exit'].get_nowait()

    async def wait_exit(self):
        return await self.receptors['exit'].get()

    async def run(self, hq):
        run_task = asyncio.create_task(self.run_task(hq),
                                       name=f'{self.name}-run_task')
        wait_exit_task = asyncio.create_task(self.wait_exit(),
                                             name=f'{self.name}-wait_exit')
        done, pending = await asyncio.wait([run_task, wait_exit_task],
                                           return_when=asyncio.FIRST_COMPLETED)
        logger.debug('done: %s, pending: %s', done, pending)
        if wait_exit_task in done:
            assert run_task in pending
            run_task.cancel()
        else:
            assert run_task in done
            assert wait_exit_task in pending
            wait_exit_task.cancel()

        try:
            await wait_exit_task
        except asyncio.CancelledError:
            pass

        try:
            await run_task
        except asyncio.CancelledError:
            self.logger.info('task %s canceled', self.name)
        except BaseException:
            self.logger.exception('task %s encountered error', self.name)
            # XXX: shall we set this to a field so it is visible in CLI?
            raise

    async def run_task(self, hq):
        raise NotImplementedError

    def send_event(self, hq, event):
        return hq.send_event(event)

    def new_artifact(self, hq, name=None, keep=True):
        return hq.new_artifact(name or self.name, keep, context=self)

    def start(self, hq):
        # copying from threading.Thread
        if self._task:
            raise RuntimeError('start called twice')
        self._task = asyncio.create_task(self.run(hq), name=self.name)

    async def join(self):
        # only call this after start
        if self._task and not self._task.done():
            await self._task

class HQ:
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
        datapath = datapath or os.environ.get('ANYKAP_DATAPATH', os.getcwd())
        self.datapath = Path(datapath).absolute()
        logger.info('hq datapath: %s', self.datapath)
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
            if task.running:
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
            if not task.running:
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
        path = self.datapath / artifact_name
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

@task_dataclass
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
    script:str = ''
    nsenter_args:Optional[list[str]] = None
    shell:str = 'bash'
    shell_args:Optional[float] = None
    timeout:Optional[float] = None
    keep_artifact:bool = True
    encoding:str = 'utf-8'
    errors:str = 'replace'
    stdout_mode:Literal['artifact','notify','null'] = 'artifact'
    stderr_mode:Literal['artifact','notify','null','stdout'] = 'artifact'
    stdout_filter:Optional[str]=None
    stderr_filter:Optional[str]=None
    stdout_file:str = 'sh.stdout'
    stderr_file:str = 'sh.stderr'
    result_file:str = 'sh.result'
    terminate_timeout:float = 5
    kill_timeout:float = 1
    popen_kw:dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        super().__post_init__()
        if not self.script:
            raise ValueError('script is not provided')
        # XXX: more validations

    async def run_task(self, hq):
        logger = self.logger
        args = [self.shell]
        if self.shell_args:
            args.extend(self.shell_args)

        args += ['-c', self.script]

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
                    return asyncio.subprocess.DEVNULL
                elif mode == 'stdout':
                    return asyncio.subprocess.STDOUT
                elif mode == 'notify':
                    return asyncio.subprocess.PIPE
                raise ValueError(f'unrecognized mode {mode!r}')

            stdout = prepare_file(self.stdout_mode, self.stdout_file)
            stderr = prepare_file(self.stderr_mode, self.stderr_file)
            logger.info('starting subprocess %s', args)
            p = await asyncio.subprocess.create_subprocess_exec(
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

            wait_p_task = asyncio.create_task(p.wait(),
                                              name=f'{self.name}-wait-p')
            all_tasks = [wait_p_task] + notify_tasks
            try:
                async with subprocess_teardown(
                    p, self.terminate_timeout, self.kill_timeout, logger):
                    done, pending = await asyncio.wait(
                        all_tasks, timeout=self.timeout,
                        return_when='FIRST_EXCEPTION')
                    if pending:
                        logger.debug('cancelling tasks in pending: %s', pending)
                        for task in pending:
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                logger.debug('task %s cancelled', task)

                    # the callstacks will be captured anyway, evaluate those in
                    # done so the errors get propergated
                    if done:
                        logger.debug('done tasks: %s', done)
                        asyncio.gather(*done)
            finally:
                # the subprocess should already be canceled
                result = p.returncode
                logger.info(f'script exit code: {result!r}')
                (artifact.path / self.result_file).write_text(str(result))
                self.send_event(hq, 
                    {
                        'kind': 'shell',
                        'topic': 'complete',
                        'status': result,
                        'task_name': self.name,
                    })

    async def notify_output(self, hq, stream, pattern, name):
        if pattern:
            pattern = re.compile(pattern)
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
            await asyncio.wait_for(p.wait(), timeout=self.kill_timeout)
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
        await server.serve_forever()

    def call_cmd(self, *args):
        raise NotImplementedError()


class HQREPLServer(REPLServer):
    TASK_KEYS = ['name', 'running', 'exiting']
    ARTIFACT_KEYS = ['name', 'state', 'path', 'upload_state', 'upload_url']

    def __init__(self, path=None, *args, **kwargs):
        if path is None:
            path = os.environ.get('ANYKAP_SERVERPATH', 'repl.sock')
        super().__init__(path, *args, **kwargs)
        parser = REPLArgumentParser(prog=self.name)
        subparsers = parser.add_subparsers(dest='command', required=True)
        info = subparsers.add_parser('info', aliases=['i'])
        info.set_defaults(func=self.cmd_info)
        tasks = subparsers.add_parser(
            'tasks', aliases=['t', 'task'], help='task management')
        tasks.set_defaults(func=self.cmd_tasks)
        tasks.add_argument('-s', '--stop', action='store_true',
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
        # artifacts.add_argument('', help='')
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
        if not args.name and args.stop:
            self.parser.error('task name must exist for stop')
        tasks = list(self.hq.tasks)
        if args.all and not args.stop:
            tasks += self.hq.done_tasks
        else:
            tasks = list(task for task in tasks if task.running)

        if args.name:
            if args.regex:
                patterns = list(map(re.compile, args.name))
                tasks = list(task for task in tasks
                             if any(pattern.search(task.name)
                                    for pattern in patterns))
            else:
                tasks = [task for task in tasks if task.name in args.name]
        if args.stop:
            tasks = list(task for task in tasks if not task.need_exit())
            for task in tasks:
                task.exit()

        result = ['\t'.join(self.TASK_KEYS)]
        result.extend('\t'.join(str(d.get(k, '')) for k in self.TASK_KEYS)
                      for d in map(asdict, tasks))
        return result

    def cmd_artifacts(self, args):
        artifacts = list(self.hq.artifacts)
        result = ['\t'.join(self.ARTIFACT_KEYS)]
        result.extend('\t'.join(str(d.get(k, '')) for k in self.ARTIFACT_KEYS)
                      for d in map(asdict, artifacts))
        return result

    def cmd_send(self, args):
        event = args.event
        self.send_event(self.hq, event)
        return []

    def cmd_info(self, args):
        return []


class Uploader:
    """uploader interface. Uploader should eitehr implement the async or the
    sync version"""

    async def upload_async(self, path:Path, name:str):
        raise NotImplementedError

    def upload_sync(self, path:Path, name:str):
        raise NotImplementedError


# @task_dataclass
class ArtifactManager(Task):
    """upload manager is a optional rule that handles archiving and uploading
    basically, manager picks up artifact completion events, then
    archive -> upload the artifact. Marks artifact uploaded / failed accordingly
    """
    # uploader:Uploader = None
    # archiver:Callable[[Path], None] = None
    # def __post_init__
    def __init__(self, archiver, uploader, name='artifact-manager',):
        super().__init__(name=name)
        self.receptors['artifact'] = QueueReceptor()
        self.receptors['artifact'].add_condition(self.filter_event)
        self.uploader = uploader
        self.archiver = archiver
        self.executor = ThreadPoolExecutor()
    # XXX: handle exceptions and retry. currently we mark artifact as failed

    def filter_event(self, event):
        return (event.get('kind') == 'artifact'
                and event.get('topic') == 'complete')

    async def process_artifact(self, artifact):
        if not artifact.keep:
            self.logger.info('skipping non-keep artifact %r', artifact)
        artifact.mark_upload_state('archiving')
        dest_path = self.hq.datapath / 'archive'
        dest_path.mkdir(exist_ok=True, parents=True)
        loop = asyncio.get_running_loop()
        try:
            self.logger.debug(
                'calling archiver: %s %s %s %s',
                self.executor, self.archiver, artifact.path, dest_path)
            fpath = await loop.run_in_executor(
                self.executor, self.archiver, artifact.path, dest_path)
        except:
            artifact.mark_upload_state('failed')
            raise
        fpath = Path(fpath)
        destname = fpath.with_stem(artifact.name).name
        artifact.mark_upload_state('uploading')
        try:
            try:
                result = await self.uploader.upload_async(fpath, destname)
            except NotImplementedError:
                result = await loop.run_in_executor(
                    self.executor, self.uploader.upload_sync, fpath, destname)
        except:
            # logger.exception('failed uploading')
            artifact.mark_upload_state('failed')
            raise
        finally:
            try:
                fpath.unlink()
            except:
                logger.exception('failed removing file %s', fpath)
        artifact.upload_complete(str(result))

    async def loop(self):
        while True:
            try:
                event = await self.receptors['artifact'].get()
            except asyncio.CancelledError:
                return

            artifact = event['artifact']
            try:
                await self.process_artifact(artifact)
            # except asyncio.CancelledError: # XXX try finish upload?
            except:
                logger.exception('processing artifact failed')

    async def run_task(self, hq):
        self.hq = hq
        loop_task = asyncio.create_task(self.loop(), name=f'{self.name}-loop')
        await self.wait_exit()
        loop_task.cancel()
        await asyncio.wait([loop_task])


class CopyUploader(Uploader):
    """This uploader simply copies files as-is to destination
    useful for copying to volumes"""
    def __init__(self, destdir):
        destdir = Path(destdir)
        if not destdir.is_dir():
            raise RuntimeError(f'dest {destdir} not a directory')
        self.destdir = destdir

    def upload_sync(self, path, name):
        if not path.is_file():
            raise RuntimeError(f'path {path} is not a file')
        abspath = (self.destdir/name).absolute()
        shutil.copyfile(path, abspath)
        return 'file://' + str(abspath)


@contextlib.contextmanager
def preserving_tempfile(*args, **kwargs):
    """keep tempfile if no exception"""
    fd, fpath = tempfile.mkstemp(*args, **kwargs)
    try:
        yield fd, fpath
    except:
        logger.exception('got exception when writing archive, removing')
        try:
            os.remove(fpath)
        except:
            logger.exception('failed removing %r', fpath)
        raise


def archive_tar(datapath:str, outpath:str, basedir:str=None, compressor='gz',
                **kwargs):
    import tarfile
    suffix = '.tar'
    tarmode = 'w'
    if not compressor in (None, 'gz', 'bz2', 'xz'):
        raise ValueError(f'invalid compressor {compressor!r}')
    if compressor:
        suffix += '.' + compressor
        tarmode += ':' + compressor
    basedir = basedir or os.path.basename(datapath)
    stack = contextlib.ExitStack()
    with stack:
        fd, fpath = stack.enter_context(
            preserving_tempfile(dir=outpath, prefix='archive-', suffix=suffix))
        f = stack.enter_context(open(fd, 'wb'))
        tf = stack.enter_context(
            tarfile.open(fileobj=f, mode=tarmode, **kwargs))
        tf.add(datapath, basedir)
    return fpath


def archive_zip(datapath:str, outpath:str, basedir:str=None, **kwargs):
    import zipfile
    basedir = basedir or os.path.basename(datapath)
    stack = contextlib.ExitStack()
    with stack:
        fd, fpath = stack.enter_context(
            preserving_tempfile(dir=outpath, prefix='archive-', suffix='.zip'))
        f = stack.enter_context(open(fd, 'wb'))
        zf = stack.enter_context(
            zipfile.ZipFile(f, mode='w', **kwargs))
        for dirpath, dirnames, filenames in os.walk(datapath):
            relpath = os.path.relpath(dirpath, datapath)
            for fn in filenames:
                zf.write(os.path.join(dirpath, fn), os.path.join(basedir, relpath, fn))
    return fpath


# -------------
# CRI discovery
# -------------
class CRICtlData:
    OBJTYPE:str
    LIST_KEY:str
    INSPECT_KEY:str
    DATA_LIST_KEY:str
    RUN_TIMEOUT:int = 3
    TERMINATE_TIMEOUT = 3
    KILL_TIMEOUT = 1
    DATA_KEYS:set[str] = set()
    INSPECT_KEYS:set[str] = set()

    @classmethod
    async def run_crictl(cls, args):
        logger.debug('running command %r', args)
        p = await asyncio.subprocess.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        async with subprocess_teardown(
            p, cls.TERMINATE_TIMEOUT, cls.KILL_TIMEOUT):
            try:
                stdout, stderr = await asyncio.wait_for(
                    p.communicate(), timeout=cls.RUN_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning('crictl command %r timed out in %r secs',
                            args, cls.RUN_TIMEOUT)
                raise

        if p.returncode != 0:
            logger.warning('crictl returncode: %r stdout: %r stderr: %r',
                           p.returncode, stdout, stderr)
            raise RuntimeError(
                f'crictl command {args!r} got return code {p.returncode}')
        try:
            data = json.loads(stdout.decode())
        except Exception as e:
            logger.error('failed when decoding crictl output: %s', e)
            raise
        return data

    @classmethod
    async def list(cls, **kwargs):
        # XXX: with optional chroot prepended
        args = ['crictl', cls.LIST_KEY, '-o', 'json'] + cls.query_args(**kwargs)
        data = await cls.run_crictl(args)
        return [cls(o) for o in data[cls.DATA_LIST_KEY]]

    async def run_inspect(self):
        return await self.run_crictl(
            ['crictl', self.INSPECT_KEY, self.id, '-o', 'json'])

    @classmethod
    def query_args(cls, **kwargs):
        return []

    def __init__(self, data, inspect=None):
        self._data = data
        self._lock = asyncio.Lock()
        self._inspect = inspect

    async def inspect_once(self):
        # lazily inspect things
        if self._inspect is None:
            async with self._lock:
                self._inspect = await self.run_inspect()

        return self._inspect

    @property
    def inspect(self):
        if self._inspect:
            return self._inspect
        raise Exception('inspect_once should be called before accessing')

    def __getattr__(self, name):
        if name in self.DATA_KEYS:
            return self._data[name]

        if name in self.INSPECT_KEYS:
            return self.inspect[name]

        raise AttributeError("attribute %s is not known" % name)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if self.OBJTYPE == getattr(other, 'OBJTYPE'):
            return self.id == other.id

        raise ValueError(f'invalid comparison {self!r} with {other!r}')


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


class CRIDiscovery(Task):
    """basic CRI discovery"""

    def __init__(self, datacls:Type[CRICtlData], name=None, cadence=30,
                 **query):
        super().__init__(name=name)
        self.datacls = datacls
        self.cadence = cadence
        self.query = query
        self.watching = set()

    async def run_once(self):
        result = set(await self.datacls.list(**self.query))
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

    async def run_task(self):
        async def loop():
            while not self.need_exit():
                await self.run_once()
                await asyncio.sleep(self.cadence)

        try:
            loop_task = asyncio.create_task(loop(), name=f'{self.name}-loop')
            await self.wait_exit()
        finally:
            loop_task.cancel()
            await loop_task


def main(hq):
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(hq.run())
