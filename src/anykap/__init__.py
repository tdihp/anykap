from collections.abc import Callable
from typing import Optional, Any, Type, Union, Literal
import os
from pathlib import Path
import itertools as it
import asyncio
import asyncio.queues as queue
import asyncio.subprocess as subprocess
import datetime
import re
import contextlib
import platform
import shutil
import time
import logging

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
        self.future = asyncio.Future()
        self.value = initial_value
    
    def send(self, event):
        # unconditionally mark future done
        if self.future.done():
            return

        self.value = event
        self.future.set_result(None)

    def get_nowait(self):
        return self.value

    async def get(self, ):
        # done, pending = await asyncio.wait([self.future], timeout=timeout)
        # regardless of timeout or not, we return the value.
        await self.future
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
        if self.task:
            await self.task

    def is_alive(self):
        if self.task:
            return not (self.task.done() or self.task.cancelled())

class HQ(object):
    """
    A one for all manager for Tasks, Rules, Artifacts
    rules: a list of functions to react on events. Rules can be added,
           not removed
    tasks: a list of Task objects
    """
    def __init__(self, name=None, datapath=None, loop=None):
        """by default we use cwd as datapath"""
        self.tasks = []  # XXX: should be dict for uniqueness?
        self.done_tasks = []  # to accelerate
        self.rules = []
        datapath = datapath or os.environ.get('ANYKAP_PATH', os.getcwd())
        self.datapath = Path(datapath).absolute()
        self.datapath.mkdir(parents=True, exist_ok=True)
        self.running = False
        self.quit = asyncio.Future(loop=loop) # external trigger for test
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

        looptask = asyncio.create_task(self.loop())
        await self.quit
        waits = []
        for task in self.tasks:
            if task.is_alive():
                task.exit()
                waits.append(task)
        asyncio.gather(*waits)
        self.running = False
        await self.queue.join()
        looptask.cancel()
        try:
            await looptask
        except asyncio.CancelledError:
            pass

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
    SIGTERM_TIMEOUT = 5
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
                    return subprocess.DEVNULL
                elif mode == 'stdout':
                    return subprocess.STDOUT
                elif mode == 'notify':
                    return subprocess.PIPE
                raise ValueError(f'unrecognized mode {mode!r}')

            stdout = prepare_file(self.stdout_mode, self.STDOUT_FILE)
            stderr = prepare_file(self.stderr_mode, self.STDERR_FILE)
            p = await subprocess.create_subprocess_exec(
                *args, cwd=str(artifact.path),
                stdout=stdout, stderr=stderr,
            )
            notify_tasks = []
            if self.stdout_mode == 'notify':
                notify_tasks.append(asyncio.create_task(self.notify_output(
                    hq, p.stdout, self.stdout_filter, 'stdout'
                )))
            if self.stderr_mode == 'notify':
                notify_tasks.append(asyncio.create_task(self.notify_output(
                    hq, p.stderr, self.stderr_filter, 'stderr'
                )))

            wait_exit_task = asyncio.create_task(self.wait_exit())
            wait_p_task = asyncio.create_task(p.wait())
            done, pending = await asyncio.wait(
                [wait_exit_task, wait_p_task],
                timeout=self.timeout,
                return_when=asyncio.FIRST_COMPLETED)

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
            await asyncio.wait_for(p.wait(), timeout=self.SIGTERM_TIMEOUT)
            return
        except asyncio.TimeoutError:
            self.logger.warning('terminating process timed out')

        p.kill()
        try:
            await asyncio.wait_for(p.wait(), timeout=self.SIGKILL_TIMEOUT)
            return
        except asyncio.TimeoutError:
            self.logger.error('killing process timed out, giving up')
