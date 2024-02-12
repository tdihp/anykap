# pytest
import queue
from anykap.sync import *


# logging.basicConfig(level=logging.DEBUG)


def test_shelltask_simple(hq, hqthread):
    hq.add_task(ShellTask('foobar', 'echo "FooBar!"; exit 42'))
    result = False
    def myrule(event):
        if event['kind'] == 'shell':
            nonlocal result
            result = event['topic'] == 'complete' and event['status'] == 42

    hq.add_rule(myrule)
    hqthread.start()
    time.sleep(1)
    assert result
    # we verify artifacts are there
    my_artifact, = hq.artifacts
    set(child.name for child in my_artifact.path.iterdir()) >= {'sh.stdout', 'sh.stderr', 'sh.result', 'anykap.log'}
    assert (my_artifact.path / 'sh.stdout').read_text().strip() == 'FooBar!'


def test_shelltask_notify(hq, hqthread):
    q = queue.SimpleQueue()

    def myrule(event):
        # nonlocal q
        if event.get('kind') == 'shell' and event.get('topic') == 'line':
            q.put(event)

    hq.add_rule(myrule)
    hqthread.start()
    outnotify = ShellTask(
        'foobarnotify',
        r'''
        echo "should ignore" >&2
        echo "FooBar!"
        exit 42
        ''',
        stdout_mode='notify',
        stderr_mode='null'
    )
    hq.add_task(outnotify)
    event = q.get(timeout=1)
    assert event['kind'] == 'shell' and event['topic'] == 'line'
    assert event['line'].strip() == 'FooBar!' and event['output'] == 'stdout'
    outnotify.join(1)
    assert not outnotify.is_alive()
    assert q.empty()

    errnotify = ShellTask(
        'errnotify',
        r'''
        echo "someerror" >&2
        echo "should ignore"
        exit 10''',
        stdout_mode='null',
        stderr_mode='notify',
    )
    hq.add_task(errnotify)
    event = q.get(timeout=1)
    assert event['kind'] == 'shell' and event['topic'] == 'line'
    assert event['line'].strip() == 'someerror' and event['output'] == 'stderr'
    errnotify.join(1)
    assert not errnotify.is_alive()
    assert q.empty()

    regexnotify = ShellTask(
        'regexnotify',
        r'''
        seq 100 | xargs -I{} bash -c 'echo "log info {}"'
        sleep 1
        ''',
        stdout_mode='notify',
        stdout_filter=r'log info (?P<num>\d*9\d*)'
    )
    hq.add_task(regexnotify)
    myresult = [str(i) for i in range(1, 101)]
    myresult = set(s for s in myresult if '9' in s)
    shellresult = [q.get(timeout=3) for i in range(len(myresult))]
    assert set(e['groupdict']['num'] for e in shellresult) == myresult
    regexnotify.join(1)
    assert not regexnotify.is_alive()
    assert q.empty()
