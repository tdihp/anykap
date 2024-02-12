from anykap import *
import pytest


async def test_shelltask_simple(hq, hqtask):
    result = asyncio.Future()
    def myrule(event):
        if event['kind'] == 'shell':
            result.set_result(event['topic'] == 'complete' and event['status'] == 42)

    hq.add_rule(myrule)
    hq.add_task(ShellTask('foobar', 'echo "FooBar!"; exit 42'))
    await asyncio.wait_for(result, timeout=3)
    assert result.result
    # we verify artifacts are there
    my_artifact, = hq.artifacts
    set(child.name for child in my_artifact.path.iterdir()) >= {'sh.stdout', 'sh.stderr', 'sh.result', 'anykap.log'}
    assert (my_artifact.path / 'sh.stdout').read_text().strip() == 'FooBar!'


async def test_shelltask_notify(hq, hqtask):
    q = queue.Queue()

    def myrule(event):
        if event.get('kind') == 'shell' and event.get('topic') == 'line':
            q.put_nowait(event)

    hq.add_rule(myrule)
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
    event = await asyncio.wait_for(q.get(), timeout=3)
    q.task_done()
    assert event['kind'] == 'shell' and event['topic'] == 'line'
    assert event['line'].strip() == 'FooBar!' and event['output'] == 'stdout'
    await asyncio.wait_for(outnotify.join(), timeout=1)
    assert q.empty()  # we should only have one of such event for now
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
    event = await asyncio.wait_for(q.get(), timeout=3)
    q.task_done()
    assert event['kind'] == 'shell' and event['topic'] == 'line'
    assert event['line'].strip() == 'someerror' and event['output'] == 'stderr'
    await asyncio.wait_for(errnotify.join(), timeout=1)
    assert q.empty

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
    shellresult = [await asyncio.wait_for(q.get(), timeout=3) for i in range(len(myresult))]
    for i in shellresult:
        q.task_done()

    await regexnotify.join()
    assert q.empty()
    assert set(e['groupdict']['num'] for e in shellresult) == myresult
