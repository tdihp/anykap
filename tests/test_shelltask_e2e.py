# pytest

from anykap import *

logging.basicConfig(level=logging.DEBUG)


def test_shelltask_e2e(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    hq = HQ(tmp_path / 'hq')
    hq.add_task(ShellTask('foobar', 'echo "FooBar!"; exit 42'))
    result = False
    def myrule(event):
        if event['kind'] == 'shell':
            nonlocal result
            result = event['topic'] == 'complete' and event['status'] == 42

        hq.running = False

    hq.add_rule(myrule)
    hqthread = threading.Thread(target=hq.run)
    hqthread.start()
    hqthread.join(timeout=3)  # it should be fairly quick
    assert not hqthread.is_alive()
    assert result
    # we verify artifacts are there
    my_artifact, = hq.artifacts
    set(child.name for child in my_artifact.path.iterdir()) >= {'sh.stdout', 'sh.stderr', 'sh.result', 'anykap.log'}
    assert (my_artifact.path / 'sh.stdout').read_text().strip() == 'FooBar!'




