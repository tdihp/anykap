
from anykap import *
import socket
import pytest
import contextlib


@pytest.fixture
def replserver(tmp_path):
    class MyREPLServer(REPLServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.commands = {
                'ping': self.cmd_ping,
                'nping': self.cmd_nping,
            }

        def cmd_ping(self):
            print('got ping!')
            return ['pong']

        def cmd_nping(self, num):
            return ['pong'] * int(num)

        def need_exit(self):
            """should be tested in run"""
            print('need_exit called')
            result = super().need_exit()
            print('need exit: %s' % result)
            return result

    unixsocketpath = tmp_path / 'server.unix'
    result = MyREPLServer(path=unixsocketpath, idle_timeout=1)
    yield result
    result.exit()


class REPLClient(object):
    """test impl of syncronous REPL socket client"""
    def __init__(self, path):
        sk = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sk.connect(str(path))
        sk.settimeout(2)
        self.sk = sk

    def send(self, *args):
        sk = self.sk
        querybytes = shlex.join(args).encode('ascii') + b'\r\n'
        print('querybytes: %r' % querybytes)
        sk.send(querybytes)

    def recv(self):
        sk = self.sk
        buffer = bytearray()
        while not buffer.endswith(b'\r\n\r\n'):
            buffer.extend(sk.recv(1024))

        result = buffer.decode('ascii').splitlines(keepends=True)
        print('result: %r' % result)
        assert result[-1] == '\r\n'
        result = result[:-1]
        assert all(line.endswith('\r\n') for line in result)
        return [line[:-2] for line in result]

    def query(self, *args):
        self.send(*args)
        return self.recv()

    def close(self):
        self.sk.close()


@pytest.fixture
def replclient(hq, hqthread, replserver):
    hq.add_task(replserver)
    hqthread.start()
    for i in range(10):
        if not replserver.path.exists():
            time.sleep(0.1)
        else:
            break

    time.sleep(0.1)
    with contextlib.closing(REPLClient(replserver.path)) as client:
        yield client


def test_ok(replclient):
    result = replclient.query('ping')
    assert result == ['OK', 'pong']
    result = replclient.query('nping', '10')
    assert result == ['OK'] + ['pong'] * 10
    result = replclient.query('nping', '100')
    assert result == ['OK'] + ['pong'] * 100
    result = replclient.query('nping', '1000')
    assert result == ['OK'] + ['pong'] * 1000


def test_idle(replclient):
    assert replclient.query('ping') == ['OK', 'pong']
    time.sleep(1.5) #  wait until idles
    with pytest.raises(BrokenPipeError):
        replclient.send('ping')
