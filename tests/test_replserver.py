import time
import shlex
import socket
import pytest
import contextlib
import json
from queue import SimpleQueue
from anykap import REPLServer, HQREPLServer


@pytest.fixture
def replserver(tmp_path, event_loop):
    class MyREPLServer(REPLServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.commands = {
                "ping": self.cmd_ping,
                "nping": self.cmd_nping,
            }

        def call_cmd(self, cmd, *args):
            """basically, all parsed args are sent here, expects either return"""
            cmdfunc = self.commands[cmd]  # fine to raise KeyError
            return cmdfunc(*args)

        def cmd_ping(self):
            print("got ping!")
            return ["pong"]

        def cmd_nping(self, num):
            return ["pong"] * int(num)

        def need_exit(self):
            """should be tested in run"""
            print("need_exit called")
            result = super().need_exit()
            print("need exit: %s" % result)
            return result

    unixsocketpath = tmp_path / "server.unix"
    result = MyREPLServer(path=unixsocketpath, idle_timeout=1)
    return result
    # event_loop.call_soon_threadsafe(result.exit)


@pytest.fixture
def hqreplserver(tmp_path):
    unixsocketpath = tmp_path / "server.unix"
    result = HQREPLServer(path=unixsocketpath, idle_timeout=1)
    return result
    # event_loop.call_soon_threadsafe(result.exit)


class REPLClient(object):
    """test impl of syncronous REPL socket client"""

    def __init__(self, path):
        for i in range(10):  # we wait patiently for the socket to appear
            if not path.exists():
                time.sleep(0.1)
            else:
                break

        time.sleep(0.1)  # we wait more patiently for server to start serving
        sk = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sk.setblocking(True)
        sk.settimeout(2)  # no idea why this doesn't work
        # sk.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, struct.pack('ll', 2, 0))
        sk.connect(str(path))
        self.sk = sk

    def send(self, *args):
        sk = self.sk
        # print('sk timeout:%s' % sk.gettimeout())
        # print('sk.SO_RCVTIMEO: %s' % sk.getsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO))
        querybytes = shlex.join(args).encode("ascii") + b"\r\n"
        print("querybytes: %r" % querybytes)
        sk.send(querybytes)

    def recv(self):
        sk = self.sk
        buffer = bytearray()
        while not buffer.endswith(b"\r\n\r\n"):
            # print(f"buffer: {buffer}")
            data = sk.recv(1024)
            assert data
            buffer.extend(data)

        result = buffer.decode("ascii").splitlines(keepends=True)
        print("result: %r" % result)
        assert result[-1] == "\r\n"
        result = result[:-1]
        assert all(line.endswith("\r\n") for line in result)
        return [line[:-2] for line in result]

    def query(self, *args):
        self.send(*args)
        return self.recv()

    def close(self):
        self.sk.close()


def test_ok(
    hq,
    hqthread,
    replserver,
):
    hq.add_task(replserver)
    hqthread.start()
    with contextlib.closing(REPLClient(replserver.path)) as replclient:
        result = replclient.query("ping")
        assert result == ["OK", "pong"]
        result = replclient.query("nping", "10")
        assert result == ["OK"] + ["pong"] * 10
        result = replclient.query("nping", "100")
        assert result == ["OK"] + ["pong"] * 100
        result = replclient.query("nping", "1000")
        assert result == ["OK"] + ["pong"] * 1000


def test_idle(hq, hqthread, replserver):
    hq.add_task(replserver)
    hqthread.start()
    with contextlib.closing(REPLClient(replserver.path)) as replclient:
        assert replclient.query("ping") == ["OK", "pong"]
        time.sleep(1.5)  #  wait until idles
        with pytest.raises(BrokenPipeError):
            replclient.send("ping")


def test_tasks(hq, hqthread, hqreplserver):
    hq.add_task(hqreplserver)
    hqthread.start()
    with contextlib.closing(REPLClient(hqreplserver.path)) as replclient:
        result = replclient.query("tasks")
        assert result[0] == "OK"
        assert len(result) == 3

        # we no longer test exact content for the text output
        result = replclient.query("-o", "json", "t", "-a")
        assert result[0] == "OK"


def test_send(hq, hqthread, hqreplserver):
    hq.add_task(hqreplserver)
    q = SimpleQueue()

    def eventrule(event):
        if event.get("kind") == "test":
            q.put_nowait(event)

    hq.add_rule(eventrule)
    hqthread.start()
    with contextlib.closing(REPLClient(hqreplserver.path)) as replclient:
        assert replclient.query("send", '{"kind": "test", "data": "foobar"}') == ["OK"]

    result = q.get(timeout=3)
    assert result["data"] == "foobar"


def test_artifacts(hq, hqthread, hqreplserver):
    hq.add_task(hqreplserver)
    # a new artifact
    a1 = hq.new_artifact("foobar1")
    # a completed artifact
    a2 = hq.new_artifact("foobar2")
    a2.start()
    a2.complete()
    # a uploaded artifact
    a3 = hq.new_artifact("foobar3")
    a3.start()
    a3.complete()
    a3.upload_complete("testlocation://foobar3")
    hqthread.start()
    with contextlib.closing(REPLClient(hqreplserver.path)) as replclient:
        result = replclient.query("artifacts")
        print(result)
        assert result[0] == "OK"
        assert len(result) == 4
        result = replclient.query("-ojson", "artifacts")
        assert result[0] == "OK"
        assert set(a["name"] for a in json.loads(result[1])["items"]) == {
            a2.name,
            a3.name,
        }
        result = replclient.query("-ojson", "artifacts", "-a")
        assert set(a["name"] for a in json.loads(result[1])["items"]) == {
            a1.name,
            a2.name,
            a3.name,
        }
        result = replclient.query(
            "-ojson", "artifacts", "--mark-uploaded", "-r", "foobar2"
        )
        set(a["name"] for a in json.loads(result[1])["items"]) == {a2.name}
        assert a2.upload_state == "completed"
        assert a2.upload_url == "<manual>"
