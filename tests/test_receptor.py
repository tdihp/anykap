from anykap import task_receptor, Task


def test_receptor():
    class MyTask(Task):
        descriptors = set()

        @task_receptor
        def foo(self, event):
            """a simple function"""
            print(f"foo got event {event}")
            self.data = event

    # with pytest.raises(AttributeError):
    t = MyTask()
    t.foo("data")
    assert t.data == "data"
    # print(f't.foo {t.foo!r}')
    t.foo_at(lambda x: x.get("foo") == "bar" and x)
    assert len(t.rules) == 1
    t.rules[0]({"foo": "bar", "a": "b"})
    assert t.data == {"foo": "bar", "a": "b"}
