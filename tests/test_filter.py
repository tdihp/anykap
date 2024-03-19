from anykap import SugarStub, build_filter, Filter, AlternativeFilter, FilterChain
import pytest


@pytest.mark.parametrize(
    "args,event,verdict,mutated",
    [
        ((lambda x: False,), "any", False, None),
        ((lambda x: (True, x),), "any", True, (True, "any")),
        ((lambda x: {"foo": "bar"}, lambda x: x["foo"]), "any", True, "bar"),
        (({"bar": SugarStub().foo},), {"foo": "yes"}, True, {"bar": "yes"}),
    ],
)
def test_build_filter(args, event, verdict, mutated):
    filter = build_filter(*args)
    got_verdict, got_mutated = filter(event)
    assert got_verdict == verdict
    if verdict:
        assert got_mutated == mutated


def test_alternative_filters():
    f = build_filter(lambda x: x["foo"]) | (lambda x: x["bar"])
    assert f({"foo": "yes"}) == (True, "yes")
    with pytest.raises(KeyError):
        f({"bar": "yes"})
    f = build_filter(lambda x: x.get("foo")) | (lambda x: x.get("bar"))
    assert f({"foo": "yes"}) == (True, "yes")
    assert f({"bar": "bar"}) == (True, "bar")


def test_repr():
    class MyCallable:
        def __init__(self, v):
            self.v = v

        def __call__(self, event):
            return True

        def __repr__(self):
            return f"MyCallable({self.v})"

    class MyFilter(Filter):
        def mutator(self):
            return "yes"

    assert (
        repr(MyFilter(condition=MyCallable(1))) == "MyFilter(condition=MyCallable(1))"
    )
    assert (
        repr(AlternativeFilter(Filter(MyCallable(1)), Filter(MyCallable(2))))
        == "AlternativeFilter(Filter(condition=MyCallable(1)), Filter(condition=MyCallable(2)))"
    )
    assert (
        repr(AlternativeFilter(Filter(MyCallable(1)), Filter(MyCallable(2))))
        == "AlternativeFilter(Filter(condition=MyCallable(1)), Filter(condition=MyCallable(2)))"
    )
    assert (
        repr(FilterChain(Filter(MyCallable(1)), Filter(MyCallable(2))))
        == "FilterChain(Filter(condition=MyCallable(1)), Filter(condition=MyCallable(2)))"
    )
