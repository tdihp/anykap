from collections import UserDict
from anykap import SugarStub, SugarLiteral


def test_sugarstub():
    e = SugarStub()
    assert e("oops") == "oops"
    assert e.abc("oops") is None
    assert e.abc({"abc": "yum"}) == "yum"
    assert e[0].foo([{"foo": "bar"}]) == "bar"

    class Foobar(UserDict):
        x = 100

        def __init__(self):
            super().__init__({"x": 10})

    assert e.x(Foobar()) == 100
    assert e["x"](Foobar()) == 10
    assert e["xyz"][1](Foobar()) is None
    assert repr(e) == "SugarStub()"
    assert repr(e["x"][1].abc) == "SugarStub()['x'][1].abc"


def test_sugarliteral():
    assert SugarLiteral([1, 2, 3])("anything") == [1, 2, 3]
    assert SugarLiteral({"foo": "bar", "nested": ["array"]})(["anything"]) == {
        "foo": "bar",
        "nested": ["array"],
    }


def test_sugarop():
    assert (
        repr(SugarLiteral("a") == SugarLiteral("b"))
        == "SugarOp(SugarLiteral('a') == SugarLiteral('b'))"
    )
    e = SugarStub()
    assert (e.abc == [1, 2, 3])({"abc": [1, 2, 3]}) is True
    assert (e.abc == [1, 2, 3])({"abc": [1, 2]}) is False
    assert ([1, 2, 3] == e.abc)({"abc": [1, 2, 3]}) is True
    assert ([1, 2, 3] == e.abc)({"abc": [1, 2, 3, 4]}) is False
    assert (e.abc == e.foo)({"abc": "data", "foo": "data"}) is True
    assert (e.abc == e.foo)({"abc": "data"}) is False
    assert (e.abc > e.foobar)({"abc": 7, "foobar": 10}) is False
    assert (e.abc <= e.foobar)({"abc": 7, "foobar": 10}) is True
