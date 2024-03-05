
from anykap import *
import pytest

@pytest.mark.parametrize('args,event,verdict,mutated',[
    ((lambda x: False,), 'any', False, None),
    ((lambda x: (True, x),), 'any', True, (True, 'any')),
    ((lambda x: {'foo': 'bar'}, lambda x: x['foo']), 'any', True, 'bar'),
    (({'bar': SugarStub().foo},), {'foo': 'yes'}, True, {'bar': 'yes'}),
])
def test_build_filter(args, event, verdict, mutated):
    filter = build_filter(*args)
    got_verdict, got_mutated = filter(event)
    assert got_verdict == verdict
    if verdict:
        assert got_mutated == mutated


def test_alternative_filters():
    f = build_filter(lambda x: x['foo']) | (lambda x: x['bar'])
    assert f({'foo': 'yes'}) == (True, 'yes')
    with pytest.raises(KeyError):
        f({'bar': 'yes'})
    f = build_filter(lambda x: x.get('foo')) | (lambda x: x.get('bar'))
    assert f({'foo': 'yes'}) == (True, 'yes')
    assert f({'bar': 'bar'}) == (True, 'bar')
