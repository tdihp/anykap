import sys
from subprocess import run, PIPE
from anykap import envimporter
import inspect
from textwrap import dedent
import pytest

envimporter_src = inspect.getsource(envimporter)

def test_envimporter_direct():
    samplemodule = dedent("""\
        print("hello world")
        important_value=42
        """)
    script = dedent("""\
        install_envimporter()
        from foobar import important_value
        assert important_value == 42
        """)
    full_script = '\n'.join([envimporter_src, script])
    result = run([sys.executable, '-c', full_script],
                 env={'PYMODULE__FOOBAR': samplemodule},
                 stdout=PIPE, text=True)
    assert result.stdout.strip() == 'hello world'
    assert result.returncode == 0
    # assert False


def test_envimporter_prefix():
    samplemodule = dedent("""\
        print("hello world")
        important_value=42
        """)
    script = dedent("""\
        install_envimporter(valid_prefix="foobar")
        from foobar import important_value
        assert important_value == 42
        """)
    full_script = '\n'.join([envimporter_src, script])
    result = run([sys.executable, '-c', full_script],
                 env={'PYMODULE__FOOBAR': samplemodule},
                 stdout=PIPE, text=True)
    assert result.returncode == 0
    script = dedent("""\
        install_envimporter(valid_prefix="nope")
        from foobar import important_value
        assert important_value == 42
        """)
    full_script = '\n'.join([envimporter_src, script])
    result = run([sys.executable, '-c', full_script],
                 env={'PYMODULE__FOOBAR': samplemodule},
                 stdout=PIPE, stderr=PIPE, text=True)
    assert result.returncode == 1
    assert 'ModuleNotFoundError' in result.stderr


def test_submodule():
    samplemodule = dedent("""\
        print("hello world")
        important_value=42
        """)
    script = dedent("""\
        install_envimporter(valid_prefix="foo")
        from foo.bar import important_value
        assert important_value == 42
        """)
    full_script = '\n'.join([envimporter_src, script])
    result = run([sys.executable, '-c', full_script],
                 env={'PYMODULE__FOO__BAR': samplemodule, 'PYMODULE__FOO': ''},
                 stdout=PIPE, stderr=PIPE, text=True
                 )
    assert result.stdout.strip() == 'hello world'
    assert result.returncode == 0