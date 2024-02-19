from inspect import getsource
import sys
import zipfile
import tarfile
import pytest
from hashlib import md5
from pathlib import Path
from anykap import *

def md5hash(x):
    return md5(x).hexdigest()

@pytest.fixture
def data(tmp_path):
    p = tmp_path / 'datadir'
    p.mkdir()
    source = getsource(sys.modules[__name__])
    datalist = [
        (Path('source'), source),
        (Path('x10') / 'source', source * 10),
        (Path('empty'), '')
    ]
    for itempath, data in datalist:
        mypath = (p / itempath)
        mypath.parent.mkdir(exist_ok=True)
        mypath.write_text(data)
    return p, [(item, md5hash(data.encode())) for item, data in datalist]

def test_zip_archiver(data, tmp_path):
    datadir, datalist = data
    outpath  = tmp_path / 'outdir'
    outpath.mkdir()
    result = archive_zip(datadir, outpath)
    result = Path(result)
    assert result.parent == outpath
    assert result.suffix == '.zip'
    with zipfile.ZipFile(result) as zf:
        p = zipfile.Path(zf)
        for itempath, hv in datalist:
            assert md5hash((p / 'datadir' / itempath).read_bytes()) == hv
    # assert False

@pytest.mark.parametrize(
        ['compressor', 'suffix'],
        [(None,     '.tar'),
         ('gz',     '.tar.gz'),
         ('xz',     '.tar.xz'),
         ('bz2',    '.tar.bz2')])
def test_tar_archiver(data, tmp_path, compressor, suffix):
    datadir, datalist = data
    outpath  = tmp_path / 'outdir'
    outpath.mkdir()
    result = archive_tar(datadir, outpath, compressor=compressor)
    assert result.endswith(suffix)
    with tarfile.open(result) as tf:
        for itempath, hv in datalist:
            assert md5hash(tf.extractfile('datadir/' + str(itempath)).read()) == hv



