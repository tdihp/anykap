import sys
from inspect import getsource
import zipfile
import pytest
from anykap import *


# @pytest.mark.skip
async def test_artifactmanager_working(hq, hqtask, tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    result = asyncio.Future()
    manager = ArtifactManager(archiver=archive_zip, uploader=CopyUploader(dest))
    data = getsource(sys.modules[__name__])

    def myrule(event):
        if event.get("kind") == "artifact" and event.get("topic") == "uploaded":
            result.set_result(event["artifact"])

    class MyTask(Task):
        async def run_task(self, hq):
            artifact = self.new_artifact(hq, "foobar")
            with artifact:
                (artifact.path / "data").write_text(data)

    hq.add_rule(myrule)
    hq.add_task(manager)
    hq.add_task(MyTask("mytask"))
    await asyncio.wait_for(result, timeout=1)
    artifact = result.result()
    assert artifact == hq.artifacts[0] and len(hq.artifacts) == 1
    artifact.upload_state == "completed"
    artifact.upload_url == f"file://{dest}/{artifact.name}.zip"
    with zipfile.ZipFile(artifact.upload_url.removeprefix("file://")) as zf:
        p = zipfile.Path(zf)
        assert (p / artifact.name / "data").read_text() == data
