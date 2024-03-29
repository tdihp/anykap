import asyncio
from anykap import Task, Rule, Filter


# failed task run shouldn't fail hq, visually inspect if a message is logged
# for exception not catched
async def test_exception(hq, hqtask):
    called = asyncio.Future()

    class MyTask(Task):
        async def run_task(self, hq):
            # nonlocal called
            # called.set_result(True)
            raise Exception("oops")

    task = MyTask()
    hq.add_task(task)
    task._task.add_done_callback(lambda task: called.set_result(True))
    await asyncio.wait_for(called, timeout=1)
    assert len(task.warnings) == 1 and "oops" in task.warnings[0]


async def test_task_exit(hq, hqtask):
    class MyTask(Task):
        async def run_task(self, hq):
            await asyncio.sleep(1000)

    task = MyTask()
    # task.exit_at(lambda event: event.get("foo") == "bar"))  # equivalent
    task.rules.append(Rule(Filter(lambda event: event.get("foo") == "bar"), task.exit))
    hq.add_task(task)
    await asyncio.sleep(0)
    hq.send_event({"foo": "bar"})
    await asyncio.wait_for(task.join(), timeout=1)
