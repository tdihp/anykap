# ruff: noqa: F403, F405
from anykap import *
from datetime import timedelta

hq = HQ()
hq.add_task(HQREPLServer())
e = SugarStub()


logsniff = hq.add_task(
    ShellTask(
        name="logsniff",
        script=r'journalctl -u kubelet -S now -g "Probe failed.*Client.Timeout exceeded while awaiting headers" -f',
        stdout_mode="notify",
        stderr_mode="null",
    )
)


throttle_rule = hq.add_rule(
    DelayRule(
        logsniff[e.kind == "shell", e.topic == "line"],
        hq,
        "errorgrep",
        # We only take 1 capture every 10 minutes
        throttle=timedelta(minutes=10),
    )
)


def tcpdump_factory(name=None, context=None, **ignored):
    task = ShellTask(
        name=name,
        context=context,
        # take 10MB x 10 rotation should be sufficient for truncated files
        # for a few minutes, just to make sure the probe pacets get delivered
        script=r'chmod 777 .; tcpdump -iany -s 100 -C10 -W6 --dont-verify-checksums -w "out.pcap" tcp',
    )
    # this is the current recommended way to update receptor if receptors needs
    # to be changed on task creation
    task.exit_at(throttle_rule.getfilter())
    return task


hq.add_rule(
    FissionRule(
        throttle_rule[{}],
        hq,
        name="tcpdump",
        task_factory=tcpdump_factory,
    )
)

# we start tcpdump immediately
hq.add_task(tcpdump_factory(name="tcpdump-onstart"))


def infodump_factory(name=None, context=None, **ignored):
    task = ShellTask(
        name=name,
        context=context,
        script=r"""
            conntrack -L >conntrack.log
            perf record -F99 -ag -- sleep 10
            perf script >out.perf
        """,
    )
    return task


hq.add_rule(
    FissionRule(
        throttle_rule[{}],
        hq,
        name="infodump",
        task_factory=infodump_factory,
    )
)


if __name__ == "__main__":
    main(hq)
