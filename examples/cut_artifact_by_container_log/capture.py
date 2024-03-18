# ruff: noqa: F403, F405
from anykap import *

# from anykap.azure import AzureBlobUploader
from datetime import timedelta
from functools import partial

hq = HQ()
e = SugarStub()
# used for communication
hq.add_task(HQREPLServer())
discovery = hq.add_task(
    PodContainerDiscovery(
        pod_query={
            "labels": ["application=contoso"],
            "name": "contoso",  # crictl pods --name supports regular expression
        },
        container_query={},  # maybe we want to find a specific container
        cadence=20,  # we do a scan every 20secs
        # inspect_pod=True,  # if we want crictl inspectp output
        # inspect_container=True,  # if we want crictl inspect output
    )
)
fission_rule = hq.add_rule(
    FissionRule(
        discovery[
            e.topic == "new",
            e.objtype == "container",
            {"env": {"container_id": e.object.id}},
        ],
        hq,
        name="errorgrep",
        task_factory=partial(
            ShellTask,
            keep_artifact=False,
            stdout_mode="notify",
            # Optional Python regular expression pattern
            stdout_filter="a very curous error: (?P<detail>.*)",
            script=r'crictl logs -f --tail 0 -t "$container_id" ',
        ),
    )
)

delay_rule = hq.add_rule(
    DelayRule(
        fission_rule[e.kind == "shell", e.topic == "line"],
        hq,
        "errorgrep",
        delay=10,  # we delay for 10 seconds to catch more of the same messages
        # we throttle for 3 more minutes to avoid frequent bumps
        throttle=timedelta(minutes=3),
    )
)


def tcpdump_factory(name=None, context=None, **ignored):
    task = ShellTask(
        name=name,
        context=context,
        # chmod needed since tcpdump changes owner.
        script=r'chmod 777 .; tcpdump -iany -C10 -W6 -w "out.pcap" port 53',
    )
    # this is the current recommended way to update receptor if receptors needs
    # to be changed on task creation
    task.exit_at(
        delay_rule.getfilter() | discovery[e.objtype == "pod", e.topic == "last_gone"]
    )
    return task


hq.add_rule(
    FissionRule(
        (
            delay_rule.getfilter()
            | discovery[e.objtype == "pod", e.topic == "first_seen"]
        ),
        hq,
        name="tcpdump",
        task_factory=tcpdump_factory,
    )
)


if __name__ == "__main__":
    main(hq)
