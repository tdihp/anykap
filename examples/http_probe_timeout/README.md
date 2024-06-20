http probe failure capture
======

This is made to diagnose when pods in a given node fails with messages similar
to:

    prober.go:107] "Probe failed" probeType="" pod="" podUID= containerName="" probeResult=failure output="Get \"http://ip:port/\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)"

This is necessary due to https://github.com/kubernetes/kubernetes/issues/89898.

Things captured:

* rotating tcpdump between localhost to pod cidr, configured in env variable
  ANYKAP_PODCIDR, if not null
* conntrack -L in root namespace
* perf-record + perf-script output potentially for flamegraph analysis for 1m

To use it (under anykap code root):

``` shell
mkdir http_probe_timeout
kubectl anykap -k http_probe_timeout init httpprobetimeout
cp examples/http_probe_timeout/* http_probe_timeout
cd http_probe_timeout
kubectl apply -k .
kubectl anykap info
# after repro
kubectl anykap a --copy
```

[repro.yaml](./repro.yaml) is a pod which always fails http health probe with
with a timeout, as a testing environment.
