Anykap: Rule Based Observability in Kubernetes Nodes
====================================================

Feature
-------

* A Daemonset based troubleshooting tool. 
* Event based scripting system for defining your own capture flow.
* Pod/Container discovery with crictl.
* Artifact management.
* Relies on tools that you already know, not re-inventing wheels.
  Need packet capture? Run tcpdump;
  Need check io performance? Run your favourite performance tool.
* No special container image required. General purpose image like alpine works
  out-of-box.

Installation
------------

Currently anykap is in pre-Alpha. To install the dev version for testing with a 
recent Python3:

``` shell
python3 -m venv .venv
. .venv/bin/activate
pip install -U git+https://github.com/tdihp/anykap.git@main
```


Usage
-----

See [examples](./examples) for prepared capture examples.

