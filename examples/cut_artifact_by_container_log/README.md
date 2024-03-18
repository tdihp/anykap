A very typical usecase of anykap is to conditionally capture packets only when
some issue happens.

This example demonstrates such a capturing setting.

capture.py configures tasks such that:

* Discovers containers of "contoso" pod
  (see [curiouspod.yaml](./curiouspod.yaml)).
* When contoso pods are first seen, start tcpdump, and stop when last of contoso
  pods are gone.
* Grep logs of the contoso pod, and detect any error lines matches a pattern.
* When the match found, triggers a delayed and throttled event which stops the
  current tcpdump capture, and starts a new one.
