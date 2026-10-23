"""Failure drills: break the running pipeline on purpose, then prove that no
event was lost and none was written twice.

`make drill NAME=producer-crash` (or job-crash, storage-outage,
broker-outage, poison-message). Each drill notes where every topic partition
ends, breaks something, lets the pipeline recover, notes the ends again, and
then audits: every message that went through Redpanda in between must be
in the lake exactly once (or, if it isn't a WikiChange at all, in the
rejected table), and every minute it touched must be counted right. The
result goes to docs/drills/.

Runs on this machine against the running stack (`make up produce stream`),
with the development environment: confluent-kafka to read the topic,
PyIceberg to read the lake, `docker compose` to break things.
"""
