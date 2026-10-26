# Drill: storage-outage

2026-10-26 08:09 UTC · result: passed

SeaweedFS (catalog and S3) goes away. The job's batches fail, it exits, Docker restarts it until storage is back, and it catches up.

## What happened

| Time (UTC) | |
|---|---|
| 08:09:23 | stop SeaweedFS for 120 s: no catalog, no S3 |
| 08:11:42 | SeaweedFS is back; Docker restarted the Spark job 7 times meanwhile |
| 08:13:20 | drill over: reading what went through Redpanda meanwhile |
| 08:13:20 | 9,501 messages; checking the lake until it has them all |
| 08:14:10 | audit passed |

## The audit

| | |
|---|---|
| Messages in Redpanda during the drill | 9,501 across 3 partitions |
| Distinct events | 9,501 |
| Sent more than once (at-least-once at work) | 0 |
| Not WikiChange records | 0 (0 not set aside) |
| Missing from the lake | 0 |
| In the lake more than once | 0 |
| Minutes counted right (wiki by wiki) | 407 of 407 |
| Every event in the lake, at the latest | 137 s after the broken part was back |
