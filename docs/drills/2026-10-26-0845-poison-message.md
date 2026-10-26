# Drill: poison-message

2026-10-26 08:45 UTC · result: passed

Messages that aren't WikiChange records land in the topic. The job sets them aside in the rejected table and carries on.

## What happened

| Time (UTC) | |
|---|---|
| 08:45:17 | wrote 2 malformed messages to the topic (partition/offset 0/12549170, 0/12549171) |
| 08:46:54 | drill over: reading what went through Redpanda meanwhile |
| 08:46:54 | 4,506 messages; checking the lake until it has them all |
| 08:47:13 | audit passed |

## The audit

| | |
|---|---|
| Messages in Redpanda during the drill | 4,506 across 3 partitions |
| Distinct events | 4,504 |
| Sent more than once (at-least-once at work) | 0 |
| Not WikiChange records | 2 (0 not set aside) |
| Missing from the lake | 0 |
| In the lake more than once | 0 |
| Minutes counted right (wiki by wiki) | 131 of 131 |
| Every event in the lake, at the latest | 106 s after the broken part was back |
