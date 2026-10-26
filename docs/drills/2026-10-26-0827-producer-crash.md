# Drill: producer-crash

2026-10-26 08:27 UTC · result: passed

The producer dies between two checkpoints and resends what it sent since the last one. Expect duplicates in the topic, none in the lake.

## What happened

| Time (UTC) | |
|---|---|
| 08:27:28 | kill -9 the producer: no flush, no saved position |
| 08:27:47 | producer started again; it resumes from its last saved position |
| 08:28:51 | drill over: reading what went through Redpanda meanwhile |
| 08:28:52 | 3,988 messages; checking the lake until it has them all |
| 08:29:07 | audit passed |

## The audit

| | |
|---|---|
| Messages in Redpanda during the drill | 3,988 across 3 partitions |
| Distinct events | 3,986 |
| Sent more than once (at-least-once at work) | 2 |
| Not WikiChange records | 0 (0 not set aside) |
| Missing from the lake | 0 |
| In the lake more than once | 0 |
| Minutes counted right (wiki by wiki) | 210 of 210 |
| Every event in the lake, at the latest | 76 s after the broken part was back |
