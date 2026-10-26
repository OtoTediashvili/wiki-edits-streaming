# Drill: producer-crash

2026-10-26 08:00 UTC · result: passed

The producer dies between two checkpoints and resends what it sent since the last one. Expect duplicates in the topic, none in the lake.

## What happened

| Time (UTC) | |
|---|---|
| 08:00:05 | kill -9 the producer: no flush, no saved position |
| 08:00:22 | producer started again; it resumes from its last saved position |
| 08:01:26 | drill over: reading what went through Redpanda meanwhile |
| 08:01:26 | 3,162 messages; checking the lake until it has them all |
| 08:02:17 | audit passed |

## The audit

| | |
|---|---|
| Messages in Redpanda during the drill | 3,162 across 3 partitions |
| Distinct events | 3,139 |
| Sent more than once (at-least-once at work) | 23 |
| Not WikiChange records | 0 (0 not set aside) |
| Missing from the lake | 0 |
| In the lake more than once | 0 |
| Minutes counted right (wiki by wiki) | 219 of 219 |
| Every event in the lake, at the latest | 106 s after the broken part was back |
