# Runbook

What each failure looks like, what the pipeline does about it on its own,
and what (if anything) to do. The failures with a drill have been broken on
purpose and audited: run `make drill NAME=<drill>` to see it happen, and the
reports are in [docs/drills](drills/).

All services run with `restart: unless-stopped`: a container that exits on
its own is started again by Docker, over and over, until it stays up.

## The producer died

- **You see:** in `make logs`, the producer starting again. The dashboard
  barely notices.
- **On its own:** Docker restarts it. It resumes from the last position it
  saved, which is at most 5 seconds old, and sends those seconds again. The
  job removes the copies by event id.
- **You do:** nothing. If it keeps restarting, read its last lines: a 4xx
  from Wikimedia stops it on purpose (check `WIKISTREAM_CONTACT`), a
  `DeliveryError` means the broker is the problem (below).
- **Drill:** `producer-crash`.

## The Spark job died

- **You see:** in `make stream-logs`, `query.failed` and a new start. After
  5 minutes without a write, the dashboard says *Stalled*.
- **On its own:** Docker restarts it. Spark replays the batch that didn't
  finish from its checkpoint. Both writes are idempotent (an anti-join on the
  event id, then minutes counted again from the rows), so the replay changes
  nothing that was already written.
- **You do:** nothing, unless it keeps failing: then the first `ERROR` or
  `query.failed` line says why.
- **Drill:** `job-crash` (killed in the middle of a batch, three times).

## Storage is down (SeaweedFS)

- **You see:** the dashboard says it can't read the lake; the job fails its
  batches and restarts again and again; `make peek` fails.
- **On its own:** the job keeps restarting until storage is back, then
  catches up. Nothing is lost meanwhile: Redpanda holds the topic for 48
  hours. Iceberg commits are atomic, so a batch cut off halfway leaves no
  half-written table.
- **You do:** `make up`. If SeaweedFS itself won't start, its log
  (`docker compose logs seaweedfs`) usually means a full disk.
- **Drill:** `storage-outage`.

## The broker is down (Redpanda)

- **You see:** the producer logs `DeliveryError` (nothing confirmed after
  30 s) and restarts; the job writes nothing; *Stalled* on the dashboard
  after 5 minutes.
- **On its own:** the producer exits without saving its position, so when
  the broker is back it asks Wikimedia for everything from its last saved
  position: the outage is fetched again, not skipped. The job doesn't fail:
  its reads wait for the broker, and it carries on from its checkpoint
  (in the drills, a one-minute outage never restarted it).
- **You do:** `make up`.
- **Drill:** `broker-outage`.

## Messages were set aside

- **You see:** `batch.rejected` warnings in `make stream-logs`, and a line
  in `make peek`: `wiki.rejected: N messages set aside ... (reason)`.
- **On its own:** the job keeps going. Each message is in `wiki.rejected`
  with its partition, offset, bytes and the reason.
- **You do:** a handful, from a test or a stray client: nothing. A steady
  stream of `does not decode as a WikiChange record` means the producer and
  the job disagree about the schema: deploy both from the same commit. The
  set-aside events are missing from `changes` until then. They are still in
  Redpanda for 48 hours, and `make lake-reset` followed by `make stream`
  rebuilds the lake from what Redpanda holds, which also drops anything
  older than that.
- **Drill:** `poison-message`.

## The job was stopped longer than 48 hours

- **You see:** the job fails at start with a data-loss error, and keeps
  failing after every restart.
- **What happened:** Redpanda deleted messages the job never read, because
  the producer kept running while the job was stopped.
- **You do:** decide to accept the gap: add
  `WIKISTREAM_FAIL_ON_DATA_LOSS=false` to `.env`, run `make stream`, and
  remove the line once it runs. Those events are not in the lake.
  To avoid it, stop both together: `make stop stream-stop`.

## The job is far behind

- **You see:** *Catching up* on the dashboard, batches of 200,000 events,
  `make peek` hours behind. While the producer replays a gap, `make peek`
  may also report a few straggling changes counted hours ahead of the
  rest: live changes from Wikimedia's quieter datacenter, mixed into the
  replay. They're real changes, counted like any other; how far the job
  has got is the newest *full* minute (100 changes or more), which is what
  `make peek` measures from.
- **On its own:** it works through the backlog, about 15 batches for a day
  of edits.
- **You do:** nothing. Don't run drills meanwhile: they wait up to 10
  minutes for a full minute within 5 minutes of the clock, saying how far
  behind the job is once a minute, then refuse.

## Maintenance failed

- **You see:** `maintenance.failed` in `make stream-logs`, with the Java
  cause.
- **On its own:** the other maintenance tasks still run, and the next hour
  tries again. A compaction that lost a commit race to the job is harmless.
- **You do:** if the same task fails every hour, `make maintain` runs it on
  its own, with the full error.

## The disk is filling up

- **You see:** `make peek`'s "on disk" growing well past the current data.
- **On its own:** hourly expiry keeps six hours of snapshots, which bounds
  the old versions copy-on-write leaves behind.
- **You do:** lower `WIKISTREAM_SNAPSHOT_RETENTION_HOURS` in `.env`, then
  `make maintain`. Orphan files from crashed batches aren't removed
  automatically; `make peek` counts them in "on disk".
