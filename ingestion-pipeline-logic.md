# Ingestion Pipeline — Architecture & Logic Reference

Author: Bashkar Sampath
Date: June 2026

A reusable design template for event-triggered, sequentially-processed,
single-worker ingestion pipelines on a multi-pod Kubernetes cluster.

---

## 1. Pattern Overview

An external event registers a job in a PostgreSQL job table. A scheduled
cron on every pod races to acquire a singleton pipeline lock. The winner
becomes the exclusive worker and processes jobs sequentially. When the
queue is empty, the lock is released and any pod may acquire it on the
next cycle.

**What this pattern solves:**
- Exactly one active worker at any time across N pods
- Automatic retry with cooldown on transient failures
- Poison pill isolation with admin-controlled recovery
- Stuck job detection via heartbeat, not lock coordination
- Priority queue support
- Full append-only audit trail per job
- No external systems — native PostgreSQL only

**What this pattern does not solve:**
- Parallel high-throughput processing (deliberately single-worker)
- Sub-second scheduling latency

---

## 2. Design Principles

**Pipeline-level singleton lock — not advisory lock.**
A single row in `ingestion_pipeline_lock` answers "who may schedule?"
Acquired via a native `UPDATE WHERE status='IDLE'` — a database-level CAS.
No `@Version`, no entity lifecycle, no Hibernate involvement on the lock row.
Three `@Modifying @Query` methods own it entirely: `tryAcquire`, `renewHeartbeat`,
`release`. No `PipelineLock` entity class needed.

**No external coordinator.**
No Redis, ZooKeeper, ShedLock, or message broker. PostgreSQL is the
only coordinator. The lock table IS the distributed lock.

**Two heartbeats, two questions.**
`pipeline_lock.heartbeat_at` answers: is the pod that owns the pipeline
still alive? `ingestion_audit_log.heartbeat_at` answers: has this
specific job made progress recently? These are different concerns and
tracked separately.

**Lock heartbeat is independent of job processing.**
A dedicated `@Scheduled` task updates the lock's `heartbeat_at` every
30 seconds. It runs regardless of what the job is doing — downloading,
parsing, batch-inserting. Even a 3-hour job keeps the lock alive.

**`volatile boolean ownsPipelineLock` is an advisory local cache, not authoritative ownership.**
Ownership truth lives in the database — specifically in `owner_id`. The
boolean is a fast-path optimization: the heartbeat task reads it before
hitting the database, avoiding a DB call on every 30-second tick when this
pod does not own the lock. If the boolean is stale (e.g. after a GC pause
where the janitor reset the lock and another pod acquired it), the
`WHERE owner_id=:podId` clause on the heartbeat and release UPDATEs
absorbs the inconsistency silently — 0 rows affected. The boolean being
wrong never causes a correctness problem; it only causes an extra DB call
at worst.

**`owner_id` in the lock table for correctness guarantee.**
The lock row stores the Kubernetes pod name (`HOSTNAME` env var) of the
current owner. The heartbeat UPDATE includes `AND owner_id = :podId`.
If a GC pause causes the janitor to reset the lock and another pod
acquires it, the paused pod's eventual heartbeat fires against the wrong
`owner_id` — 0 rows affected, silently ignored. Split-brain eliminated
as a failure class. Cost: one VARCHAR column and a WHERE clause on two
statements.

**`ownsPipelineLock = false` before `release()`, not after.**
The heartbeat task checks the boolean before hitting the database.
Setting it false first stops the heartbeat task from racing with the
release UPDATE. If the order were reversed, the heartbeat could fire
between release and the boolean being cleared, refreshing a lock
this pod no longer owns.

**`stream_position` enforces strict sequential ordering.**
Each job is assigned a monotonically increasing position from a database
sequence on registration. The scheduler always processes the lowest
non-terminal stream_position first. This is not a priority queue — it is
a stream. Position 3 never processes before position 2, even if position 2
is in a FAILURE_RETRYABLE cooldown. The scheduler simply waits.

This matters when files are incremental updates to shared records. Applying
C before B' could produce incorrect state. `stream_position` makes stream
order an invariant, not a convention.

**`priority` is intentionally absent.**
A priority field would conflict with stream ordering. There is no legitimate
urgency use case that overrides sequence in a stream-based pipeline. Instead,
the admin reset endpoint accepts corrected source metadata — B is replaced
by B' at the same `stream_position`. Sequence repair, not priority override.

**Status column drives all scheduling queries.**
`state_history` JSONB is append-only audit trail — never used in
scheduling logic.

**`state_history` as JSONB, not a separate table.**
Each job produces 8–20 state transitions. The JSONB column stays well
under the TOAST threshold. For this workload — single writer, sequential
pipeline, no analytics on state events, human readability matters —
JSONB is simpler. `appendState()` is a read-modify-write but safe because
the pipeline lock guarantees single-writer access at all times.

**Idempotent writes simplify recovery.**
The destination table uses `ON CONFLICT DO UPDATE`. Reprocessing a job
from scratch is always safe. Recovery is simple: reset the job, reprocess.

**stateHistory as JSONB, not a separate table.**
Each job produces 8–20 state transitions maximum. The JSONB column stays
well under the TOAST threshold. A separate table adds join complexity
without meaningful benefit at this volume.

---

## 3. Components

```
IngestionController      Receives the external event trigger. Calls
                         IngestionTx.register(). Returns 200 immediately.
                         No processing here.
                         ← DOMAIN SPECIFIC: event parsing, source extraction

IngestionScheduler       @Scheduled cron. Acquires pipeline lock. Drains
                         job queue sequentially. Releases lock.

LockHeartbeatTask        @Scheduled task (separate). If ownsPipelineLock=true,
                         updates lock.heartbeat_at every 30 seconds.

IngestionJanitor         @Scheduled cron (separate). Two responsibilities:
                         (1) Reset stale pipeline lock to IDLE.
                         (2) Mark stuck jobs as STUCK_RELEASED.

IngestionFlow            Pure orchestration. processJob(id).
                         claim → execute domain work → complete/fail.
                         ← DOMAIN SPECIFIC: what processing means

IngestionProcessor       Domain work only. Processes data per batch.
                         Calls tx.heartbeat(jobId) per batch.
                         ← DOMAIN SPECIFIC: entirely

IngestionTx              All @Transactional(REQUIRES_NEW) operations.
                         Exists solely to preserve Spring AOP proxy
                         boundaries. Not a domain boundary.

IngestionAuditLog        JPA entity for the job table.
PipelineLockRepository   Native @Modifying queries only. No entity class.
                         tryAcquire / renewHeartbeat / release / resetStaleLock.
AuditState               Enum of all job status values.
```

---

## 4. Tables

### 4.1 ingestion_pipeline_lock

```sql
CREATE TABLE ingestion_pipeline_lock (
    id           INT       PRIMARY KEY,
    status       VARCHAR   NOT NULL,       -- IDLE | ACTIVE
    owner_id     VARCHAR   NULL,           -- Kubernetes pod name (HOSTNAME env var)
    heartbeat_at TIMESTAMP NULL            -- updated every 30s by lock owner
);

-- Singleton row. Insert once. Never insert again.
INSERT INTO ingestion_pipeline_lock VALUES (1, 'IDLE', NULL, NULL);
```

**Column roles:**

| Column | Purpose |
|---|---|
| `status` | IDLE = no pod is working. ACTIVE = a pod owns the pipeline. CAS variable — acquire races on `WHERE status='IDLE'`. |
| `owner_id` | Kubernetes pod name of the current lock owner. Set on acquire, cleared on release and janitor reset. Scopes heartbeat and release UPDATEs to the actual owner. |
| `heartbeat_at` | Touched every 30s by the owning pod. Staleness = pod is dead. |

### 4.2 ingestion_audit_log

```sql
CREATE SEQUENCE ingestion_stream_pos_seq START 1 INCREMENT 1;

CREATE TABLE ingestion_audit_log (
    -- Identity
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    version              BIGINT      NOT NULL DEFAULT 0,

    -- Stream ordering
    stream_position      BIGINT      NOT NULL DEFAULT nextval('ingestion_stream_pos_seq'),

    -- Queue control
    status               VARCHAR(32) NOT NULL DEFAULT 'CREATED',
    retry_count          INT         NOT NULL DEFAULT 0,
    retry_after          TIMESTAMP   NULL,

    -- Timestamps
    request_received_at  TIMESTAMP   NOT NULL,
    status_updated_at    TIMESTAMP   NOT NULL,
    heartbeat_at         TIMESTAMP   NULL,

    -- Source metadata                        ← DOMAIN SPECIFIC
    bucket               VARCHAR     NOT NULL,
    object_name          VARCHAR     NOT NULL,

    -- Raw event payload
    request_header       JSONB,
    request_body         JSONB,

    -- Append-only audit trail
    state_history        JSONB       NOT NULL DEFAULT '[]',

    -- Denormalized result summary            ← DOMAIN SPECIFIC
    total_rows_read      INT,
    total_rows_inserted  INT,
    total_rows_orphaned  INT,
    total_rows_failed    INT,

    error_message        TEXT
);

CREATE INDEX idx_ial_status
    ON ingestion_audit_log (status);

CREATE INDEX idx_ial_stream
    ON ingestion_audit_log (stream_position ASC)
    WHERE status NOT IN ('COMPLETED', 'ADMIN_DISMISSED');
```

**Column roles:**

| Column | Purpose |
|---|---|
| `version` | @Version on job. Secondary race guard on claim. |
| `stream_position` | Immutable. Assigned from sequence on CREATED. Defines processing order. Never changes, even on admin reset. |
| `status` | Drives all scheduler queries. |
| `retry_count` | Increments on each failure. At MAX_RETRIES → FAILURE_POISON. |
| `retry_after` | Cooldown gate. NULL = eligible immediately. |
| `request_received_at` | Immutable. Set once on CREATED. |
| `status_updated_at` | Updated on every status transition. Not used for liveness. |
| `heartbeat_at` | Updated per batch commit during PROCESSING. Used by janitor for stuck detection. NULL outside of PROCESSING. |
| `state_history` | Append-only JSONB. Full audit trail. Never queried for scheduling. |

**Why two timestamps on the job?**

`status_updated_at` changes on every status transition and admin operation.
Using it for stuck detection would cause false negatives — any write to the
row would reset the liveness clock mid-PROCESSING.

`heartbeat_at` is only ever written by the batch heartbeat during PROCESSING.
No other operation touches it. The janitor can trust it exclusively.

---

## 5. State Machine

### 5.1 States

| State | Category | Description |
|---|---|---|
| `CREATED` | Active | Registered, awaiting worker pickup |
| `PROCESSING` | Active | Domain processor running, heartbeat active |
| `FAILURE_RETRYABLE` | Active | Failed, within retry budget, cooldown in progress |
| `FAILURE_POISON` | Terminal* | Retry budget exhausted. Incident raised. Halts all processing. |
| `STUCK_RELEASED` | Active | Janitor detected stale heartbeat, released for re-queue |
| `COMPLETED` | Terminal | Successfully processed |
| `ADMIN_DISMISSED` | Terminal | Admin dismissed without retry |

*FAILURE_POISON is terminal unless an admin explicitly resets it.

### 5.2 Transitions

```
CREATED           → PROCESSING         Scheduler claims and starts in one transaction
FAILURE_RETRYABLE → PROCESSING         Scheduler retries (retry_after elapsed)
STUCK_RELEASED    → PROCESSING         Scheduler claims and starts in one transaction

PROCESSING        → COMPLETED          Success
PROCESSING        → FAILURE_RETRYABLE  Failure, retry_count < MAX_RETRIES
                                       retry_after = NOW() + RETRY_COOLDOWN
PROCESSING        → FAILURE_POISON     Failure, retry_count = MAX_RETRIES
                                       callForIncident(exception)
PROCESSING        → STUCK_RELEASED     Janitor: heartbeat_at stale > PROCESSING threshold

FAILURE_POISON    → CREATED            Admin reset, retry_count=0, retry_after=NULL
FAILURE_POISON    → ADMIN_DISMISSED    Admin dismiss
```

### 5.3 Diagram

```
                    EVENT
                      │
                      ▼
                  REGISTER
                      │
                      ▼
                  CREATED ◄──────────────────── Admin Reset
                      │                               ▲
                      │ claimAndStartProcessing()      │
                      ▼                               │
    ┌─────────── PROCESSING ──────────────────► FAILURE_POISON ──► ADMIN_DISMISSED
    │                 │                               ▲
    │ Janitor         ├──► COMPLETED                  │
    │ (stale          │                       retry_count = MAX
    │ heartbeat)      └──► FAILURE_RETRYABLE          │
    │                       retry_count++ ────────────┘
    ▼                       retry_after set
STUCK_RELEASED              retry_count < MAX
    │
    └──► claimAndStartProcessing()
```

---

## 6. Scheduler Logic

`@Scheduled(fixedDelay = SCHEDULER_INTERVAL_MS)`

Use `fixedDelay`, not `fixedRate`. Countdown begins after execution
completes, preventing overlapping cycles on the same pod.

```
STEP 1 — ACQUIRE PIPELINE LOCK

  int rows = lockRepository.tryAcquire(podId);
    → UPDATE ingestion_pipeline_lock
      SET status='ACTIVE', owner_id=:podId, heartbeat_at=NOW()
      WHERE id=1 AND status='IDLE'

  rows = 0 → another pod won the race. Return.
  rows = 1 → this pod is the exclusive worker. ownsLock = true.

  No entity load. No @Version. WHERE status='IDLE' is the CAS.
  Two pods race: one UPDATE gets 1 row, one gets 0. Database resolves it.

STEP 2 — FAILURE_POISON CHECK

  SELECT EXISTS (
    SELECT 1 FROM ingestion_audit_log WHERE status = 'FAILURE_POISON'
  )

  true  → log clearly. Release lock. Return.
           Processing halted until admin resolves ALL FAILURE_POISON records.
  false → proceed.

STEP 3 — DRAIN QUEUE (while loop)

  a. Query next eligible job — strict sequential:

     WITH next_position AS (
         SELECT MIN(stream_position) AS pos
         FROM   ingestion_audit_log
         WHERE  status NOT IN ('COMPLETED', 'ADMIN_DISMISSED')
     )
     SELECT a.*
     FROM   ingestion_audit_log a
     JOIN   next_position n ON a.stream_position = n.pos
     WHERE  a.status IN ('CREATED', 'FAILURE_RETRYABLE', 'STUCK_RELEASED')
     AND    (a.retry_after IS NULL OR a.retry_after <= NOW())

     None found → break.

     Why this query:
       The CTE finds the lowest non-terminal stream_position.
       The outer query checks if that position is actually claimable.

       If position 2 is FAILURE_RETRYABLE with retry_after > NOW():
         CTE returns pos=2. Outer query finds it but retry_after gates it.
         Returns nothing. Scheduler waits. Position 3 does NOT skip ahead.

       If position 2 is FAILURE_POISON:
         Caught by Step 2 halt check before this query runs.

       If position 2 is LOCKED or PROCESSING:
         Pipeline lock check (Step 1) returns before this query runs.

       Stream order is an invariant. No job processes before its predecessor
       reaches a terminal state (COMPLETED or ADMIN_DISMISSED).

  b. Claim → PROCESSING in one transaction (claimAndStartProcessing)
     Set job.status = PROCESSING, job.heartbeat_at = NOW().
     Save → @Version check on the job row.
     OptimisticLockingFailureException → extremely unlikely under pipeline
     lock but handled: log, break.

     No LOCKED intermediate state. Claim and start are atomic.

  c. IngestionFlow.processJob(jobId)
     See: Claim Protocol, Heartbeat Protocol, Retry Protocol.

  d. Exception in processJob → job is already FAILURE_RETRYABLE or
     FAILURE_POISON. Log. Continue loop (next iteration picks up next job).

STEP 4 — RELEASE PIPELINE LOCK (finally block)

  lockRepository.release(podId);
    → UPDATE ingestion_pipeline_lock
      SET status='IDLE', owner_id=NULL, heartbeat_at=NULL
      WHERE id=1 AND owner_id=:podId

  ownsLock = false.
  No entity load. WHERE owner_id=:podId ensures a stale pod cannot
  accidentally release another pod's lock.
```

---

## 7. Pipeline Lock Lifecycle

### 7.1 Acquire

```java
int rows = lockRepository.tryAcquire(podId);
if (rows == 0) return; // lost the race
ownsPipelineLock = true;
```

`WHERE status='IDLE'` is a database-level CAS. Two pods submit
simultaneously: one gets 1 row affected, the other 0. No `@Version`,
no Hibernate entity lifecycle, no exception handling needed.

### 7.2 Lock Heartbeat

```java
@Scheduled(fixedDelay = 30_000)
void renewLockHeartbeat() {
    if (!ownsPipelineLock) return;
    lockRepository.renewHeartbeat(podId);
    // UPDATE ... WHERE id=1 AND owner_id=:podId
}
```

`WHERE owner_id=:podId` is the split-brain guard. If a GC pause caused
the janitor to reset the lock and Pod2 acquired it, Pod1's eventual
heartbeat fires against a mismatched `owner_id` — 0 rows, silent no-op.
Pod2's liveness is unaffected.

Independent of job processing. Fires every 30 seconds regardless of
whether the processor is mid-batch or idle between jobs.

### 7.3 Release

```java
// in finally block of scheduler
ownsPipelineLock = false;          // stop heartbeat task first
lockRepository.release(podId);
// UPDATE ... SET status='IDLE', owner_id=NULL, heartbeat_at=NULL WHERE owner_id=:podId
```

`ownsPipelineLock = false` before the release UPDATE.
The `WHERE owner_id=:podId` clause already makes the heartbeat race harmless —
a heartbeat that fires after release simply hits 0 rows. Setting the boolean
first is about avoiding that needless DB call during the release window,
not about correctness. Either order is correct; this order is cleaner.

### 7.4 Crash Recovery

Pod dies mid-processing. `heartbeat_at` stops updating.

```
Janitor detects:
  status = 'ACTIVE'
  AND heartbeat_at < NOW() - LOCK_STALE_THRESHOLD

Resets:
  UPDATE ingestion_pipeline_lock
  SET    status = 'IDLE', owner_id = NULL, heartbeat_at = NULL
  WHERE  status = 'ACTIVE'
  AND    heartbeat_at < NOW() - LOCK_STALE_THRESHOLD

Next pod acquires on its next cycle.
Stuck job: still in PROCESSING. Covered by job janitor sweep.
```

---

## 8. Janitor Logic

`@Scheduled(fixedDelay = JANITOR_INTERVAL_MS)` — independent of scheduler.

Two sweeps per cycle. No pipeline lock required. The heartbeats provide
all liveness information the janitor needs.

### 8.1 Pipeline Lock Sweep

```sql
UPDATE ingestion_pipeline_lock
SET    status = 'IDLE',
       owner_id = NULL,
       heartbeat_at = NULL
WHERE  status = 'ACTIVE'
AND    heartbeat_at < NOW() - LOCK_STALE_THRESHOLD
```

Resets a stale lock so another pod may acquire the pipeline.

### 8.2 Stuck PROCESSING Jobs

```sql
UPDATE ingestion_audit_log
SET    status = 'STUCK_RELEASED',
       status_updated_at = NOW(),
       heartbeat_at = NULL
WHERE  status = 'PROCESSING'
AND    heartbeat_at IS NOT NULL
AND    heartbeat_at < NOW() - PROCESSING_STUCK_THRESHOLD
```

An active pod touches `heartbeat_at` on every batch commit.
Stale `heartbeat_at` means the pod died mid-processing.
With LOCKED eliminated, there is no ephemeral intermediate state to sweep.
A pod dying after claim = job is already in PROCESSING with heartbeat_at
set. The single sweep handles it.

**Threshold is based on heartbeat interval, not file size:**

```
PROCESSING_STUCK_THRESHOLD = BATCH_HEARTBEAT_INTERVAL × SAFETY_MULTIPLIER

Example:
  Heartbeat fires per batch commit (~5–30s depending on batch size)
  PROCESSING_STUCK_THRESHOLD = 15 minutes

  A 3-hour file processes safely. Heartbeat fires every batch.
  The threshold is never exceeded while the pod is alive.
  File size is irrelevant.
```

---

## 9. Claim Protocol

```
Scheduler finds eligible job via Step 3 query.

Sets job.status = PROCESSING, job.heartbeat_at = NOW().
Saves → Hibernate issues:
  UPDATE ingestion_audit_log
  SET status='PROCESSING', heartbeat_at=NOW(), version=N+1, status_updated_at=NOW()
  WHERE id=? AND version=N

Claim and start are a single @Transactional(REQUIRES_NEW) operation.
No intermediate LOCKED state. The job is PROCESSING from the moment
it is claimed.

@Version on the job row: secondary guard against any concurrent claim.
Under the pipeline lock, only one pod runs the scheduler at a time, so
this should never fire. Handled defensively: log + break drain loop.
```

---

## 10. Retry Protocol

```
On PROCESSING failure:

  retry_count++

  retry_count < MAX_RETRIES:
    status      = FAILURE_RETRYABLE
    retry_after = NOW() + RETRY_COOLDOWN
    Append FAILURE_RETRYABLE to state_history with error context

  retry_count = MAX_RETRIES:
    status = FAILURE_POISON
    callForIncident(exception)
      Must include: job id, source metadata, error_message, retry_count,
      last state_history entry. Give on-call everything without a DB query.
    Append FAILURE_POISON to state_history
    scheduler halts at Step 2 until admin resolves
```

**Retry ordering — stream_position is the invariant:**

```
A FAILURE_RETRYABLE job retains its stream_position.
When retry_after elapses, the scheduler's next cycle finds it as the
minimum non-terminal position and processes it in sequence.
No head-of-line manipulation. No ordering tricks.
Position 2 retries before position 3 processes, always.
```

---

## 11. Job Heartbeat Protocol

```
Where:  IngestionTx.heartbeat(UUID jobId)

  @Transactional(REQUIRES_NEW)
  UPDATE ingestion_audit_log
  SET heartbeat_at = NOW()
  WHERE id = :jobId

When:   Called from IngestionProcessor on every batch commit.

  per batch:
    saveBatch(entities)       → domain data committed to destination table
    tx.heartbeat(jobId)       → heartbeat_at = NOW()

Why REQUIRES_NEW:
  The heartbeat must commit independently of the batch transaction.
  If the batch fails and rolls back, the heartbeat commit must survive
  so the janitor sees an accurate liveness timestamp.

Why every batch:
  Simple. One UPDATE by PK per batch. Overhead is negligible.
  No conditional logic, no counter tracking.

Dependency:
  IngestionProcessor.process() receives jobId as a parameter.
```

---

## 12. FAILURE_POISON Halt

```
Trigger:  retry_count reaches MAX_RETRIES on a PROCESSING failure.

Effect:   callForIncident(exception) fires synchronously.
          status = FAILURE_POISON.
          All scheduler cycles halt at Step 2 until resolved.

Scope:    Global. One FAILURE_POISON blocks all job processing.
          Intentional. A systemic failure (schema change, corrupt source
          format, infrastructure degradation) would fail all subsequent
          jobs anyway. Halt early, fix root cause, resume cleanly.

Multiple FAILURE_POISON records:
          ALL must be resolved before the scheduler resumes.
          Admin must explicitly handle each one — reset or dismiss.
```

---

## 13. Admin Operations

All admin operations validate the current status before executing.
All transitions append an entry to `state_history`.

### POST /ingestion/admin/{id}/reset

```
Valid from:  FAILURE_POISON only.
Body:        { "bucket": "...", "objectName": "..." }  ← optional

Effect:      status        = CREATED
             bucket        = new value if provided, else unchanged
             object_name   = new value if provided, else unchanged
             retry_count   = 0
             retry_after   = NULL
             stream_position = UNCHANGED  ← position in stream preserved
             status_updated_at = NOW()
             Appended to state_history: ADMIN_RESET + timestamp + operator note

Use when:    Root cause identified and fixed. Optionally replaces the
             source file (B → B') while preserving stream order.
             The job processes at its original position, not appended
             to the end of the queue.
```

### POST /ingestion/admin/{id}/dismiss

```
Valid from:  FAILURE_POISON only.
Effect:      status = ADMIN_DISMISSED
             status_updated_at = NOW()
             Appended to state_history: ADMIN_DISMISSED + timestamp + operator note
Use when:    Source is corrupt, malformed, or unrecoverable. No retry.
             Clears this record's contribution to the FAILURE_POISON halt.
             The next stream_position becomes the new minimum and processing resumes.
```

---

## 14. Configuration Reference

| Parameter | Description | Suggested Default |
|---|---|---|
| `SCHEDULER_INTERVAL_MS` | fixedDelay between scheduler cycles | `30_000` (30s) |
| `JANITOR_INTERVAL_MS` | fixedDelay between janitor sweeps | `60_000` (1 min) |
| `LOCK_HEARTBEAT_INTERVAL_MS` | fixedDelay of lock heartbeat task | `30_000` (30s) |
| `LOCK_STALE_THRESHOLD` | How long ACTIVE with no heartbeat before janitor resets lock | `5 minutes` |
| `PROCESSING_STUCK_THRESHOLD` | How long PROCESSING with no job heartbeat before janitor releases | `15 minutes` |
| `MAX_RETRIES` | Failures before FAILURE_POISON | `3` |
| `RETRY_COOLDOWN` | Duration added to retry_after on FAILURE_RETRYABLE | `5 minutes` |
| `BATCH_SIZE` | Rows per saveBatch() commit | `2500` |

**Threshold relationships:**

```
LOCK_STALE_THRESHOLD      >> LOCK_HEARTBEAT_INTERVAL
  Must give the heartbeat several missed cycles before declaring dead.
  30s interval → 5min threshold = 10 missed heartbeats before reset. Safe.

PROCESSING_STUCK_THRESHOLD >> per-batch commit time
  As long as pod is alive, heartbeat fires every batch.
  15min threshold >> any reasonable batch commit time.
  File size is irrelevant. Heartbeat interval is the only dependency.
```

---

## 15. Edge Cases

### Two pods race to acquire the pipeline lock
Both issue `UPDATE WHERE status='IDLE'` simultaneously.
Database serializes the writes — one gets rows=1 (acquired), one gets rows=0 (lost).
Loser sees rows=0, sets ownsLock=false, returns from scheduler cycle.
No exception. No entity. Database resolves it. ✓

### FAILURE_POISON raised mid-drain-cycle
Job hits MAX_RETRIES → FAILURE_POISON during drain loop iteration.
catch(Exception) in drain loop logs it, continues to next iteration.
Next iteration: Step 2 EXISTS check fires → scheduler breaks, releases lock.
No mid-batch halt. ✓

### STUCK_RELEASED job already at retry_count = MAX_RETRIES - 1
Janitor marks it STUCK_RELEASED (retry_count unchanged).
Scheduler claims it → PROCESSING directly.
Fails → retry_count++ = MAX_RETRIES → FAILURE_POISON.
Correct — prior failures count toward the budget.
Admin can reset with retry_count=0 for fresh start if crash was transient. ✓

### Position N in FAILURE_RETRYABLE cooldown
CTE finds position N as minimum non-terminal.
Outer query: retry_after > NOW() → returns nothing.
Scheduler returns. Position N+1 does NOT process. Stream order preserved.
Next cycle: if retry_after elapsed, position N is claimed. ✓

### All active jobs in FAILURE_RETRYABLE cooldown
CTE finds minimum position. Outer query returns nothing. Scheduler returns.
All jobs become eligible progressively as their cooldowns elapse. ✓

### Multiple FAILURE_POISON records
Step 2 EXISTS() fires on any. ALL must be resolved.
Dismissing position 2 does not resume scheduling if position 5 is also FAILURE_POISON. ✓

### Admin resets FAILURE_POISON with new source file
status=CREATED, bucket/objectName updated, retry_count=0.
stream_position unchanged. Job reprocesses at its original position.
Execution resumes: A → B' → C. ✓

### Admin resets FAILURE_POISON, job fails again
retry_count=0 after reset → MAX_RETRIES fresh attempts.
If all fail → FAILURE_POISON again → callForIncident() again.
Admin must investigate further. ✓

### Pod dies immediately after claim
Job transitions directly to PROCESSING with heartbeat_at=NOW() in one
transaction. Heartbeat stops. Janitor sweep 8.2: PROCESSING AND heartbeat_at
stale > threshold → STUCK_RELEASED. Next pod acquires lock → reprocesses. ✓

### Admin priority update during PROCESSING
Priority column no longer exists. No conflict possible. ✓

### Two registrations for the same source file
Each call to register() draws the next sequence value.
Both get distinct stream_positions. Both process in arrival order.
Second file would overwrite first via ON CONFLICT DO UPDATE.
Deduplication at registration is a controller concern, not scheduler concern. ✓

---

## 16. stateHistory JSONB Format

Append-only. Read by humans and diagnostic queries. Never by scheduler.

```json
[
  {
    "state": "CREATED",
    "timestamp": "2026-06-21T10:00:00"
  },
  {
    "state": "PROCESSING",
    "timestamp": "2026-06-21T10:05:01"
  },
  {
    "state": "GCS_FETCHED",
    "timestamp": "2026-06-21T10:05:03",
    "data": { "bytes": 524288000 }
  },
  {
    "state": "PROCESSING_STARTED",
    "timestamp": "2026-06-21T10:05:04"
  },
  {
    "state": "FAILURE_RETRYABLE",
    "timestamp": "2026-06-21T10:07:00",
    "data": { "error": "Connection reset by peer" }
  },
  {
    "state": "PROCESSING",
    "timestamp": "2026-06-21T10:12:01"
  },
  {
    "state": "COMPLETED",
    "timestamp": "2026-06-21T10:48:00",
    "data": {
      "rows_read": 880000,
      "rows_inserted": 878342,
      "rows_orphaned": 1201,
      "rows_failed": 457
    }
  }
]
```

In-memory append (read → mutate → write) is safe. The pipeline lock
guarantees single-writer access at all times.

---

## 17. Implementation Checklist

### Database
- [ ] `ingestion_pipeline_lock` table, singleton row inserted
- [ ] `ingestion_stream_pos_seq` sequence created
- [ ] `ingestion_audit_log` table with all columns
- [ ] Index on `status`
- [ ] Partial index on `stream_position ASC` WHERE status NOT IN ('COMPLETED','ADMIN_DISMISSED')

### Entities
- [ ] No `PipelineLock` entity — lock table owned entirely by native queries
- [ ] `IngestionAuditLog` JPA entity — `@Version Long version`, `Long streamPosition`
- [ ] `streamPosition` is `@Column(updatable = false)` — assigned on INSERT via sequence, never changed
- [ ] `AuditState` enum: CREATED, PROCESSING, FAILURE_RETRYABLE,
      FAILURE_POISON, STUCK_RELEASED, COMPLETED, ADMIN_DISMISSED
- [ ] `IngestionAuditLog.start()` factory: sets CREATED, request_received_at,
      status_updated_at, stateHistory = [] — stream_position assigned by DB sequence

### Repository
- [ ] `PipelineLockRepository` — all native `@Modifying @Query`, no entity, no Hibernate lifecycle
  - [ ] `int tryAcquire(String ownerId)` — UPDATE WHERE status='IDLE', returns rows affected
  - [ ] `void renewHeartbeat(String ownerId)` — UPDATE WHERE owner_id=:ownerId
  - [ ] `void release(String ownerId)` — UPDATE SET IDLE WHERE owner_id=:ownerId
  - [ ] `void resetStaleLock(Instant threshold)` — janitor reset WHERE heartbeat_at stale
- [ ] `IngestionAuditLogRepository`
  - [ ] `findNextEligible()` — strict sequential CTE query (stream_position)
  - [ ] `existsByStatus("FAILURE_POISON")` — halt check
  - [ ] `findStuckProcessing(Instant threshold)` — janitor query on heartbeat_at

### IngestionTx (all REQUIRES_NEW)
- [ ] `register(source, headerJson, bodyJson)` → CREATED, stateHistory init
- [ ] `claimAndStartProcessing(id)` → PROCESSING, heartbeat_at=NOW(), @Version check
- [ ] `heartbeat(id)` → heartbeat_at = NOW() only. Nothing else.
- [ ] `update(id, state)` → append to stateHistory, status_updated_at
- [ ] `update(id, state, data)` → append with data node
- [ ] `complete(id, result)` → COMPLETED, status_updated_at, summary fields
- [ ] `fail(id, error)` → FAILURE_RETRYABLE or FAILURE_POISON
      based on retry_count. Sets retry_after on RETRYABLE.
      Calls callForIncident() on POISON.
- [ ] `saveBatch(entities)` → REQUIRES_NEW, flush, entityManager.clear()

### IngestionScheduler
- [ ] `@Scheduled(fixedDelay)` not fixedRate
- [ ] `podId = System.getenv("HOSTNAME")` — resolved at startup
- [ ] Step 1: `int rows = lockRepository.tryAcquire(podId)` — rows=0 → return
- [ ] `ownsPipelineLock = true` on acquire
- [ ] Step 2: FAILURE_POISON EXISTS check
- [ ] Drain loop: query → claimAndStartProcessing → processJob → catch/continue
- [ ] Finally: `ownsPipelineLock = false` THEN `lockRepository.release(podId)`

### LockHeartbeatTask
- [ ] `@Scheduled(fixedDelay = 30_000)`
- [ ] `if (!ownsPipelineLock) return`
- [ ] `lockRepository.renewHeartbeat(podId)`

### IngestionJanitor
- [ ] `@Scheduled(fixedDelay)` independent of scheduler
- [ ] Sweep 1: `lockRepository.resetStaleLock(threshold)` — stale ACTIVE lock → IDLE
- [ ] Sweep 2: stale PROCESSING jobs → STUCK_RELEASED (via heartbeat_at)

### IngestionFlow ← DOMAIN SPECIFIC
- [ ] `processJob(UUID jobId)` — synchronous, no locking logic
- [ ] claimAndStartProcessing() → domain work → complete() or fail()
- [ ] All domain exceptions caught: tx.fail() → rethrow

### IngestionProcessor ← DOMAIN SPECIFIC
- [ ] `process(source, lookupData, jobId)` → returns result
- [ ] Per batch: saveBatch() then tx.heartbeat(jobId)
- [ ] Returns IngestionResult

### IngestionController ← DOMAIN SPECIFIC
- [ ] Parse event (CloudEvent, SQS, HTTP)
- [ ] Extract source metadata
- [ ] Call tx.register()
- [ ] Always return 200 OK

### Admin Controller
- [ ] POST /ingestion/admin/{id}/reset — FAILURE_POISON → CREATED
      Accepts optional body: { bucket, objectName } for source file replacement
      stream_position preserved, retry_count=0, retry_after=NULL
- [ ] POST /ingestion/admin/{id}/dismiss — FAILURE_POISON → ADMIN_DISMISSED
- [ ] Internal auth on all endpoints
- [ ] All transitions appended to stateHistory

### Application
- [ ] `@EnableScheduling` on main application class
- [ ] `volatile boolean ownsPipelineLock` accessible to scheduler and heartbeat task
- [ ] `podId` resolved from `System.getenv("HOSTNAME")` — set automatically by Kubernetes
- [ ] Configuration properties externalised for all thresholds and intervals

---

## 18. Domain-Specific Extension Points

Everything in this document is reusable across ingestion pipelines.
Only the following must be implemented per pipeline:

| Component | What to implement |
|---|---|
| `IngestionController` | Event source parsing. Source metadata extraction. |
| `IngestionFlow` | What processing means: what file to download, what lookup data to load. |
| `IngestionProcessor` | Data transformation: parsing, validation, mapping, destination write. |
| Source metadata columns | `bucket`, `object_name` for GCS. Adapt to your source. |
| `IngestionResult` | Result metrics. Adapt to what your processor measures. |
| `callForIncident()` | PagerDuty, OpsGenie, Slack, email — your incident system. |
| Destination repository | Where processed data lands. |
