DATAPROC = Google's MANAGED Hadoop/Spark cluster service (on Compute Engine VMs)

NODE TYPES:
- Master  : YARN ResourceManager + driver coordination (the brain)
- Worker  : YARN NodeManager + executors run tasks (the muscle)
- Secondary/Preemptible (Spot): cheap extra workers, can be reclaimed
  -> only for fault-tolerant compute, NEVER store HDFS data on them

KEY PRINCIPLE: COMPUTE SEPARATED FROM STORAGE
- Don't persist data on cluster HDFS
- Read/write GCS via the GCS connector
- Clusters are DISPOSABLE; data is DURABLE in GCS

WHY USE IT (interview):
1. Lift-and-shift existing Spark/Hadoop with minimal rewrite  <-- YOUR migration
2. Ephemeral job-scoped clusters => pay only for runtime

CLUSTER PATTERNS:
- Ephemeral: create -> run job -> delete  (cost-optimal, cloud-native)
- Long-running: shared/interactive use
- Orchestrate via Cloud Composer/Airflow:
  create cluster -> submit job -> delete cluster

COST & SCALING LEVERS:
- Right-size machine types to executor memory/cores
- Autoscaling: scales workers on YARN pending memory
- Preemptible/Spot workers: 60-80% cheaper, reclaimable
- Ephemeral clusters kill idle cost

GCS CONNECTOR GOTCHA:
- GCS = object store, NOT a filesystem
- "rename"/commit = expensive copy => slow output commit on big writes
- Mitigate: fewer larger output files, newer commit algorithms,
  write then move once

DATAPROC SERVERLESS:
- No cluster to manage; submit Spark, Google sizes/runs it
- The modern direction for batch Spark

DATAPROC vs DATAFLOW (common Q):
- Dataproc  = you control Spark/Hadoop (lift-and-shift, existing code)
- Dataflow  = fully managed Apache Beam (Google sizes everything, streaming-first)

DATAPROC SERVERLESS = run Spark with NO cluster to manage
- Submit a "batch" workload -> Google provisions, runs, tears down
- No master/worker, no machine types, no autoscaling config
- Unit of work = the JOB (batch), not a cluster
- Pay per job consumption (DCUs + runtime), no idle cost

TWO FLAVORS:
- Batch       : one-off Spark/PySpark jobs  <-- typical pipeline use
- Interactive : notebook/Sessions (Vertex AI Workbench)

WHY USE IT:
- Zero cluster ops (no sizing, no idle clusters)
- Per-job isolation (no noisy neighbors)
- Built-in autoscaling of executors
- Pay only for what the job consumes

WHAT YOU STILL TUNE (key senior point):
- spark.executor.cores / .memory
- spark.driver.memory
- spark.dynamicAllocation (min/max executors)
- custom container image for dependencies
=> Cluster ops removed, but SPARK TUNING is still YOUR job

CLASSIC vs SERVERLESS:
- Classic: you size cluster; full Hadoop ecosystem; OS control; init actions
- Serverless: no cluster; pure Spark batch; fast start; limited to Spark props

WHEN CLASSIC WINS:
- Need Hive/HBase/broader Hadoop ecosystem
- Custom init actions / OS-level control
- Persistent shared cluster for interactive use


DATAPROC SERVERLESS = run Spark with NO cluster to manage
- Submit a "batch" workload -> Google provisions, runs, tears down
- No master/worker, no machine types, no autoscaling config
- Unit of work = the JOB (batch), not a cluster
- Pay per job consumption (DCUs + runtime), no idle cost

TWO FLAVORS:
- Batch       : one-off Spark/PySpark jobs  <-- typical pipeline use
- Interactive : notebook/Sessions (Vertex AI Workbench)

WHY USE IT:
- Zero cluster ops (no sizing, no idle clusters)
- Per-job isolation (no noisy neighbors)
- Built-in autoscaling of executors
- Pay only for what the job consumes

WHAT YOU STILL TUNE (key senior point):
- spark.executor.cores / .memory
- spark.driver.memory
- spark.dynamicAllocation (min/max executors)
- custom container image for dependencies
=> Cluster ops removed, but SPARK TUNING is still YOUR job

CLASSIC vs SERVERLESS:
- Classic: you size cluster; full Hadoop ecosystem; OS control; init actions
- Serverless: no cluster; pure Spark batch; fast start; limited to Spark props

WHEN CLASSIC WINS:
- Need Hive/HBase/broader Hadoop ecosystem
- Custom init actions / OS-level control
- Persistent shared cluster for interactive use

Classic Dataproc	Dataproc Serverless
Cluster mgmt	You create/size it	None — Google handles it
Unit of work	Cluster	Batch (job)
Best for	Custom Hadoop ecosystem (Hive, HBase, long-running, fine OS control)	Pure Spark batch jobs, hands-off
Startup	Cluster spin-up time	Fast, no cluster to wait on
Control	Full (OS, init actions, node types)	Limited to Spark props + container image
Cost	Pay for cluster uptime	Pay per-job consumption


Q1 (Easy): What are the three node types in a Dataproc cluster, and what does each do?
Q2 (Easy-Medium): Why is it considered best practice to read/write data from GCS instead of the cluster's HDFS in Dataproc? What's the architectural principle?
Q3 (Medium): In your resume you "migrated legacy segmentation PySpark pipelines to GCP Dataproc." Describe the migration pattern — did you use one always-on cluster or ephemeral clusters? How did you orchestrate it, and why that choice?
Q4 (Medium-Hard): You claim you implemented partitioning, de-duplication, and join optimization in that migration. Pick the join optimization — walk me through one concrete Spark join problem you'd hit on 22M+ customer records and exactly how you'd fix it.
Q5 (Hard / scenario): Your Dataproc job processing 15-18M records/day suddenly starts taking 3x longer and you see executors failing with OOM errors. You haven't changed the code. Walk me through your diagnosis — what do you check, in what order, and what are the likely root causes?

═══════════════════════════════════════════════
Q1 — THREE NODE TYPES IN A DATAPROC CLUSTER
═══════════════════════════════════════════════
1. MASTER NODE
   - Runs YARN ResourceManager (allocates cluster resources)
   - Coordinates the Spark driver / job scheduling
   - The "brain" — manages, doesn't do the heavy lifting

2. WORKER NODE (primary)
   - Runs YARN NodeManager + executors (do the actual tasks)
   - Provides CPU/memory/disk for Spark task execution
   - The "muscle"

3. SECONDARY / PREEMPTIBLE (SPOT) WORKERS  [optional]
   - Cheap extra compute (Spot VMs, 60-80% cheaper)
   - Can be reclaimed by Google anytime
   - Compute-only, NO HDFS data stored on them
   - Used to scale out fault-tolerant batch cheaply

═══════════════════════════════════════════════
Q2 — WHY GCS OVER HDFS (architectural principle)
═══════════════════════════════════════════════
PRINCIPLE: SEPARATION OF COMPUTE AND STORAGE

- HDFS on the cluster is tied to the cluster's lifecycle
  -> delete the cluster, you lose the data
- GCS is durable, independent, decoupled from compute

WHY IT MATTERS:
- Clusters become DISPOSABLE (ephemeral) -> spin up/down freely,
  pay only for runtime, no idle cost
- Data SURVIVES cluster deletion -> persistent in GCS
- Multiple clusters/jobs can read the same GCS data (shared source of truth)
- Scale compute and storage INDEPENDENTLY

ONE-LINER: "Treat clusters as cattle, not pets — keep state in GCS
            so compute can be created and destroyed at will."

═══════════════════════════════════════════════
Q3 — MIGRATION PATTERN (segmentation -> Dataproc)
═══════════════════════════════════════════════
PATTERN: EPHEMERAL, JOB-SCOPED CLUSTERS (not always-on)
  [or Dataproc Serverless batch — say whichever your project uses]

FLOW:
- Legacy on-prem PySpark jobs lifted-and-shifted to Dataproc
  (minimal rewrite — same PySpark, point I/O at GCS)
- Source/target data on GCS (not cluster HDFS)
- Orchestrated via Cloud Composer (Airflow):
    DAG step 1: create Dataproc cluster (or submit Serverless batch)
    DAG step 2: submit PySpark segmentation job
    DAG step 3: validate output
    DAG step 4: delete cluster
- Each daily run is isolated and self-contained

WHY EPHEMERAL (not always-on):
- No idle cluster burning cost overnight
- Per-run isolation — one bad run can't poison a shared cluster
- Right-size compute per workload
- Cloud-native cost model: pay for runtime only

HONEST FRAMING (if pushed):
- "We moved to Dataproc Serverless batch, so there's no cluster to
   manage at all — Composer submits the batch, Google provisions it."

═══════════════════════════════════════════════
Q4 — JOIN OPTIMIZATION ON 22M+ RECORDS
═══════════════════════════════════════════════
PROBLEM: large-to-large shuffle join + DATA SKEW

Scenario:
- Joining 22M customer fact table to a dimension table on customer_id
- Default = SORT-MERGE JOIN -> shuffles both sides across network
- Skew: a few keys (e.g. null/placeholder customer_id, or one mega-segment)
  have disproportionate rows -> one task does most of the work
  -> stragglers, spill to disk, OOM, 3x slower stage

FIXES (pick based on table sizes):

A) BROADCAST JOIN (if one side is small, < ~10MB-ish / fits memory):
   - broadcast the small dimension table to all executors
   - eliminates shuffle entirely
   - spark.sql.autoBroadcastJoinThreshold, or broadcast(df) hint

B) HANDLE SKEW (both sides large):
   - Enable Adaptive Query Execution (AQE):
       spark.sql.adaptive.enabled = true
       spark.sql.adaptive.skewJoin.enabled = true
     -> AQE auto-splits skewed partitions at runtime
   - Or SALTING: add random salt to skewed keys to spread them
     across partitions, then join on (key + salt)

C) REDUCE SHUFFLE VOLUME:
   - Filter/project columns BEFORE the join (don't shuffle dead weight)
   - Pre-partition / bucket on join key to avoid re-shuffling
   - Tune spark.sql.shuffle.partitions to data size
     (default 200 often wrong for big data)

INTERVIEW LINE: "On the segmentation join I'd first check if the
dimension fits a broadcast — that kills the shuffle. If both sides are
big, I'd enable AQE skew-join handling, and if a specific key was still
hot, salt it."

═══════════════════════════════════════════════
Q5 — JOB SUDDENLY 3x SLOWER + EXECUTOR OOM (no code change)
═══════════════════════════════════════════════
KEY INSIGHT: code didn't change => DATA changed (volume/skew/distribution)

DIAGNOSIS ORDER:
1. CHECK INPUT DATA VOLUME
   - Did upstream data spike? More records/day than usual?
   - New source added? Partition exploded?

2. CHECK DATA SKEW (most likely culprit)
   - Spark UI -> Stages -> look for ONE task running far longer
   - One executor OOM while others idle = classic skew signature
   - Root cause: a hot key (null id, one giant segment) ballooned

3. CHECK SPARK UI / EXECUTORS TAB
   - Look for shuffle SPILL (memory -> disk) = memory pressure
   - GC time high? -> memory thrashing
   - Which stage fails? -> isolate the operation (usually a join/agg)

4. CHECK PARTITIONING
   - Too few partitions => each task too big => OOM
   - Too many tiny partitions => overhead
   - Re-evaluate spark.sql.shuffle.partitions vs new data size

5. CHECK CLUSTER / RESOURCES
   - Preemptible workers got reclaimed -> fewer executors -> slower?
   - Executor memory under-provisioned for new volume?

LIKELY ROOT CAUSES (rank):
- #1 Data skew from a new hot key (e.g. flood of null customer_ids)
- #2 Input volume spike pushing partitions past memory
- #3 Lost preemptible/spot workers reducing parallelism

FIXES MAP TO CAUSE:
- Skew    -> AQE skew join / salting / filter bad keys
- Volume  -> increase shuffle partitions, bump executor memory
- Workers -> reduce preemptible ratio for critical SLA jobs

INTERVIEW LINE: "First thing I'd say is: the code didn't change, so the
DATA changed. I'd go to the Spark UI, find the straggler stage, and
nine times out of ten it's a skewed key or a volume spike pushing a
partition past executor memory."


THE FRAMEWORK (answer skeleton for ANY optimization Q):
  1. SHUFFLE  2. SKEW  3. SPILL  4. PARTITIONING

──────────────────────────────────────────────
1. SHUFFLE (data moves across network — #1 cost)
   Triggered by: join, groupBy, distinct, repartition, window
   REDUCE BY:
   - Broadcast join (small side -> all executors, no shuffle)
   - Filter + select columns BEFORE shuffle (move less data)
   - Avoid reflexive repartition()
   - Bucket/pre-partition on join key

2. SKEW (one key dominates -> one task runs forever)
   Causes: null keys, one mega-value, placeholder IDs
   FIX:
   - AQE skew join: spark.sql.adaptive.skewJoin.enabled=true (first reach)
   - Salting: add random suffix to hot key, join on salted key
   - Isolate/filter bad keys (handle nulls separately)
   SIGNATURE: one task/executor slow or OOM while others idle

3. SPILL (partition > executor memory -> writes to disk)
   See in Spark UI: "Spill (Memory)" / "Spill (Disk)"
   FIX:
   - Raise spark.sql.shuffle.partitions (default 200 usually wrong)
   - Increase executor memory
   - Filter/project early to shrink data

4. PARTITIONING (right-size the work)
   - Too few -> huge tasks -> spill/OOM
   - Too many -> scheduling overhead
   - Target ~128-256MB per partition; 2-4 tasks per core
   - repartition() = FULL SHUFFLE (increase / even out)
   - coalesce()    = NO SHUFFLE (decrease, e.g. before write)

──────────────────────────────────────────────
CROSS-CUTTING WEAPONS:
- AQE (spark.sql.adaptive.enabled=true) = #1 modern flag
  -> coalesces partitions, fixes skew joins, auto-broadcast
- Cache/persist reused DataFrames (don't over-cache)
- Predicate + projection pushdown (Parquet/ORC) -> scan less
- File format: Parquet/ORC > CSV/JSON (columnar, splittable)
- Lazy eval: transformations lazy, actions trigger jobs
  -> NEVER collect() big data to driver (OOM)

KEY CONFIGS TO NAME-DROP:
- spark.sql.adaptive.enabled
- spark.sql.adaptive.skewJoin.enabled
- spark.sql.shuffle.partitions
- spark.sql.autoBroadcastJoinThreshold
- spark.executor.memory / .cores
- spark.dynamicAllocation.enabled

DEBUG REFLEX: go to SPARK UI -> find the slow STAGE -> check for
skew (one long task), spill (disk), shuffle size. Reason from evidence,
don't randomly tune.


Q1 (Easy): Name the four categories where Spark performance problems come from (the framework). Then: which one is usually the most expensive, and why?
Q2 (Easy-Medium): What's the difference between `repartition()` and `coalesce()`? When would you use each?
Q3 (Medium): What does AQE (Adaptive Query Execution) do, and name two specific problems it solves automatically at runtime.
Q4 (Medium-Hard): In your segmentation pipeline you process 22M+ customers daily. You have a transformation that does a `groupBy(customer_segment)` and you notice the job has one task taking 10x longer than the rest. Diagnose it and give me two different fixes.
Q5 (Hard / scenario): You're caching a DataFrame with `.cache()` because it's reused 3 times downstream, but after adding the cache the job got slower and you see executor memory pressure. What's going on, and how do you decide whether caching is even worth it here?

═══════════════════════════════════════════════
Q1 — THE FOUR CATEGORIES + MOST EXPENSIVE
═══════════════════════════════════════════════
THE FRAMEWORK: 1. Shuffle  2. Skew  3. Spill  4. Partitioning

MOST EXPENSIVE: SHUFFLE
WHY: it's the only one that moves data ACROSS THE NETWORK between
executors — disk write on map side, network transfer, disk read on
reduce side. Network + disk I/O dwarf in-memory compute. Skew, spill,
and bad partitioning often make shuffle WORSE, but shuffle is the
underlying cost. Minimize shuffle first.

═══════════════════════════════════════════════
Q2 — repartition() vs coalesce()
═══════════════════════════════════════════════
repartition(n):
- FULL SHUFFLE, redistributes data evenly across n partitions
- Can INCREASE or decrease partition count
- Use to: increase parallelism, or fix skew/uneven distribution

coalesce(n):
- NO shuffle (merges existing partitions on same executor)
- Can only DECREASE partition count
- Cheaper but can leave UNEVEN partitions
- Use to: reduce partitions before WRITING output
  (e.g. avoid 200 tiny files -> coalesce to a sane number)

ONE-LINER: "repartition reshuffles for even distribution (costly);
coalesce just merges to fewer partitions cheaply. I coalesce before
writes, repartition when I need even parallelism or to break skew."

═══════════════════════════════════════════════
Q3 — AQE (Adaptive Query Execution)
═══════════════════════════════════════════════
WHAT: Spark re-optimizes the query plan AT RUNTIME using actual
shuffle statistics (not just compile-time estimates).
Enable: spark.sql.adaptive.enabled = true

TWO+ PROBLEMS IT SOLVES AUTOMATICALLY:
1. COALESCING SHUFFLE PARTITIONS
   - merges many small post-shuffle partitions into right-sized ones
   - fixes the "default 200 partitions too many/small" problem
2. SKEW JOIN HANDLING
   - detects skewed partitions at runtime, splits them into subparts
   - (spark.sql.adaptive.skewJoin.enabled=true)
3. (BONUS) DYNAMIC JOIN SWITCHING
   - switches sort-merge join -> broadcast join when it discovers
     one side is actually small enough

WHY IT MATTERS: pre-AQE you had to tune partitions/joins by hand and
guess; AQE adapts to the real data shape mid-job. Table stakes now.

═══════════════════════════════════════════════
Q4 — groupBy(customer_segment), ONE task 10x slower
═══════════════════════════════════════════════
DIAGNOSIS: DATA SKEW.
- groupBy shuffles all rows of the same segment to ONE partition/task
- One dominant segment (or null/placeholder segment) = huge partition
- That one task processes way more rows -> 10x straggler
- Confirm in Spark UI: Stages -> one task with massive input/duration
  while others finished; possibly spill/OOM on that task

FIX 1 — AQE SKEW HANDLING (easiest, first reach):
  spark.sql.adaptive.enabled = true
  spark.sql.adaptive.skewJoin.enabled = true
  -> note: skewJoin targets JOINS; for groupBy-aggregation skew,
     AQE partition coalescing + bumping shuffle.partitions helps,
     but the strongest manual fix is salting (below)

FIX 2 — SALTING THE AGGREGATION (two-stage agg):
  - Add salt: key = customer_segment + "_" + (rand int 0..N)
  - Stage 1: groupBy(salted_key).agg(partial)   -> spreads hot segment
  - Stage 2: strip salt, groupBy(customer_segment).agg(combine partials)
  -> hot segment now split across N tasks instead of 1

FIX 3 (bonus) — ISOLATE BAD KEY:
  - If skew is from null/placeholder segment, filter it out, aggregate
    separately, union back. Removes the single hot partition.

INTERVIEW LINE: "One task 10x slower on a groupBy = textbook skew —
one segment dominates the shuffle. I'd confirm in the Spark UI, enable
AQE, and if a single segment is still hot, salt the aggregation into a
two-stage groupBy so that segment spreads across multiple tasks."

═══════════════════════════════════════════════
Q5 — .cache() made it SLOWER + memory pressure
═══════════════════════════════════════════════
WHAT'S GOING ON:
- cache() defaults to MEMORY_AND_DISK; it must MATERIALIZE + store the
  full DataFrame. That storage competes with execution memory.
- If the DF is large, caching evicts other data / forces spill to disk
  -> EXECUTION memory starved -> more spill, GC pressure -> SLOWER
- Also: caching adds upfront cost (must compute + store before reuse).
  If the recompute was cheap, the cache overhead isn't worth it.

HOW TO DECIDE IF CACHE IS WORTH IT:
- Worth it WHEN: DataFrame is reused multiple times AND its lineage is
  EXPENSIVE to recompute (big joins/aggregations upstream) AND it FITS
  in memory without starving execution.
- NOT worth it WHEN: cheap to recompute, used once, or too big to fit
  -> caching just steals memory.

WHAT TO DO HERE:
1. Check Storage tab in Spark UI: is the DF fully cached or partially
   evicted? Partial caching = worst of both worlds.
2. If lineage is cheap -> drop the cache, let it recompute.
3. If reuse is real but DF is big -> use persist(StorageLevel.DISK_ONLY)
   or filter/project to shrink it BEFORE caching.
4. Or restructure so the expensive part is computed once and written
   to GCS as Parquet, then re-read (often better than caching at scale).

INTERVIEW LINE: "Cache isn't free — it materializes and holds the whole
DF, competing with execution memory. If it's large it starves execution
and causes spill, so the job slows down. I only cache when the lineage
is expensive AND it's reused AND it fits. Otherwise I'd recompute, use
DISK_ONLY, or stage to Parquet on GCS."

KEY NUANCE: cache(... ) = lazy (materializes on first action).
StorageLevels: MEMORY_ONLY, MEMORY_AND_DISK (default for DF), DISK_ONLY.