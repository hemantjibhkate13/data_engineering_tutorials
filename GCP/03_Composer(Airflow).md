CLOUD COMPOSER = Google's MANAGED Apache Airflow (runs on GKE)
AIRFLOW = workflow ORCHESTRATOR; pipelines as DAGs in Python

CORE VOCAB:
- DAG       : whole pipeline, graph of tasks, no cycles (Python file)
- Task      : one unit of work
- Operator  : template for a task (instantiate -> task)
              e.g. DataprocSubmitJobOperator, BigQueryInsertJobOperator,
                   GCSToGCSOperator, BashOperator, PythonOperator
- Scheduler : decides when DAGs/tasks run (the brain)
- Executor  : how tasks run (Celery / Kubernetes)
- Worker    : where task code executes
- XCom      : pass SMALL data between tasks (not big datasets!)

#1 PRINCIPLE: AIRFLOW ORCHESTRATES, IT DOES NOT PROCESS
- It TRIGGERS Dataproc/BigQuery; it doesn't crunch the data itself
- ANTI-PATTERN: pandas transform inside a task (worker is small) -> WRONG
- Push compute OUT to Dataproc/BQ; Airflow coordinates only

WHY AIRFLOW (vs cron + shell):
- Dependencies (A succeeds -> B runs)
- Auto retries w/ backoff
- UI visibility + per-task logs
- Backfills (re-run past dates)
- Complex scheduling

PRODUCTION LEVERS:
- schedule_interval + start_date (runs at END of interval; execution_date)
- IDEMPOTENCY: re-run = same result (overwrite partition, don't blind-append)
  -> matters because retries/backfills re-execute tasks
- retries, retry_delay, sla
- SENSORS: wait for condition (file in GCS, partition exists)
  -> use mode='reschedule' or DEFERRABLE operators (free worker slot)
- POOLS / concurrency limits (don't spawn 50 clusters at once)
- Connections & Variables: creds/config OUTSIDE dag code

TYPICAL DE DAG (your export pipeline):
  extract_from_BQ >> validate >> stage_to_GCS >> compress >> deliver_DIL
  (with retries + checksum validation between steps)

EPHEMERAL DATAPROC PATTERN (ties to Dataproc topic):
  create_cluster >> submit_pyspark_job >> delete_cluster
  (DataprocCreateClusterOperator / SubmitJob / DeleteCluster)

COMPOSER-SPECIFIC:
- Managed: Google runs scheduler/workers on GKE, handles scaling/upgrades
- DAGs stored in a GCS bucket (drop .py in the dags/ folder)
- Composer 2 = autoscaling workers (GKE Autopilot)


Questions — easy → hard, tied to YOUR DAGs
Q1 (Easy): What is a DAG, and what does the "acyclic" part actually mean / why does it matter?
Q2 (Easy-Medium): Explain the difference between an operator and a task. Give two operators you'd use in your export pipeline.
Q3 (Medium): This is the big one — explain why it's a bad idea to do heavy data processing inside an Airflow task. What's the correct pattern, and how does your export DAG follow it?
Q4 (Medium-Hard): Idempotency. Your DAG runs daily and writes segmentation output to a BigQuery table. A run fails halfway, Airflow retries the task, and now you risk duplicate data. How do you design the task so retries don't corrupt the table?
Q5 (Hard / scenario): Your export DAG has a sensor waiting for an upstream file to land in GCS. Lately, multiple DAG runs pile up and you're running out of worker slots — everything stalls. What's happening, and what are two ways to fix it?
Q3 and Q4 are the ones that separate people who use Airflow from people who understand it. Take your time.

═══════════════════════════════════════════════
Q1 — WHAT IS A DAG + WHY "ACYCLIC" MATTERS
═══════════════════════════════════════════════
DAG = Directed Acyclic Graph = the whole pipeline defined in Python.
- "Directed": tasks flow in one direction (A -> B -> C)
- "Acyclic": NO cycles — a task can never loop back to an earlier task

WHY ACYCLIC MATTERS:
- A cycle (A->B->A) would mean a task waits on itself -> infinite loop,
  the pipeline could never finish or even be scheduled
- Acyclic guarantees a clear start, finite execution, and a resolvable
  order of dependencies (Airflow can compute "what runs next")

ONE-LINER: "Directed = flow has direction; acyclic = no loops, so the
pipeline always terminates and dependencies are resolvable."

═══════════════════════════════════════════════
Q2 — OPERATOR vs TASK
═══════════════════════════════════════════════
OPERATOR = the TEMPLATE / blueprint for a unit of work (a class).
TASK     = an INSTANTIATED operator inside a DAG (the actual node).

ANALOGY: Operator = class, Task = object/instance of it.
  extract = BigQueryInsertJobOperator(task_id="extract", ...)
  -> BigQueryInsertJobOperator is the OPERATOR
  -> "extract" is the TASK

TWO OPERATORS IN MY EXPORT PIPELINE:
- BigQueryInsertJobOperator  -> run extract query from BQ
- GCSToGCSOperator / BashOperator -> stage/move/compress to GCS
  (also: DataprocSubmitJobOperator for PySpark validation steps)

═══════════════════════════════════════════════
Q3 — WHY NOT PROCESS DATA INSIDE AN AIRFLOW TASK ★
═══════════════════════════════════════════════
CORE TRUTH: Airflow is an ORCHESTRATOR, not a PROCESSING ENGINE.

WHY IT'S BAD TO PROCESS IN-TASK:
- Workers are SMALL (limited CPU/memory) — not built for big data
- Pulling 22M records into a worker (e.g. pandas) -> OOM / crash
- It doesn't scale — no distributed compute on the worker
- It blocks the worker slot, starving other tasks
- You lose all the benefits of a real engine (Spark parallelism, BQ slots)

CORRECT PATTERN: PUSH COMPUTE OUT
- Airflow only TRIGGERS the heavy work on a real engine:
    -> BigQuery (SQL at scale) or Dataproc/Spark (distributed)
- The data NEVER flows through the Airflow worker
- Airflow handles: sequencing, dependencies, retries, monitoring

HOW MY EXPORT DAG FOLLOWS IT:
- extract step = BigQueryInsertJobOperator -> BQ does the heavy query,
  result stays in BQ/GCS, not the worker
- validation/transform = DataprocSubmitJobOperator -> Spark does it
- Airflow just orchestrates the sequence + retries + checksums
- The 22M records are processed by BQ/Spark; Airflow never touches them

ONE-LINER: "Airflow conducts the orchestra, it doesn't play the
instruments. Data is processed by BigQuery and Dataproc; Airflow only
triggers, sequences, and monitors."

═══════════════════════════════════════════════
Q4 — IDEMPOTENCY: RETRY WITHOUT DUPLICATES ★
═══════════════════════════════════════════════
PROBLEM: task fails halfway, Airflow retries, naive APPEND = duplicates.

PRINCIPLE: design the WRITE so re-running produces the SAME result
(idempotent), regardless of how many times it runs.

TECHNIQUES (best for BigQuery):

1. WRITE_TRUNCATE / OVERWRITE THE PARTITION (cleanest)
   - Write to the specific date partition with WRITE_TRUNCATE
   - Retry overwrites that partition instead of appending
   - destination: table$YYYYMMDD with WRITE_TRUNCATE disposition
   -> re-run replaces the day's data, never duplicates

2. DELETE-THEN-INSERT (idempotent within a transaction)
   - DELETE WHERE event_date = {{ ds }}  then INSERT
   - or MERGE statement keyed on a unique business key (upsert)

3. PARTITION-SCOPED + execution_date ({{ ds }})
   - Always key the write to the run's logical date (ds / execution_date)
   - Each run only ever touches its own partition

WHY execution_date MATTERS:
- Airflow gives a deterministic logical date per run
- Scope every write to that date -> retries/backfills stay isolated

ONE-LINER: "I make the write idempotent — instead of appending, I
overwrite the target partition for that run's execution_date using
WRITE_TRUNCATE (or a MERGE/delete-insert). A retry just rewrites the
same partition, so there's no way to duplicate."

═══════════════════════════════════════════════
Q5 — SENSOR PILE-UP / WORKER SLOTS EXHAUSTED
═══════════════════════════════════════════════
WHAT'S HAPPENING:
- A sensor in default 'poke' mode HOLDS a worker slot the entire time
  it waits — even while idle, just polling
- If files are late, multiple DAG runs each hold a sensor slot
- Slots fill up -> no workers free for real tasks -> everything stalls
  (compounded if DAG concurrency / max_active_runs lets runs pile up)

TWO FIXES:

FIX 1 — SENSOR mode='reschedule'
   - Instead of holding the slot while waiting, the sensor RELEASES the
     slot between pokes and is rescheduled -> frees workers
   - Best for long waits

FIX 2 — DEFERRABLE OPERATORS / SENSORS (modern, best)
   - Use a deferrable sensor (e.g. GCSObjectExistenceSensorAsync)
   - It hands off waiting to the TRIGGERER process -> uses ZERO worker
     slots while waiting
   - Most scalable solution

EXTRA CONTROLS (bonus):
- max_active_runs on the DAG -> stop runs piling up
- sensor timeout + poke_interval tuned sensibly
- pools to cap concurrent sensor tasks

ONE-LINER: "Default poke-mode sensors hold a worker slot while waiting,
so late files cause pile-ups that starve the pool. I switch to
mode='reschedule' or, better, deferrable sensors that offload waiting to
the triggerer and use no worker slot — plus cap max_active_runs."