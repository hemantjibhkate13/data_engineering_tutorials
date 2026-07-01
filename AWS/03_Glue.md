# AWS Data Engineering — Study Pack
## Tier 2 · Topic 3: AWS Glue

> Glue = managed serverless **Spark** + a **Hive Metastore** + **schema discovery**. Your Spark/PySpark/Hive
> background maps almost 1:1. "Glue" is a FAMILY, not one thing — keep the components separate.

Components: **Data Catalog** (metastore) · **Crawlers** (discovery) · **ETL Jobs** (serverless Spark) ·
**Studio/Workflows/Triggers** (authoring + light orchestration).

---

## 1. Glue Data Catalog — the metastore (linchpin)
- Central managed metadata: databases, tables, schemas, partitions, types, S3 locations. **Hive Metastore-compatible.**
- Shared by **Athena, Redshift Spectrum, EMR, Glue ETL** — define a table once, every engine sees it. Backbone of the AWS lakehouse.
- **Structure:** Database (namespace) → Table (schema + S3 location + format + SerDe + partition keys; **no data, points at S3** = external-table model) → Partitions (`dt=.../` entries for pruning) → Connections (JDBC creds/network).

| Concept | Hive | GCP | Glue |
|---|---|---|---|
| Metastore | Hive Metastore | Dataplex/BQ meta | **Data Catalog** |
| External table | Hive external | BQ external | Catalog table → S3 |
| Partition discovery | `MSCK REPAIR` | — | Crawler / partition projection |

> One-liner: "Glue Data Catalog is a managed, Hive-compatible metastore shared across Athena, EMR, Redshift Spectrum, and Glue."

---

## 2. Glue Crawlers — schema & partition discovery
- Scan S3/JDBC → infer schema, detect format, discover partitions → write/update Catalog tables automatically.
- **Caveats (interview point):** cost per run + latency; inference can be **wrong** (guesses types, messy JSON, schema merges); re-crawling for frequent partitions is wasteful.
- **Prod alternatives:** **Athena partition projection** (compute partitions from a formula — no crawler), programmatic partition registration (`boto3`/Catalog API), or **explicit table defs in IaC** (Terraform/CDK).
> Signal: "Crawlers for initial/unknown schemas; in prod prefer partition projection or explicit table defs."

---

## 3. Glue ETL Jobs — serverless Spark
- Managed serverless **Apache Spark** (PySpark/Scala); Glue provisions/runs/tears down. ≈ Dataproc serverless.
- **DPU** = compute/billing unit (1 DPU ≈ 4 vCPU + 16 GB), per-second billing (1-min min). Workers: **G.1X/G.2X/G.4X/G.8X**, G.025X (small streaming).
- **Glue versions ↔ Spark:** Glue 4.0 = Spark 3.3, Glue 5.0 = Spark 3.5 (current). Native Iceberg/Hudi/Delta in 4.0+.
- **Two APIs:**
  - **Plain PySpark** — normal `SparkSession`; your existing code drops in.
  - **DynamicFrame** — Glue's schema-flexible abstraction over DataFrames; handles semi-structured/inconsistent data; transforms `ResolveChoice`, `ApplyMapping`, `Relationalize`, `DropNullFields`. Convert: `dyf.toDF()` / `DynamicFrame.fromDF()`.
  - **When which:** DynamicFrame for messy/evolving schema + Catalog/bookmark integration + ambiguous types; DataFrame for heavy/complex transforms, windows, perf tuning. Common pattern: read as DynamicFrame → `.toDF()` → work in Spark → convert back to write.
- **Job Bookmarks** — built-in **incremental** state (tracks processed S3 objects/JDBC keys); re-run handles only new data. Powerful but finicky — misconfig → silently skips/reprocesses. Top "why didn't my data show up" bug.
- **Job types:** Spark (batch, main) · Spark Streaming (micro-batch from Kinesis/Kafka) · **Python Shell** (lightweight non-Spark, 0.0625/1 DPU, cheap) · Ray (distributed Python).

---

## 4. Orchestration: Triggers / Workflows / Studio
- **Triggers** — schedule / on-demand / on-completion (conditional).
- **Workflows** — chain crawlers + jobs + triggers into a visual multi-step pipeline w/ shared state.
- **Studio** — drag-and-drop authoring → generates PySpark; fine for simple jobs.
> Honesty: Workflows OK for Glue-only pipelines; real cross-service orchestration → **Step Functions** or **MWAA (Airflow)**.

---

## 5. End-to-End Placement
```
S3 (bronze) → Crawler/explicit table → Data Catalog
            → Glue ETL (PySpark): clean/dedupe/Parquet/partition → S3 (silver/gold)
            → Data Catalog (output) → Athena / Redshift Spectrum
   orchestrated by Glue Workflow / Step Functions / MWAA
```
= GCP `GCS → Dataproc/Dataflow → BigQuery`, re-skinned.

---

## 6. Best Practices
- DataFrames for heavy logic; DynamicFrames at read/write edges (Catalog + bookmarks).
- Right-size workers + auto-scaling; watch Spark UI for skew/spill.
- Job bookmarks for incremental — but test; keep an idempotent fallback.
- Partition outputs; Parquet ~128 MB–1 GB (S3 lessons carry over).
- Avoid crawlers in steady state → partition projection / programmatic registration.
- Externalize config (job params); Glue Connections + Secrets Manager for JDBC creds.
- Continuous logging + Spark UI → CloudWatch.
- Glue 4.0+/5.0 for modern Spark + Iceberg/Hudi/Delta; **Flex execution** (spot) for cheap non-urgent jobs.

---

## 7. Mistakes / Limits / Optimization
- Treating Glue as "just Spark," ignoring DynamicFrame/bookmarks → fighting the framework.
- **Crawler schema drift** changing types → break downstream. Lock schemas in prod.
- **Bookmark misconfig** → skipped/duplicated data.
- **Small-files output** → coalesce/repartition before write.
- **Cold start / job-start latency** → not for sub-second needs.
- **Cost** — DPU-hours add up; Python Shell for small, Flex for cheap, right-size workers.
- **Skew & spill** — usual Spark tuning (salting, broadcast joins, partition tuning).
- **Soft quotas** — concurrent runs, DPUs/account; request increases at scale.

---

## ⭐ Key Takeaways
1. Glue = **Catalog + Crawlers + serverless Spark + light orchestration** — keep them distinct.
2. **Data Catalog = managed Hive Metastore** shared across all query engines — the lakehouse backbone.
3. **Crawlers** are convenient but error-prone; prefer **partition projection / explicit defs** in prod.
4. ETL = serverless Spark; know **DPUs**, **DynamicFrame vs DataFrame**, **job bookmarks** (incremental).
5. For real orchestration reach for **Step Functions / MWAA**, not Workflows.
6. S3 best practices (Parquet, file sizing, partitioning) apply directly to Glue outputs.

---
*Prev: 02_AWS_S3_Notes.md · Next: 04_AWS_Athena_Notes.md*


Questions — AWS Glue
Attempt these yourself first — and remember the coaching note: answer every clause of the question, don't stop at the definition. Glue is heavily favored in AWS DE interviews, so this is high-value drilling. Your Spark background means you should be able to reason through most of these.
Easy

1. In one sentence each, what are the four main components of Glue and what does each do?
2. What is the Glue Data Catalog, and which AWS query engines share it?
Medium 3. Explain the difference between a Glue DynamicFrame and a Spark DataFrame. When would you deliberately choose each, and what's the common real-world pattern that uses both? 4. What are Glue job bookmarks, what problem do they solve, and name one way they commonly break.
Scenario / Interview-style 5. You're asked to build a daily batch pipeline: raw JSON lands in S3, must be cleaned, deduped, converted to partitioned Parquet, and made queryable in Athena. Describe the Glue components you'd use end-to-end — and where you'd avoid a crawler and why. 6. A junior engineer set up a crawler to run every hour on a date-partitioned dataset, and you're seeing rising costs plus an occasional schema-drift incident that broke a downstream Athena query. What's going wrong and what would you change?
Stretch 7. Compare doing your ETL on Glue vs EMR vs Dataproc (which you know). When would you pick Glue over a full Spark cluster, and what are the cost/control tradeoffs? (We haven't formally covered EMR yet — reason from first principles; I'll fill gaps.)


Answer the questiona and give notes.

# AWS Data Engineering — Study Pack
## Tier 2 · Topic 3: AWS Glue — Model Answers (Q&A)

> Companion to `03_AWS_Glue_Notes.md`. Worked answers for interview drilling.
> Coaching note: attempt these unaided before reading — interview readiness is only tested when you talk them cold.

---

### Q1. Four main components (one line each)
- **Data Catalog** — managed Hive-compatible metastore (databases, schemas, partitions pointing at S3).
- **Crawlers** — scan S3/JDBC → infer schema → register/update Catalog tables & partitions.
- **ETL Jobs** — serverless Apache Spark (PySpark/Scala); Glue provisions/runs/tears down.
- **Studio/Workflows/Triggers** — visual authoring + light orchestration (schedule/on-demand/on-completion).

---

### Q2. Data Catalog + who shares it
Central managed metadata (databases, tables, schemas, partitions, types, S3 locations), **Hive Metastore-compatible**.
Tables hold no data — they point at S3 (external-table model). Shared by **Athena, Redshift Spectrum, EMR, Glue ETL**.
Define once, every engine sees it = lakehouse backbone.

---

### Q3. DynamicFrame vs DataFrame
**DataFrame** = fixed known schema, standard Spark. **DynamicFrame** = schema-flexible (per-record schema),
tolerates messy/semi-structured data, Glue transforms (`ResolveChoice`, `ApplyMapping`, `Relationalize`,
`DropNullFields`), native Catalog + bookmark integration.
- **DynamicFrame** when: messy/evolving schema, ambiguous types (`ResolveChoice`), Catalog/bookmark integration.
- **DataFrame** when: heavy/complex transforms, windows, joins, perf tuning.
- **Combined pattern:** read as DynamicFrame → `.toDF()` → do work in Spark → `DynamicFrame.fromDF()` to write.

---

### Q4. Job bookmarks
Built-in **incremental** state: Glue persists what's processed (S3 object/timestamp or JDBC key) so re-runs
handle only new data. Solves incremental loads without reprocessing all data or hand-rolled watermarks.
**Breaks when:** misconfigured key / missing stable `transformation_ctx`, or source files **overwritten in place**
(bookmark keys off new objects → in-place update skipped). → silent skip/reprocess.

---

### Q5. Daily batch: JSON → clean/dedupe → partitioned Parquet → Athena
1. Raw JSON in S3 (bronze).
2. Source table: explicit def (preferred, schema known) or one-time crawler.
3. **Glue ETL (PySpark):** read (DynamicFrame→`.toDF()`), clean (cast/drop nulls), dedupe (`dropDuplicates`/window),
   convert to **Parquet+Snappy**, **partition by date**, write ~128 MB–1 GB to silver/gold. Bookmarks for incremental.
4. Register output partitions via **partition projection** or programmatic add — **not** a recurring crawler.
5. **Athena** queries the Catalog table.
6. Orchestrate: Trigger/Workflow, or Step Functions/MWAA in a bigger DAG.
**Avoid crawler on:** ongoing output partition discovery — predictable `dt=` pattern, wasteful + drift risk.
Crawler only for initial/unknown schema.

---

### Q6. Hourly crawler on date-partitioned data — diagnosis + fix
**Problems:** (1) hourly crawl re-scans S3 for predictable `dt=` partitions = pure cost/latency waste;
(2) each crawl **re-infers types** → a differently-shaped file shifts/merges schema → breaks downstream Athena.
**Fix:** kill the hourly crawler → **partition projection** or programmatic partition registration;
**lock schema** via explicit table def (IaC), treat schema changes as reviewed events; consider **Iceberg** for
managed evolution. If a crawler must stay: tight scope, new prefixes only, schema-change policy = log-don't-modify.

---

### Q7. Glue vs EMR vs Dataproc
Axis = serverless convenience vs cluster control/cost-at-scale (all run Spark).
- **Glue (serverless):** no cluster mgmt, fast, per-sec billing, autoscale, native Catalog/bookmarks. Pick for
  standard/spiky/intermittent ETL, no-ops teams, lakehouse integration. *Cons:* less Spark control, job-start
  latency, **DPU-hours expensive for huge/constant workloads.**
- **EMR (managed clusters):** full Hadoop/Spark ecosystem, deep tuning, **cheaper at sustained scale** (spot,
  long-lived clusters). Pick for heavy/continuous/customized big-data. *Cons:* you own cluster ops. (EMR Serverless splits difference.)
- **Dataproc (GCP):** closest analog to EMR (managed clusters); Dataproc Serverless ≈ Glue serverless.
- **Map:** Glue ≈ Dataproc Serverless · EMR ≈ Dataproc.
- **Rule:** Glue for serverless/integrated/variable ETL; EMR for large/continuous/customized Spark.
  **Cost crossover** = key point: serverless wins intermittent, clusters win sustained heavy load.

---
*Companion to: 03_AWS_Glue_Notes.md · Next topic: 04_AWS_Athena_Notes.md*