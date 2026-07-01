# AWS Data Engineering — Study Pack
## Tier 2 · Topic 4: Amazon Athena

> Athena : S3 :: BigQuery (external tables) : GCS — serverless SQL over object storage, no cluster, pay per scan.
> Engine = **Trino/Presto** (v3). Metadata = **Glue Data Catalog**. Storage = **S3** (no native storage of its own).

---

## 1. What & Why
Serverless interactive query service running **standard SQL directly on S3** — no loading, no infra.
Point a Catalog table at S3, submit SQL, Athena scans in place, bills for **data scanned**.
- Default ad-hoc/interactive query layer for an S3 lake; zero ops.
- Closes the loop: Glue writes Parquet → Catalog registers → Athena queries.
- SQL dialect = **Presto/Trino** (differs from Spark SQL / BigQuery SQL). Separate **Athena for Spark** (PySpark) mode exists.

---

## 2. Architecture
No storage, no metastore of its own. Data in **S3**, metadata in **Glue Data Catalog**.
`SQL → Athena (Trino) → read Catalog schema → scan S3 → result (+ written to S3 results bucket)`.
- **Results bucket** — every query output lands here (and is cached). Must be configured or queries fail.
- **Tables = external** (Hive `CREATE EXTERNAL TABLE` + LOCATION + SerDe), or via Glue/crawlers.
- **Partitions:** registered (crawler / `ALTER TABLE ADD PARTITION` / `MSCK REPAIR`) **or**
  **partition projection** (compute partition values from a formula — no crawler, faster planning; modern best practice).

---

## 3. Cost Model (most important thing)
**Bills per TB scanned** (on-demand ~$5/TB, region-varying). Drives every optimization.
Reduce bytes scanned → save money + time:
- **Parquet/ORC** (columnar) — read only needed columns; CSV/JSON scans everything (10×+ savings).
- **Compression** (Snappy/ZSTD).
- **Partitioning** — `WHERE dt='...'` scans one partition; projection makes it cheap to manage.
- **Projection + predicate pushdown** — `SELECT col1,col2` not `*`; filter early.
- **Compaction / right-sized files** — small-files problem hurts.
- **`LIMIT` ≠ cost control** for non-partitioned data (common myth).
- **Provisioned capacity** (DPU-hour reservations) for high-volume/predictable workloads; default is per-scan.

---

## 4. Key Features
- **CTAS** (`CREATE TABLE AS SELECT`) — query → new partitioned Parquet table in S3 in one stmt. Lightweight ETL/materialization without Spark.
- **`INSERT INTO`** — append query results to existing table.
- **Views** — saved logical queries (no stored data).
- **Federated queries** — query non-S3 (RDS, DynamoDB, JDBC) via Lambda-based connectors; join lake + operational DB without moving data.
- **Athena for Apache Spark** — serverless PySpark notebooks (separate from SQL engine).
- **Workgroups** — isolate teams: separate history, results location, **per-query/per-workgroup scan limits & cost controls** (key governance lever).
- **UNLOAD / prepared statements / parameterization.**

---

## 5. GCP / Apache Mapping
| Concept | Hive/Presto | GCP | Athena |
|---|---|---|---|
| Serverless SQL on lake | Presto/Trino (self-mgd) | **BigQuery (external)** | **Athena** |
| Engine | Presto/Trino | Dremel | **Trino** |
| Metadata | Hive Metastore | BQ catalog | **Glue Data Catalog** |
| Pricing | infra | per-TB scanned | **per-TB scanned** |
| Materialize | CTAS | CTAS | **CTAS** |
| Cost guardrail | — | quotas/reservations | **Workgroups + scan limits** |

**Talking point:** Athena ≈ BigQuery external-table mode; both bill per-byte-scanned → identical playbook
(columnar, partition, scan less). **Key difference:** BigQuery has its own *native* managed storage too;
Athena has **no native storage** — always queries S3 in place.

---

## 6. Best Practices
- Parquet + Snappy/ZSTD + partitioning + compaction = the whole cost story.
- Partition projection over crawlers/manual for date-style data.
- Workgroups + per-query scan limits; separate per team/env.
- CTAS to convert + pre-aggregate; build gold tables analysts hit (not raw).
- `EXPLAIN` + query data-scanned stats to find expensive queries.
- No `SELECT *`; filter on partition keys; project needed columns.
- **Query result reuse / caching** to skip re-scans.
- **Bucketing** for high-cardinality join/filter keys.
- Govern with **Lake Formation** (column/row-level) on the Catalog.

---

## 7. Mistakes / Limits / Optimization
- **CSV/JSON in lake** → scans everything → huge bills (#1 mistake).
- **No / bad partitioning** → full scans or metadata explosion.
- **Small files** → slow planning, per-file overhead.
- **Missing results bucket / workgroup config** → failures / cost leak.
- **`LIMIT` ≠ cost control** for non-partitioned.
- **Not a high-concurrency low-latency dashboard DB** — it's interactive/ad-hoc; use **Redshift** or pre-aggregate. Concurrency quotas apply.
- **Very large transforms** — Spark/Glue/EMR better than CTAS at extreme scale.
- **Cross-region** scanning adds transfer cost/latency.

---

## ⭐ Key Takeaways
1. Athena = serverless Trino SQL on S3; **no storage, no metastore** of its own (uses S3 + Glue Catalog).
2. **Bills per TB scanned** — every optimization = scan fewer bytes (Parquet, partition, project, compress).
3. **Partition projection** > crawlers for date data; **CTAS** for in-place transform/materialization.
4. **Workgroups + scan limits** = cost governance; results bucket is mandatory.
5. ≈ BigQuery external mode; key diff = Athena has no native storage.
6. Not for high-concurrency low-latency dashboards → that's Redshift (next).

---
*Prev: 03_AWS_Glue_Notes.md · Next: 05_AWS_Redshift_Notes.md*


✅ Questions — Amazon Athena
This is your strongest mapping yet (you know BigQuery external tables cold), so this is the perfect topic to try unaided — I think you'll surprise yourself. Take a real swing; I'll evaluate honestly and find the gaps before producing the model-answers file.
Easy

1. In one or two sentences: what is Athena, and what two things does it depend on since it has no storage or metastore of its own?
2. How does Athena bill you, and why does that single fact drive almost every optimization decision?
Medium 3. Name four concrete techniques to reduce the cost of an Athena query, and briefly why each works. 4. What is partition projection, and why is it often preferred over a Glue crawler for date-partitioned data?
Scenario / Interview-style 5. An analyst complains their Athena queries over a 2 TB CSV dataset are slow and the monthly bill is huge. Walk through how you'd diagnose and fix it — and roughly what cost improvement you'd expect. 6. Your company wants analysts to explore the lake freely but is terrified of a runaway query scanning 100 TB and blowing the budget. What Athena feature(s) do you use, and how?
Stretch 7. Compare Athena and Redshift (we haven't formally done Redshift yet — reason from what you know about warehouses vs query-on-lake). When would you pick each, and what's the giveaway in a question that tells you which one the interviewer wants? Also: how does Athena compare to BigQuery, given you know BQ well?
Fire away — and genuinely, give 5 and 7 a shot in your own words. Those are the ones that separate "knows the service" from "can architect with it."

Answer the questions and give notes.



# AWS Data Engineering — Study Pack
## Tier 2 · Topic 4: Amazon Athena — Model Answers (Q&A)

> Companion to `04_AWS_Athena_Notes.md`. Worked answers for interview drilling.
> Coaching note: attempt unaided first — the reference pack and the drilling are two different products.

---

### Q1. What is Athena + two dependencies
Serverless interactive query service running standard SQL (Trino/Presto) directly on S3, no infra.
No storage/metastore of its own → depends on **(1) S3** (data) and **(2) Glue Data Catalog** (metadata).

---

### Q2. Billing + why it drives optimization
Bills **per TB scanned** (~$5/TB on-demand). Cost = bytes read, and the same things that cut bytes cut latency,
so format/compression/partitioning/projection are literally the bill. One fact → whole optimization playbook.

---

### Q3. Four cost-reduction techniques
- **Parquet/ORC (columnar)** — read only referenced columns (biggest lever, 10×+).
- **Compression (Snappy/ZSTD)** — fewer physical bytes to scan.
- **Partitioning (+ projection)** — `WHERE dt=...` prunes to one partition.
- **Column projection + predicate pushdown** — `SELECT col1,col2 WHERE...` not `*`.
- *(Bonus: compaction/right-sized files for per-file overhead.)*

---

### Q4. Partition projection + why over crawler
Athena **computes partition values from a formula** (date/int range, enum) in table props instead of storing
them in the Catalog. Preferred for date data: no crawler cost/schedule, no Catalog reads at plan time
(faster planning at thousands of partitions), partitions known instantly as new dates land (no crawl lag),
and **no re-inference → no schema drift**. Crawler = repeatedly scan+register; projection = derive on the fly.

---

### Q5. 2 TB CSV slow + expensive — diagnose + fix
**Diagnose:** CSV = row-based, uncompressed → every query scans full 2 TB regardless of columns needed.
Confirm via "data scanned" stat (~2 TB/query). Check partitioning + small files.
**Fix:** Convert to **partitioned Parquet + Snappy** via **CTAS** (or Glue) — partition by date.
**Improvement:** columnar+compression alone 5–10×+; with partition pruning a 2 TB scan → tens of GB =
**10–30×+ cost/latency reduction**. E.g., `SELECT 3 cols WHERE dt=...` drops from 2 TB to a few GB.

---

### Q6. Prevent runaway 100 TB query
**Athena Workgroups** with a **per-query data scan limit** (and optional per-workgroup aggregate limit):
put analysts in a workgroup that cancels any query scanning more than N TB → single bad query can't blow budget.
Workgroups also isolate results location + history; pair with CloudWatch alarms on bytes scanned.
Free exploration + hard ceiling.

---

### Q7. Athena vs Redshift; giveaway; vs BigQuery
**Athena** = query-on-lake: serverless, no own storage, pay-per-scan; ad-hoc/interactive, intermittent, data already in S3.
**Redshift** = warehouse: own optimized columnar storage, load/model data; high-concurrency, low-latency, repeated BI,
complex joins at scale, predictable SLAs.
- Athena when: sporadic queries, data in S3, zero ops, exploration.
- Redshift when: many concurrent users, sub-second dashboards, heavy repeated analytics, modeled schemas, SLAs.
- **Giveaway:** *ad-hoc/occasional/explore/serverless/data in S3* → Athena. *dashboards/concurrency/low-latency/BI/SLA/star schema* → Redshift.
- Bridge: **Redshift Spectrum** (Redshift queries S3) + **Redshift Serverless** blur the line.

**vs BigQuery:** Athena ≈ BQ **external-table** mode — both serverless, both per-byte-scanned, same playbook.
Key diff: **BQ also has native managed storage** (load data in for perf/clustering); **Athena has no native
storage, always queries S3 in place.** BQ spans Athena-role (external) + Redshift-role (native) in one product;
AWS splits them into Athena + Redshift.

---
*Companion to: 04_AWS_Athena_Notes.md · Next topic: 05_AWS_Redshift_Notes.md*