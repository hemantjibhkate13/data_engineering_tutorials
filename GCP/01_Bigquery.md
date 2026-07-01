GCP Services

1. BIGQUERY

BIGQUERY = Serverless, columnar, distributed cloud data warehouse

ARCHITECTURE (separation of storage & compute):
- Storage: Colossus (distributed FS) + Capacitor (columnar format)
- Compute: Dremel engine -> query tree (root -> leaf nodes/slots)
- Network: Jupiter (petabit) connects storage<->compute
- Slot = unit of compute (parallel worker)

WHY COLUMNAR: only scans columns you select -> bills on bytes scanned
  => NEVER "SELECT *"; select only needed columns

PARTITIONING (cuts bytes scanned):
- Splits table physically by: time-unit (DATE/TIMESTAMP), ingestion time, or integer range
- Filter on partition col => partition pruning
- Max 4000 partitions per table

CLUSTERING (skips blocks within partitions):
- Sorts data by up to 4 columns inside each partition
- Best on high-cardinality cols used in WHERE / JOIN / GROUP BY
- Order of clustering cols matters (filter left-to-right)

PARTITION vs CLUSTER:
- Partition = drawer (coarse pruning, hard boundaries)
- Cluster   = sort order inside drawer (fine-grained block skipping)

PRICING:
- On-demand: ~$6.25 per TB scanned (unpredictable)
- Capacity/Slots: reserved slots, editions (Std/Ent/Ent+) (predictable)

COST CONTROL LEVERS:
- Partition + cluster, select fewer columns
- Use --dry_run to estimate bytes before running
- Set maximum_bytes_billed as a guardrail
- Materialized views / table snapshots for repeated queries

TABLE TYPES:
- Native (managed, stored in Capacitor) - fastest
- External (data in GCS/Iceberg, BQ queries in place) - flexible, slower


Q1 (Easy): In one or two sentences — why does BigQuery bill you based on "bytes scanned," and what's the single most common beginner mistake that blows up that cost?
═══════════════════════════════════════════════
Q1 — WHY BILLED ON BYTES SCANNED + #1 MISTAKE
═══════════════════════════════════════════════
- BigQuery stores data COLUMNAR (Capacitor format)
- A query only reads the COLUMNS it references, not whole rows
- You are billed for (bytes of those columns) across scanned partitions
- #1 mistake: SELECT *  -> forces read of EVERY column -> max cost
- Fix: select only needed columns; partition/cluster to scan less


Q2 (Easy-Medium): What is the practical difference between partitioning and clustering? Give me one example of when you'd use each.
═══════════════════════════════════════════════
Q2 — PARTITIONING vs CLUSTERING
═══════════════════════════════════════════════
PARTITIONING:
- Physically splits table into segments (hard boundaries)
- By DATE/TIMESTAMP, ingestion time, or integer range
- Filtering on partition col => "partition pruning" (skips whole segments)
- Coarse-grained. Max 4000 partitions.
- USE WHEN: clear date/range filter on most queries
  e.g. event_date — analysts always filter "last 7 days"

CLUSTERING:
- Sorts data WITHIN each partition by up to 4 columns
- Filtering/joining on clustered col => skips data blocks
- Fine-grained. No hard limit on distinct values.
- USE WHEN: high-cardinality columns in WHERE/JOIN/GROUP BY
  e.g. customer_id, customer_segment

ONE-LINER: Partition = which drawer to open;
           Cluster  = sorted order inside the drawer.


Q3 (Medium): In your T-Systems Gold layer, you created BigQuery tables. Suppose analysts query those tables filtering mostly by event_date and grouping by customer_segment. How would you physically design that table, and why?
═══════════════════════════════════════════════
Q3 — GOLD TABLE DESIGN (event_date filter, segment group-by)
═══════════════════════════════════════════════
DESIGN:
- PARTITION BY event_date  (DATE column)
- CLUSTER BY customer_segment

WHY:
- Analysts filter on event_date => partition pruning cuts bytes scanned
  (only reads relevant days, not full history)
- They GROUP BY customer_segment => clustering co-locates same-segment
  rows in sorted blocks => less data shuffled/scanned, faster aggregation
- Combined: cheap + fast for the dominant query pattern

DDL SHAPE:
  CREATE TABLE gold.campaign_segments
  PARTITION BY event_date
  CLUSTER BY customer_segment
  AS SELECT ...

INTERVIEW BONUS: mention require_partition_filter = TRUE
  -> forces every query to filter on partition => prevents
     accidental full-table scans by analysts.


Q4 (Medium-Hard): You have a query that's scanning 2 TB and costing too much. Walk me through your debugging checklist to bring the cost down — concrete steps, in order.
═══════════════════════════════════════════════
Q4 — 2 TB QUERY COST DEBUG CHECKLIST (in order)
═══════════════════════════════════════════════
1. RUN --dry_run (or check "bytes processed" estimate in UI)
   -> confirm how much it actually scans before optimizing
2. KILL "SELECT *" -> select only required columns
3. CHECK partitioning: is there a partition filter in WHERE?
   - if table not partitioned, that's the root cause -> redesign
   - if partitioned, ensure filter is on RAW partition col (no functions)
4. CHECK clustering: filtering/joining on clustered cols?
5. PUSH FILTERS EARLY: filter before JOINs, avoid scanning then dropping
6. AVOID re-scanning: use MATERIALIZED VIEWS / cache repeated subqueries
7. CHECK the query plan (Execution Details) for skew / heavy stages
8. GUARDRAIL: set maximum_bytes_billed so a bad query can't run wild
9. If repeated heavy workload -> consider slot reservation vs on-demand


Q5 (Hard / scenario): A BigQuery table is partitioned by date. A teammate writes WHERE CAST(event_timestamp AS STRING) LIKE '2024-01%'. They complain partitioning "isn't working" — the query still scans the whole table. What's happening and how do you fix it?
═══════════════════════════════════════════════
Q5 — CAST() ON PARTITION COL BREAKS PRUNING
═══════════════════════════════════════════════
PROBLEM:
- WHERE CAST(event_timestamp AS STRING) LIKE '2024-01%'
- Wrapping partition col in a FUNCTION (CAST) disables partition pruning
- BQ can't map the expression to physical partitions -> FULL SCAN

FIX (filter on raw partition col with a range):
  WHERE event_timestamp >= '2024-01-01'
    AND event_timestamp <  '2024-02-01'

RULE: Never apply functions (CAST, DATE(), etc.) to the partition
      column in WHERE. Keep the partition col "bare" so BQ can prune.



RF3: True or false, and why in one line: "Adding more partitions always makes queries cheaper."


RF1: Your table is partitioned by event_date and clustered by customer_id. A query filters WHERE customer_id = '12345' but has no date filter. Does clustering help? Does partitioning help? One line each.
RF1 — Clustering vs partitioning without a date filter:
- Clustering on customer_id => HELPS (skips blocks in every partition)
- Partitioning by event_date => does NOT help (no date filter = no pruning)
- RULE: partition pruning needs a filter on the partition column;
        clustering works regardless.


RF2: You need to give an analyst a table they query 50 times a day with the exact same expensive aggregation. What BigQuery feature do you reach for to stop paying for that scan every time?
RF2 — Repeated expensive aggregation:
- MATERIALIZED VIEW (auto-incremental refresh)
- BQ can auto-rewrite base-table queries to hit the MV (smart tuning)


RF3: True or false, and why in one line: "Adding more partitions always makes queries cheaper."
RF3 — "More partitions always cheaper" = FALSE
- Over-partitioning => metadata overhead + tiny files => slower
- Hard limit: 4000 partitions/table
- Pick granularity to match query pattern (daily vs monthly)
- Right-sized partitions = cheaper, not max partitions