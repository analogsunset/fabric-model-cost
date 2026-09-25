# fabric_model_cost

Cost analysis for Fabric semantic models that read a **mirrored Azure Databricks catalog**, starting with
the question: *what does the missing V-Order on Databricks-written Delta tables cost us under Direct Lake?*

All cost here is Fabric capacity CU; nothing queries a Databricks SQL warehouse.

## Layout

```
fmc/config.py          tenant settings (edit me)
fmc/fmc_lib.py         shared helpers: TMSL parsing/cloning, DAX tagging, cost math, Fabric wrappers
notebooks/01_inventory.py      models, storage modes, sources, memory, Delta commit / reframe rates
notebooks/02_vorder_copy.py    pinned copies of the model's tables: B (V-Order off), C (V-Order on)
notebooks/03_vorder_models.py  three Direct Lake clones of the model: A (mirrored), B, C
notebooks/04_vorder_run.py     clear → reframe → cold touch → warm touch → real workload, tagged
notebooks/05_vorder_report.py  transcoding / warm CPU, memory, file layout, daily cost delta
tools/py2ipynb.py      converts notebooks/*.py → build/*.ipynb for Fabric import
tests/                 unit tests for the pure helpers (python -m unittest discover -s tests)
```

## The experiment

| Variant | Data | Isolates |
|---|---|---|
| A | mirrored table as-is | today's cost |
| B | Fabric copy, V-Order off | A vs B = Databricks file layout |
| C | Fabric copy, V-Order on | B vs C = V-Order itself |

B and C come from the same cached DataFrame at a pinned Delta version with optimize-write off. Parquet
footers are checked to confirm V-Order actually landed in C.

Cost per day of *not* V-Ordering:

```
Σ_table (transcode CPU_A − transcode CPU_C) × reframes/day of that table
+ (warm CPU/query_A − warm CPU/query_C) × prod queries/day
− cost of keeping a V-Ordered copy (Spark CU per sync × syncs/day + OneLake storage)
```

- Reframes/day come from the mirrored tables' `_delta_log` (commits bucketed to 5 min).
- Queries/day and the replayed DAX come from prod workspace monitoring.
- Every replayed query carries a `/* fmc:run|iter|variant|phase|key */` tag, so its QueryEnd CPU is
  attributed exactly rather than by time window.

## Setup

1. **Lab workspace** on a small dedicated F SKU. Enable **workspace monitoring** on it, and on the prod
   workspace if it isn't already.
2. In the lab workspace create lakehouses `cost_lab` (results), `lab_novorder` and `lab_vorder`.
   `lab_*` must be **schema-enabled** so `Tables/<schema>/<table>` matches the mirrored catalog.
3. Edit `fmc/config.py`, then upload `fmc/config.py` and `fmc/fmc_lib.py` to `cost_lab` → `Files/fmc/`.
4. `python tools/py2ipynb.py`, import `build/*.ipynb` into the lab workspace, and attach `cost_lab` as
   the default lakehouse of each notebook.
5. Run 01 → 05 in order. Run 02–04 in a window when Databricks isn't writing to these tables
   (04 records drift if it does).
6. Calibrate: 05 prints total `CpuTimeMs` for the run window. Sum the `vo_*` models' CU over the same
   window in the Capacity Metrics app, set `CU_PER_CPU_SECOND = CU_s / CPU_s`, rerun 05.

## Verify on first run

Written against current docs but not yet run in a tenant. Check these first:

- `sempy.fabric.execute_tmsl`, `get_tmsl`, `resolve_item_id(type="MirroredAzureDatabricksCatalog")`
  exist in your semantic-link version (`%pip install -U semantic-link` if not).
- Workspace monitoring `SemanticModelLogs` columns: `Timestamp`, `ItemName`, `OperationName`,
  `CpuTimeMs`, `DurationMs`, `EventText`, and that `EventText` holds the full DAX including the tag comment.
- The V-Order footer marker: 02 looks for a Parquet key-value key containing `vorder`. If C shows zero,
  inspect one footer by hand before trusting either the check or the write settings.
- `clearValues` on a Direct Lake model. 04 snapshots residency after reframe; 05 warns if segments were
  still resident (cold numbers would then be understated).
- Direct Lake on OneLake over the mirrored item's shortcuts, and whether cloned models need their cloud
  connection bound once (03 prints the framing error if so).
- Spark session timezone is UTC (phase windows are compared to log timestamps in 05).

## Caveats

- Cold touch reads every column, so transcoding per reframe is an **upper bound**; prod only
  re-transcodes columns queries actually hit.
- Copy maintenance is estimated from a full rewrite (`copy_seconds × cores / 2`); an incremental sync is
  cheaper. Override with `VORDER_COPY_CU_SECONDS` from Capacity Metrics.
- Memory footprint (`all_resident`) matters beyond CU: if V-Order moves the model under your SKU's
  Direct Lake memory guardrail, that's a SKU step, not a percentage.

## Next

- `06_actual_cost`: per-model daily CU from Capacity Metrics, plus each model's share of the mirrored
  item's SQL endpoint CU via `queryinsights.exec_requests_history`.
- Reuse 03/04 to compare other configurations (Import, DirectQuery on the SQL endpoint, Direct Lake on SQL).
