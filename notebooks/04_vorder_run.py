# %% [markdown]
# # 04 · Run the experiment
# For each iteration and variant (order rotated so no variant always goes first):
#
# 1. **clear** – evict everything (`clearValues`)
# 2. **reframe** – frame the model (`full` refresh = Direct Lake framing)
# 3. **cold_touch** – one query per table that touches every column → transcoding cost
# 4. **warm_touch** – same queries again → scan cost without transcoding
# 5. **workload** – weighted sample of real prod DAX queries, warm
#
# Every DAX query carries a `/* fmc:... */` tag, so 05 attributes QueryEnd CPU exactly rather than by
# time window. Appends to `vorder_windows`, `vorder_memory`, `vorder_drift`, `vorder_workload`.

# %%
import sys
sys.path.insert(0, "/lakehouse/default/Files/fmc")
import importlib
import config as cfg
import fmc_lib as lib
importlib.reload(cfg); importlib.reload(lib)

import random
import time
import uuid
import pandas as pd
import sempy.fabric as fabric
from delta.tables import DeltaTable

# %%
prod_ws, mirror_id = lib.resolve(cfg.PROD_WORKSPACE, cfg.MIRRORED_ITEM, "MirroredAzureDatabricksCatalog")
names = {v: lib.variant_name(cfg.VARIANT_PREFIX, v, cfg.BASE_MODEL) for v in lib.VARIANTS}
db = lib.load_database(fabric.get_tmsl(names["A"], workspace=cfg.LAB_WORKSPACE))
touch_items = list(lib.column_touch_queries(db).items())
run_id = uuid.uuid4().hex[:8]
print("run_id:", run_id, "| tables to touch:", len(touch_items))

# %% [markdown]
# ## Capture the real workload from prod workspace monitoring

# %%
workload = lib.kql(spark, cfg.PROD_MONITORING_URI, cfg.PROD_MONITORING_DB, f"""
SemanticModelLogs
| where Timestamp > ago({cfg.WORKLOAD_LOOKBACK_DAYS}d)
| where ItemName == '{cfg.BASE_MODEL}' and OperationName == 'QueryEnd'
| where EventText has 'EVALUATE'
| summarize executions = count() by EventText
""")
queries_per_day = workload["executions"].sum() / cfg.WORKLOAD_LOOKBACK_DAYS
workload = workload.sort_values("executions", ascending=False).head(cfg.WORKLOAD_TOP_QUERIES)

# Drop queries that don't run standalone against the clone (parameters, session-scoped measures, ...).
ok = []
for q in workload["EventText"]:
    try:
        fabric.evaluate_dax(names["A"], q, workspace=cfg.LAB_WORKSPACE)
        ok.append(True)
    except Exception:
        ok.append(False)
workload = workload[ok]
print(f"{len(workload)} replayable queries, covering "
      f"{workload['executions'].sum() / cfg.WORKLOAD_LOOKBACK_DAYS / queries_per_day:.0%} of {queries_per_day:,.0f} queries/day")

rng = random.Random(42)   # same sequence for every variant
sequence = rng.choices(list(workload["EventText"]), weights=list(workload["executions"]), k=cfg.WORKLOAD_EXECUTIONS)
workload_items = [(f"w{j}", q) for j, q in enumerate(sequence)]

# %% [markdown]
# ## Run

# %%
pinned = (spark.table("vorder_copy").where("variant = 'B'")
          .orderBy("copied_at", ascending=False).toPandas()
          .drop_duplicates(["schema_name", "entity_name"]))
windows, memory, drift = [], [], []

def exec_queries(model, items, it, v, phase):
    errors = 0
    for key, q in items:
        try:
            fabric.evaluate_dax(model, lib.tag_query(q, run_id, it, v, phase, key), workspace=cfg.LAB_WORKSPACE)
        except Exception:
            errors += 1
    return errors

def tmsl(model, refresh_type):
    try:
        lib.run_tmsl(lib.refresh_script(model, refresh_type), cfg.LAB_WORKSPACE)
        return 0
    except Exception as e:
        print(f"  {model} {refresh_type} failed: {str(e)[:200]}")
        return 1

def phase(model, v, it, name, fn):
    start = lib.utcnow()
    errors = fn()
    end = lib.utcnow()
    windows.append({"run_id": run_id, "iteration": it, "variant": v, "model": model, "phase": name,
                    "start_utc": start, "end_utc": end, "seconds": (end - start).total_seconds(),
                    "errors": errors})
    time.sleep(cfg.PHASE_GAP_SEC)

def snapshot(model, v, it, state):
    memory.append({"run_id": run_id, "iteration": it, "variant": v, "state": state,
                   **lib.memory_stats(model, cfg.LAB_WORKSPACE), "at_utc": lib.utcnow()})

for it in range(cfg.ITERATIONS):
    # Databricks commits during the run change A's data (and reframe it) and invalidate the comparison.
    for r in pinned.itertuples():
        path = lib.table_path(prod_ws, mirror_id, r.schema_name, r.entity_name)
        current = DeltaTable.forPath(spark, path).history(1).collect()[0]["version"]
        if current != r.source_version:
            drift.append({"run_id": run_id, "iteration": it, "entity_name": r.entity_name,
                          "pinned_version": r.source_version, "current_version": int(current)})

    k = it % len(lib.VARIANTS)
    for v in lib.VARIANTS[k:] + lib.VARIANTS[:k]:
        m = names[v]
        print(f"iteration {it} · {v}")
        phase(m, v, it, "clear", lambda: tmsl(m, "clearValues"))
        phase(m, v, it, "reframe", lambda: tmsl(m, "full"))
        snapshot(m, v, it, "after_reframe")          # should be ~0 resident segments
        phase(m, v, it, "cold_touch", lambda: exec_queries(m, touch_items, it, v, "cold_touch"))
        phase(m, v, it, "warm_touch", lambda: exec_queries(m, touch_items, it, v, "warm_touch"))
        if it == cfg.ITERATIONS - 1:
            snapshot(m, v, it, "all_resident")       # full-model memory footprint
        phase(m, v, it, "workload", lambda: exec_queries(m, workload_items, it, v, "workload"))

if drift:
    print(f"WARNING: {len(drift)} drifted table/iteration pairs; A's numbers are suspect.")

# %%
lib.save(spark, pd.DataFrame(windows), "vorder_windows")
lib.save(spark, pd.DataFrame(memory), "vorder_memory")
lib.save(spark, pd.DataFrame(drift), "vorder_drift")
lib.save(spark, pd.DataFrame([{
    "run_id": run_id, "queries_per_day": float(queries_per_day),
    "replayable_queries": len(workload), "executions_per_iteration": len(workload_items),
    "tables_touched": len(touch_items),
}]), "vorder_workload")
print("done, run_id", run_id, "- wait for log ingestion, then run 05")
