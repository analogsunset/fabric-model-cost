# %% [markdown]
# # 05 · V-Order report
# Pulls the tagged QueryEnd events for a run from lab workspace monitoring and turns them into:
#
# * transcoding CPU per table (cold_touch − warm_touch), per variant
# * warm CPU per real workload query, per variant
# * memory footprint and file layout per variant
# * daily cost delta = query-side savings × prod reframe / query rates − cost of keeping a V-Ordered copy
#
# Transcoding is measured by touching every column, so it's an upper bound per reframe: in prod only
# the columns queries actually use get re-transcoded.

# %%
import sys
sys.path.insert(0, "/lakehouse/default/Files/fmc")
import importlib
import config as cfg
import fmc_lib as lib
importlib.reload(cfg); importlib.reload(lib)

import time
import pandas as pd
import sempy.fabric as fabric

RUN_ID = None   # None = latest run

# %%
windows = spark.table("vorder_windows").toPandas()
RUN_ID = RUN_ID or windows.sort_values("end_utc")["run_id"].iloc[-1]
windows = windows[windows["run_id"] == RUN_ID]
start, end = pd.to_datetime(windows["start_utc"], utc=True).min(), pd.to_datetime(windows["end_utc"], utc=True).max()

wait = cfg.LOG_INGEST_WAIT_SEC - (lib.utcnow() - end.to_pydatetime()).total_seconds()
if wait > 0:
    print(f"waiting {wait:.0f}s for log ingestion")
    time.sleep(wait)

models = ", ".join(f"'{m}'" for m in windows["model"].unique())
fmt = "%Y-%m-%dT%H:%M:%SZ"
logs = lib.kql(spark, cfg.LAB_MONITORING_URI, cfg.LAB_MONITORING_DB, f"""
SemanticModelLogs
| where Timestamp between (datetime({start:{fmt}}) - 1m .. datetime({end:{fmt}}) + 5m)
| where ItemName in ({models})
| project Timestamp, ItemName, OperationName, CpuTimeMs, DurationMs, EventText
""")
logs["Timestamp"] = pd.to_datetime(logs["Timestamp"], utc=True)
print(f"run {RUN_ID}: {len(logs):,} log events, {len(windows)} phase windows")

# %% [markdown]
# ## Tagged query CPU

# %%
qe = logs[logs["OperationName"] == "QueryEnd"].copy()
tags = qe["EventText"].map(lib.parse_tag)
qe = pd.concat([qe[tags.notna()].reset_index(drop=True),
                pd.DataFrame(list(tags.dropna())).reset_index(drop=True)], axis=1)
qe = qe[qe["run_id"] == RUN_ID]
qe["cpu_s"] = qe["CpuTimeMs"] / 1000.0

per_key = qe.groupby(["iteration", "variant", "phase", "key"], as_index=False)["cpu_s"].sum()

touch = per_key[per_key["phase"].isin(["cold_touch", "warm_touch"])].pivot_table(
    index=["iteration", "variant", "key"], columns="phase", values="cpu_s").reset_index()
touch["transcode_cpu_s"] = touch["cold_touch"] - touch["warm_touch"]
transcode = touch.groupby(["variant", "key"])["transcode_cpu_s"].median().unstack("variant")
display(transcode.assign(C_vs_A=transcode["C"] / transcode["A"]).sort_values("A", ascending=False))

wl = per_key[per_key["phase"] == "workload"].groupby(["iteration", "variant"]).agg(
    cpu_s=("cpu_s", "sum"), executions=("key", "count")).reset_index()
wl["cpu_s_per_query"] = wl["cpu_s"] / wl["executions"]
warm = wl.groupby("variant")["cpu_s_per_query"].median()
display(warm.to_frame("median warm CPU s / query"))

# %% [markdown]
# ## All operations by phase (window-based)
# QueryEnd CPU may not include every background cost (e.g. framing, paging). This shows every operation
# type seen in each phase window so nothing hides.

# %%
rows = []
for w in windows.itertuples():
    s, e = pd.to_datetime(w.start_utc, utc=True), pd.to_datetime(w.end_utc, utc=True) + pd.Timedelta(seconds=2)
    ev = logs[(logs["ItemName"] == w.model) & logs["Timestamp"].between(s, e)]
    for op, cpu in ev.groupby("OperationName")["CpuTimeMs"].sum().items():
        rows.append({"iteration": w.iteration, "variant": w.variant, "phase": w.phase, "op": op, "cpu_s": cpu / 1000})
ops = pd.DataFrame(rows)
if len(ops):
    display(ops.groupby(["phase", "op", "variant"])["cpu_s"].median().unstack("variant"))
print(f"Total lab CpuTimeMs in run window: {logs['CpuTimeMs'].sum():,.0f} "
      f"(calibrate CU_PER_CPU_SECOND against Capacity Metrics for {start:%H:%M}–{end:%H:%M} UTC)")

# %% [markdown]
# ## Memory and file layout

# %%
mem = spark.table("vorder_memory").toPandas()
mem = mem[(mem["run_id"] == RUN_ID)]
resident = mem[mem["state"] == "all_resident"].set_index("variant")
resident["total_gb"] = (resident["segment_used_bytes"] + resident["dictionary_bytes"]) / 1e9
display(resident[["segment_used_bytes", "dictionary_bytes", "total_gb"]])
cold = mem[mem["state"] == "after_reframe"]
if (cold["segments_resident"] > 0).any():
    print("WARNING: segments were still resident after clear+reframe; cold numbers understate transcoding.")

files = spark.table("vorder_files").toPandas()
files = files[files["copied_at"] == files["copied_at"].max()]
display(files.groupby("variant")[["files", "size_bytes", "avg_row_groups_per_file",
                                  "avg_rows_per_row_group", "vorder_files"]].agg(
    {"files": "sum", "size_bytes": "sum", "avg_row_groups_per_file": "mean",
     "avg_rows_per_row_group": "mean", "vorder_files": "sum"}))

drift = spark.table("vorder_drift").toPandas() if spark.catalog.tableExists("vorder_drift") else pd.DataFrame()
if len(drift) and (drift["run_id"] == RUN_ID).any():
    print("WARNING: mirrored tables changed during this run; variant A is not comparable.")

# %% [markdown]
# ## Daily cost delta
# Prod rates from the latest inventory (01) and the captured workload (04).

# %%
inv_parts = spark.table("inv_partitions").toPandas()
inv_parts = inv_parts[(inv_parts["model"] == cfg.BASE_MODEL) &
                      (inv_parts["inventoried_at"] == inv_parts["inventoried_at"].max())]
commits = spark.table("inv_table_commits").toPandas()
commits = commits[commits["inventoried_at"] == commits["inventoried_at"].max()]
entity_to_table = {(r.schema_name, r.entity_name): r.table for r in inv_parts.itertuples()}
reframes_by_table = {entity_to_table[(r.schema_name, r.entity_name)]: r.reframes_per_day
                     for r in commits.itertuples() if (r.schema_name, r.entity_name) in entity_to_table}

model_reframes = spark.table("inv_model_reframes").toPandas()
model_reframes = model_reframes[(model_reframes["model"] == cfg.BASE_MODEL)]
model_reframes_per_day = float(model_reframes.sort_values("inventoried_at")["reframes_per_day"].iloc[-1])
queries_per_day = float(spark.table("vorder_workload").where(f"run_id = '{RUN_ID}'").first()["queries_per_day"])

copies = spark.table("vorder_copy").toPandas()
copies = copies[copies["copied_at"] == copies["copied_at"].max()]
c_copy = copies[copies["variant"] == "C"]
# 1 CU = 2 Spark vCores. Full rewrite, so an over-estimate of an incremental sync.
maint_cu_s = cfg.VORDER_COPY_CU_SECONDS or float((c_copy["copy_seconds"] * c_copy["spark_cores"] / 2).sum())
syncs_per_day = cfg.COPY_SYNCS_PER_DAY if cfg.COPY_SYNCS_PER_DAY is not None else model_reframes_per_day
c_storage_gb = files[files["variant"] == "C"]["size_bytes"].sum() / 1e9

def variant_profile(v):
    return {"transcode_cpu_s": transcode[v].fillna(0).to_dict(), "warm_cpu_s_per_query": float(warm[v])}

comparisons = {
    "C vs A (adopt V-Ordered copy)": ("A", "C", True),
    "C vs B (pure V-Order effect)": ("B", "C", False),
    "B vs A (Databricks layout effect)": ("A", "B", False),
}
summary = []
for label, (base, cand, with_copy) in comparisons.items():
    d = lib.daily_delta(
        variant_profile(base), variant_profile(cand), reframes_by_table, queries_per_day,
        maintenance_cu_s_per_sync=maint_cu_s if with_copy else 0.0,
        syncs_per_day=syncs_per_day if with_copy else 0.0,
        extra_storage_gb=c_storage_gb if with_copy else 0.0,
        cu_per_cpu_s=cfg.CU_PER_CPU_SECOND, price_per_cu_hour=cfg.PRICE_PER_CU_HOUR,
        onelake_price_gb_month=cfg.ONELAKE_PRICE_GB_MONTH)
    summary.append({"comparison": label, **d})

summary = pd.DataFrame(summary)
display(summary)
print(f"inputs: {queries_per_day:,.0f} queries/day, {model_reframes_per_day:.1f} model reframes/day, "
      f"copy maintenance {maint_cu_s:,.0f} CU-s/sync × {syncs_per_day:.1f} syncs/day, "
      f"extra storage {c_storage_gb:.1f} GB")
if cfg.CU_PER_CPU_SECOND is None:
    print("Set CU_PER_CPU_SECOND in config.py to convert CPU savings into CU and dollars.")

# %%
lib.save(spark, summary.assign(run_id=RUN_ID, reported_at=lib.utcnow()), "vorder_summary")
