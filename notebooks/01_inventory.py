# %% [markdown]
# # 01 · Inventory
# Every semantic model in the prod workspace: effective storage mode per partition, what it reads,
# resident memory, and the Delta commit rate of each mirrored table it reads. Commit rate drives
# Direct Lake reframes, which is what makes the missing V-Order expensive (or not).
#
# Default lakehouse: `cost_lab`. Appends to `inv_models`, `inv_partitions`, `inv_table_commits`,
# `inv_model_reframes`.

# %%
import sys
sys.path.insert(0, "/lakehouse/default/Files/fmc")
import importlib
import config as cfg
import fmc_lib as lib
importlib.reload(cfg); importlib.reload(lib)

from datetime import timedelta
import pandas as pd
import sempy.fabric as fabric
from delta.tables import DeltaTable

# %%
prod_ws, mirror_id = lib.resolve(cfg.PROD_WORKSPACE, cfg.MIRRORED_ITEM, "MirroredAzureDatabricksCatalog")
items = fabric.list_items(workspace=prod_ws)
endpoint = items[(items["Type"] == "SQLEndpoint") & (items["Display Name"] == cfg.MIRRORED_ITEM)]
mirror_ids = {mirror_id.lower()} | {i.lower() for i in endpoint["Id"]}
run_at = lib.utcnow()

# %% [markdown]
# ## Models and partitions

# %%
model_rows, part_rows = [], []
for name in fabric.list_datasets(workspace=prod_ws)["Dataset Name"]:
    parts, mem, err = [], {}, None
    try:
        db = lib.load_database(fabric.get_tmsl(name, workspace=prod_ws))
        parts = lib.describe_partitions(db)
        mem = lib.memory_stats(name, prod_ws)   # Direct Lake: only what's resident right now
    except Exception as e:
        err = str(e)[:500]
    for p in parts:
        p.update(model=name, inventoried_at=run_at,
                 reads_mirror=(p["source_id2"] or "").lower() in mirror_ids)
    part_rows += parts
    model_rows.append({
        "model": name, "mode": lib.model_mode(parts),
        "tables": len({p["table"] for p in parts}),
        "reads_mirror": any(p["reads_mirror"] for p in parts),
        **mem, "error": err, "inventoried_at": run_at,
    })

models = pd.DataFrame(model_rows)
partitions = pd.DataFrame(part_rows)
display(models)

# %% [markdown]
# ## Delta commit rate of mirrored tables
# Read straight from `_delta_log` through the shortcut, so no Databricks access is needed.

# %%
since = run_at - timedelta(days=cfg.COMMIT_LOOKBACK_DAYS)
mirrored = partitions[partitions["reads_mirror"] & (partitions["source_type"] == "entity")] if len(partitions) else partitions
commit_times, table_rows = {}, []
for schema, entity in sorted({(r.schema_name, r.entity_name) for r in mirrored.itertuples()}, key=str):
    path = lib.table_path(prod_ws, mirror_id, schema, entity)
    try:
        hist = (DeltaTable.forPath(spark, path).history()
                .select("version", "timestamp", "operation").toPandas())
    except Exception as e:
        table_rows.append({"schema_name": schema, "entity_name": entity, "error": str(e)[:500], "inventoried_at": run_at})
        continue
    hist["timestamp"] = pd.to_datetime(hist["timestamp"], utc=True)
    recent = hist[hist["timestamp"] >= since]
    times = list(recent["timestamp"].dt.to_pydatetime())
    commit_times[(schema, entity)] = times
    table_rows.append({
        "schema_name": schema, "entity_name": entity,
        "latest_version": int(hist["version"].max()),
        "commits": len(recent),
        "commits_per_day": len(recent) / cfg.COMMIT_LOOKBACK_DAYS,
        "reframes_per_day": lib.reframes_per_day(times, cfg.COMMIT_LOOKBACK_DAYS, cfg.REFRAME_BUCKET_MINUTES),
        "operations": ", ".join(f"{k}:{v}" for k, v in recent["operation"].value_counts().items()),
        "error": None, "inventoried_at": run_at,
    })

table_commits = pd.DataFrame(table_rows)
display(table_commits.sort_values("reframes_per_day", ascending=False) if "reframes_per_day" in table_commits else table_commits)

# %%
# Model-level reframes: any commit on any table the model reads (bucketed) triggers one.
model_reframes = []
for name, grp in mirrored.groupby("model") if len(mirrored) else []:
    times = [t for key in {(r.schema_name, r.entity_name) for r in grp.itertuples()}
             for t in commit_times.get(key, [])]
    model_reframes.append({
        "model": name,
        "reframes_per_day": lib.reframes_per_day(times, cfg.COMMIT_LOOKBACK_DAYS, cfg.REFRAME_BUCKET_MINUTES),
        "inventoried_at": run_at,
    })
model_reframes = pd.DataFrame(model_reframes)
display(model_reframes)

# %%
lib.save(spark, models, "inv_models")
lib.save(spark, partitions, "inv_partitions")
lib.save(spark, table_commits, "inv_table_commits")
lib.save(spark, model_reframes, "inv_model_reframes")
