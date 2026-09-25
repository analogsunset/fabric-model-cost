# %% [markdown]
# # 02 · V-Order copies
# Copies the base model's mirrored tables, pinned to one Delta version, into two lab lakehouses:
#
# | Variant | Data |
# |---|---|
# | A | mirrored table as-is (Databricks-written), read through the shortcut |
# | B | Fabric copy, **V-Order off** |
# | C | Fabric copy, **V-Order on** |
#
# B and C are written from the same cached DataFrame with optimize-write off, so V-Order is the only
# difference between them. A vs B is the Databricks file-layout effect.
#
# Default lakehouse: `cost_lab`. Appends to `vorder_copy`, `vorder_files`.

# %%
import sys
sys.path.insert(0, "/lakehouse/default/Files/fmc")
import importlib
import config as cfg
import fmc_lib as lib
importlib.reload(cfg); importlib.reload(lib)

import time
from urllib.parse import unquote
import pandas as pd
import pyarrow.parquet as pq
import sempy.fabric as fabric
import notebookutils
from delta.tables import DeltaTable

# %%
prod_ws, mirror_id = lib.resolve(cfg.PROD_WORKSPACE, cfg.MIRRORED_ITEM, "MirroredAzureDatabricksCatalog")
lab_ws, lh_b = lib.resolve(cfg.LAB_WORKSPACE, cfg.LH_NO_VORDER, "Lakehouse")
_, lh_c = lib.resolve(cfg.LAB_WORKSPACE, cfg.LH_VORDER, "Lakehouse")

db = lib.load_database(fabric.get_tmsl(cfg.BASE_MODEL, workspace=prod_ws))
tables = sorted({(p["schema_name"], p["entity_name"]) for p in lib.describe_partitions(db)
                 if p["source_type"] == "entity"}, key=str)
print(f"{len(tables)} tables:", tables)

# %% [markdown]
# ## Copy

# %%
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "false")
VORDER_CONFS = ["spark.sql.parquet.vorder.default",   # Runtime 1.3+
                "spark.sql.parquet.vorder.enabled"]   # older runtimes

def set_vorder(on):
    for k in VORDER_CONFS:
        spark.conf.set(k, str(on).lower())

cores = spark.sparkContext.defaultParallelism
copied_at = lib.utcnow()
copy_rows = []
for schema, table in tables:
    src = lib.table_path(prod_ws, mirror_id, schema, table)
    version = DeltaTable.forPath(spark, src).history(1).collect()[0]["version"]
    df = spark.read.format("delta").option("versionAsOf", version).load(src)
    if cfg.COPY_REPARTITION:
        df = df.repartition(cfg.COPY_REPARTITION)
    df = df.cache()
    rows = df.count()
    for variant, lh, vorder in (("B", lh_b, False), ("C", lh_c, True)):
        set_vorder(vorder)
        dst = lib.table_path(lab_ws, lh, schema, table)
        t0 = time.time()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dst)
        secs = time.time() - t0
        spark.sql(f"ALTER TABLE delta.`{dst}` SET TBLPROPERTIES ('delta.parquet.vorder.enabled' = '{str(vorder).lower()}')")
        copy_rows.append({"schema_name": schema, "entity_name": table, "variant": variant,
                          "source_version": int(version), "rows": rows, "copy_seconds": secs,
                          "spark_cores": cores, "copied_at": copied_at})
    df.unpersist()

copies = pd.DataFrame(copy_rows)
display(copies)

# %% [markdown]
# ## File stats
# Parquet footers confirm V-Order actually landed (C should show `vorder_files > 0`, A and B zero).

# %%
mounts = {}
def local_root(ws_id, item_id):
    key = (ws_id, item_id)
    if key not in mounts:
        mp = f"/fmc_{item_id[:8]}"
        notebookutils.fs.mount(lib.abfss(ws_id, item_id).rstrip("/"), mp)
        mounts[key] = notebookutils.fs.getMountPath(mp)
    return mounts[key]

def file_stats(ws_id, item_id, schema, table, version=None):
    path = lib.table_path(ws_id, item_id, schema, table)
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    files = reader.load(path).inputFiles()
    detail = DeltaTable.forPath(spark, path).detail().collect()[0]
    root = local_root(ws_id, item_id)
    row_groups, rg_rows, vorder_files = [], [], 0
    for uri in files[:cfg.FOOTER_SAMPLE_FILES]:
        rel = unquote(uri.split(f"/{item_id}/", 1)[1])
        md = pq.ParquetFile(f"{root}/{rel}").metadata
        row_groups.append(md.num_row_groups)
        rg_rows.append(md.num_rows / max(md.num_row_groups, 1))
        if any(b"vorder" in k.lower() for k in (md.metadata or {})):
            vorder_files += 1
    return {
        "files": len(files),
        "size_bytes": int(detail["sizeInBytes"]),   # latest version; for A this can drift after the pin
        "sampled_files": len(row_groups),
        "avg_row_groups_per_file": sum(row_groups) / len(row_groups) if row_groups else None,
        "avg_rows_per_row_group": sum(rg_rows) / len(rg_rows) if rg_rows else None,
        "vorder_files": vorder_files,
    }

file_rows = []
for r in copies[copies["variant"] == "B"].itertuples():
    for variant, ws_id, item_id, version in (("A", prod_ws, mirror_id, r.source_version),
                                              ("B", lab_ws, lh_b, None), ("C", lab_ws, lh_c, None)):
        file_rows.append({"schema_name": r.schema_name, "entity_name": r.entity_name, "variant": variant,
                          **file_stats(ws_id, item_id, r.schema_name, r.entity_name, version),
                          "copied_at": copied_at})

files = pd.DataFrame(file_rows)
display(files)
bad = files[(files["variant"] == "C") & (files["vorder_files"] == 0)]
if len(bad):
    print("WARNING: no V-Order marker found in variant C footers for:", list(bad["entity_name"]))

# %%
lib.save(spark, copies, "vorder_copy")
lib.save(spark, files, "vorder_files")
