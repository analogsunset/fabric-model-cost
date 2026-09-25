# %% [markdown]
# # 03 · Variant models
# Clones the base model three times into the lab workspace as Direct Lake on OneLake, each pointed at
# a different copy of the same tables (A = mirrored item, B / C = lab lakehouses). Then frames them
# and checks row counts match.

# %%
import sys
sys.path.insert(0, "/lakehouse/default/Files/fmc")
import importlib
import config as cfg
import fmc_lib as lib
importlib.reload(cfg); importlib.reload(lib)

import pandas as pd
import sempy.fabric as fabric

# %%
prod_ws, mirror_id = lib.resolve(cfg.PROD_WORKSPACE, cfg.MIRRORED_ITEM, "MirroredAzureDatabricksCatalog")
lab_ws, lh_b = lib.resolve(cfg.LAB_WORKSPACE, cfg.LH_NO_VORDER, "Lakehouse")
_, lh_c = lib.resolve(cfg.LAB_WORKSPACE, cfg.LH_VORDER, "Lakehouse")

db = lib.load_database(fabric.get_tmsl(cfg.BASE_MODEL, workspace=prod_ws))
targets = {"A": (prod_ws, mirror_id), "B": (lab_ws, lh_b), "C": (lab_ws, lh_c)}
names = {v: lib.variant_name(cfg.VARIANT_PREFIX, v, cfg.BASE_MODEL) for v in lib.VARIANTS}

# %%
for v, (ws_id, item_id) in targets.items():
    lib.run_tmsl(lib.clone_database(db, names[v], ws_id, item_id), cfg.LAB_WORKSPACE)
    try:
        lib.run_tmsl(lib.refresh_script(names[v], "full"), cfg.LAB_WORKSPACE)
        print(f"{v}: {names[v]} deployed and framed")
    except Exception as e:
        # A freshly deployed Direct Lake model sometimes needs its cloud connection bound once
        # (model settings > Gateway and cloud connections), then rerun this cell.
        print(f"{v}: {names[v]} deployed, framing failed: {e}")

# %% [markdown]
# ## Sanity check: same rows everywhere
# A reads the latest mirrored version; if it differs, Databricks committed after 02 ran. Re-run 02 in a
# quiet window.

# %%
tables = [t["name"] for t in db["model"]["tables"]
          if any(p.get("source", {}).get("type") == "entity" for p in t.get("partitions", []))]
counts_dax = "EVALUATE UNION(" + ", ".join(
    f'ROW("table", "{t.replace(chr(34), chr(34) * 2)}", "rows", COUNTROWS({lib.dax_table(t)}))' for t in tables) + ")"
counts = pd.concat([lib.dax(names[v], counts_dax, cfg.LAB_WORKSPACE).assign(variant=v) for v in lib.VARIANTS])
pivot = counts.pivot(index="table", columns="variant", values="rows")
pivot["match"] = pivot.nunique(axis=1) == 1
display(pivot)
