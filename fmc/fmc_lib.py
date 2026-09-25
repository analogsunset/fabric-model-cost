"""Shared helpers for the Fabric model-cost lab.

Pure helpers (TMSL parsing, DAX generation, query tagging, cost math) have no
Fabric dependencies and are unit-tested locally (tests/test_fmc_lib.py).
Fabric helpers import sempy / notebookutils lazily so this module imports
anywhere.
"""
import copy
import json
import re
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# TMSL parsing
# ---------------------------------------------------------------------------

ONELAKE_RE = re.compile(r'onelake\.dfs\.fabric\.microsoft\.com/([^/"\s]+)/([^/"\s]+)', re.I)
SQL_RE = re.compile(r'Sql\.Database\(\s*"([^"]+)"\s*,\s*"([^"]+)"', re.I)


def load_database(tmsl):
    """Return the TMSL database object from a get_tmsl() string/dict or a create/alter script."""
    obj = json.loads(tmsl) if isinstance(tmsl, str) else tmsl
    for key in ("createOrReplace", "create", "alter"):
        if key in obj:
            obj = obj[key]
            break
    return obj.get("database", obj) if "model" not in obj else obj


def expression_text(obj):
    expr = obj.get("expression", "")
    return "\n".join(expr) if isinstance(expr, list) else (expr or "")


def classify_expression(text):
    """Where a Direct Lake / M expression points: onelake, sql_endpoint or other."""
    if "AzureStorage.DataLake" in text and ONELAKE_RE.search(text):
        return "onelake"
    if SQL_RE.search(text):
        return "sql_endpoint"
    return "other"


def expression_ids(text):
    """(workspace, item) for OneLake expressions; (server, database) for Sql.Database."""
    m = ONELAKE_RE.search(text) or SQL_RE.search(text)
    return (m.group(1), m.group(2)) if m else (None, None)


def describe_partitions(db):
    """One row per partition with its effective storage mode and source."""
    model = db.get("model", {})
    default_mode = model.get("defaultMode", "import")
    exprs = {e["name"]: expression_text(e) for e in model.get("expressions", [])}
    rows = []
    for t in model.get("tables", []):
        for p in t.get("partitions", []):
            src = p.get("source", {})
            mode = p.get("mode", "default")
            if mode == "default":
                mode = default_mode
            expr_name = src.get("expressionSource")
            text = exprs.get(expr_name, "") if expr_name else expression_text(src)
            id1, id2 = expression_ids(text)
            rows.append({
                "table": t["name"],
                "partition": p.get("name"),
                "mode": mode,
                "source_type": src.get("type"),
                "schema_name": src.get("schemaName"),
                "entity_name": src.get("entityName"),
                "expression_source": expr_name,
                "source_kind": classify_expression(text),
                "source_id1": id1,
                "source_id2": id2,
            })
    return rows


def model_mode(partition_rows):
    modes = {r["mode"] for r in partition_rows if r["source_type"] not in ("calculated", "calculationGroup")}
    if not modes:
        return "unknown"
    return modes.pop() if len(modes) == 1 else "composite"


# ---------------------------------------------------------------------------
# Cloning a Direct Lake model onto another OneLake item
# ---------------------------------------------------------------------------

def onelake_expression(ws_id, item_id):
    return [
        "let",
        f'    Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/{ws_id}/{item_id}", [HierarchicalNavigation=true])',
        "in",
        "    Source",
    ]


def clone_database(db, new_name, ws_id, item_id):
    """createOrReplace script for a copy of `db` whose entity partitions read Direct Lake on OneLake from item_id.

    Entity (schema/table) names are kept, so the target item must expose the same
    Tables/<schema>/<table> layout as the original source.
    """
    new = copy.deepcopy(db)
    new["name"] = new_name
    new.pop("id", None)
    model = new["model"]
    sources = set()
    for t in model.get("tables", []):
        for p in t.get("partitions", []):
            src = p.get("source", {})
            if src.get("type") == "entity":
                sources.add(src.get("expressionSource"))
                p["mode"] = "directLake"
    if not sources:
        raise ValueError(f"{db.get('name')}: no entity partitions; not a Direct Lake model")
    for e in model.get("expressions", []):
        if e["name"] in sources:
            e["expression"] = onelake_expression(ws_id, item_id)
    return {"createOrReplace": {"object": {"database": new_name}, "database": new}}


VARIANTS = ("A", "B", "C")
VARIANT_LABELS = {
    "A": "mirrored as-is (Databricks-written)",
    "B": "Fabric copy, V-Order off",
    "C": "Fabric copy, V-Order on",
}


def variant_name(prefix, variant, base_model):
    return f"{prefix}{variant}_{base_model}"


def refresh_script(database, refresh_type):
    return {"refresh": {"type": refresh_type, "objects": [{"database": database}]}}


# ---------------------------------------------------------------------------
# DAX generation and tagging
# ---------------------------------------------------------------------------

def dax_table(name):
    return "'" + name.replace("'", "''") + "'"


def dax_column(table, column):
    return f"{dax_table(table)}[{column.replace(']', ']]')}]"


def column_touch_queries(db):
    """{table: DAX} that forces every column of each Direct Lake table into memory."""
    out = {}
    for t in db.get("model", {}).get("tables", []):
        parts = t.get("partitions", [])
        if not parts or any(p.get("source", {}).get("type") != "entity" for p in parts):
            continue
        cols = [c["name"] for c in t.get("columns", [])
                if c.get("type", "data") == "data"]
        if not cols:
            continue
        items = ", ".join(f'"c{i}", DISTINCTCOUNT({dax_column(t["name"], c)})' for i, c in enumerate(cols))
        out[t["name"]] = f"EVALUATE ROW({items})"
    return out


TAG_RE = re.compile(r"/\* fmc:(.+?) \*/")


def tag_query(dax, run_id, iteration, variant, phase, key):
    """Prefix a DAX query with a marker comment so its QueryEnd log event can be attributed exactly."""
    key = key.replace("*/", "* /")
    return f"/* fmc:{run_id}|{iteration}|{variant}|{phase}|{key} */\n{dax}"


def parse_tag(event_text):
    m = TAG_RE.search(event_text or "")
    if not m:
        return None
    run_id, iteration, variant, phase, key = m.group(1).split("|", 4)
    return {"run_id": run_id, "iteration": int(iteration), "variant": variant, "phase": phase, "key": key}


# ---------------------------------------------------------------------------
# Reframe estimation and cost math
# ---------------------------------------------------------------------------

def reframes_per_day(commit_times, days, bucket_minutes=5):
    """Estimate Direct Lake reframes/day from Delta commit times.

    Auto-sync detects changes on a polling interval, so commits landing in the
    same bucket collapse into one reframe.
    """
    if days <= 0:
        return 0.0
    buckets = {int(t.replace(tzinfo=t.tzinfo or timezone.utc).timestamp() // (bucket_minutes * 60))
               for t in commit_times}
    return len(buckets) / days


def daily_delta(base, cand, reframes_by_table, queries_per_day,
                maintenance_cu_s_per_sync=0.0, syncs_per_day=0.0,
                extra_storage_gb=0.0, cu_per_cpu_s=None,
                price_per_cu_hour=None, onelake_price_gb_month=0.023):
    """Daily savings of `cand` (e.g. V-Ordered copy) over `base` (as-is mirrored tables).

    base / cand: {"transcode_cpu_s": {table: s}, "warm_cpu_s_per_query": s}
    Positive saved_* means cand is cheaper on the query side.
    """
    transcode_saved = sum(
        (base["transcode_cpu_s"].get(t, 0.0) - cand["transcode_cpu_s"].get(t, 0.0)) * n
        for t, n in reframes_by_table.items())
    warm_saved = (base["warm_cpu_s_per_query"] - cand["warm_cpu_s_per_query"]) * queries_per_day
    out = {
        "transcode_cpu_s_saved": transcode_saved,
        "warm_query_cpu_s_saved": warm_saved,
        "query_cpu_s_saved": transcode_saved + warm_saved,
        "maintenance_cu_s": maintenance_cu_s_per_sync * syncs_per_day,
        "storage_usd": extra_storage_gb * onelake_price_gb_month / 30.0,
        "query_cu_s_saved": None,
        "net_cu_s": None,
        "net_usd": None,
    }
    if cu_per_cpu_s is not None:
        out["query_cu_s_saved"] = out["query_cpu_s_saved"] * cu_per_cpu_s
        out["net_cu_s"] = out["query_cu_s_saved"] - out["maintenance_cu_s"]
        if price_per_cu_hour is not None:
            out["net_usd"] = out["net_cu_s"] / 3600.0 * price_per_cu_hour - out["storage_usd"]
    return out


def utcnow():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Fabric helpers (run inside a Fabric notebook)
# ---------------------------------------------------------------------------

def abfss(ws_id, item_id, *parts):
    return f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{item_id}/" + "/".join(parts)


def table_path(ws_id, item_id, schema, table):
    return abfss(ws_id, item_id, "Tables", *([schema] if schema else []), table)


def resolve(workspace, item=None, item_type=None):
    """workspace id, or (workspace id, item id)."""
    import sempy.fabric as fabric
    ws_id = fabric.resolve_workspace_id(workspace)
    if item is None:
        return ws_id
    return ws_id, fabric.resolve_item_id(item, type=item_type, workspace=ws_id)


def strip_brackets(df):
    """INFO.* / DAX results come back as '[Col]' or 'Table[Col]'; normalize to 'Col'."""
    df.columns = [re.sub(r"^.*\[(.*)\]$", r"\1", str(c)) for c in df.columns]
    return df


def dax(dataset, query, workspace):
    import sempy.fabric as fabric
    return strip_brackets(fabric.evaluate_dax(dataset, query, workspace=workspace))


def run_tmsl(script, workspace):
    import sempy.fabric as fabric
    return fabric.execute_tmsl(script=json.dumps(script), workspace=workspace)


def memory_stats(dataset, workspace):
    """Resident segment bytes, dictionary bytes and resident/total segment counts."""
    seg = dax(dataset, "EVALUATE INFO.STORAGETABLECOLUMNSEGMENTS()", workspace)
    cols = dax(dataset, "EVALUATE INFO.STORAGETABLECOLUMNS()", workspace)
    resident = seg["ISRESIDENT"].astype(str).str.lower().isin(["true", "1"]) if "ISRESIDENT" in seg else None
    return {
        "segment_used_bytes": int(seg["USED_SIZE"].sum()) if "USED_SIZE" in seg else None,
        "dictionary_bytes": int(cols["DICTIONARY_SIZE"].sum()) if "DICTIONARY_SIZE" in cols else None,
        "segments": len(seg),
        "segments_resident": int(resident.sum()) if resident is not None else None,
    }


def kql(spark, query_uri, database, query):
    """Run KQL against an Eventhouse (e.g. workspace monitoring) and return pandas."""
    import notebookutils
    token = notebookutils.credentials.getToken(query_uri)
    return (spark.read.format("com.microsoft.kusto.spark.synapse.datasource")
            .option("accessToken", token)
            .option("kustoCluster", query_uri)
            .option("kustoDatabase", database)
            .option("kustoQuery", query)
            .load()
            .toPandas())


def save(spark, pdf, table, mode="append"):
    if pdf is None or len(pdf) == 0:
        return
    pdf = pdf.copy()
    for c in pdf.columns:  # Spark can't infer a type for all-null columns
        if pdf[c].isna().all():
            pdf[c] = ""
    (spark.createDataFrame(pdf).write.mode(mode)
     .option("mergeSchema", "true").saveAsTable(table))
