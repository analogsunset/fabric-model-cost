"""Edit for your tenant, then upload with fmc_lib.py to the results lakehouse under Files/fmc/."""

# --- Production: where the models and the mirrored Databricks catalog live ---
PROD_WORKSPACE = "Analytics Prod"
MIRRORED_ITEM = "dbx_catalog"            # mirrored Azure Databricks catalog item

# --- Lab: small dedicated capacity so CU numbers aren't polluted ---
LAB_WORKSPACE = "Model Cost Lab"
RESULTS_LAKEHOUSE = "cost_lab"           # attach as the default lakehouse in every notebook
LH_NO_VORDER = "lab_novorder"            # variant B (schema-enabled lakehouse)
LH_VORDER = "lab_vorder"                 # variant C (schema-enabled lakehouse)

# --- Experiment subject: a Direct Lake model over MIRRORED_ITEM ---
BASE_MODEL = "Sales"
VARIANT_PREFIX = "vo_"                   # clones are vo_A_Sales, vo_B_Sales, vo_C_Sales

# Keep B and C physically identical except for V-Order.
COPY_REPARTITION = None                  # None = Spark default; int = repartition(n) for both B and C

# --- Workspace monitoring (Eventhouse) ---
# Lab: records the experiment's queries. Prod: source of the real DAX workload.
LAB_MONITORING_URI = "https://<lab-eventhouse>.kusto.fabric.microsoft.com"
LAB_MONITORING_DB = "Monitoring KQL database"
PROD_MONITORING_URI = "https://<prod-eventhouse>.kusto.fabric.microsoft.com"
PROD_MONITORING_DB = "Monitoring KQL database"
WORKLOAD_LOOKBACK_DAYS = 7
WORKLOAD_TOP_QUERIES = 200
WORKLOAD_EXECUTIONS = 300                # weighted sample replayed per variant per iteration

# --- Run shape ---
ITERATIONS = 7
PHASE_GAP_SEC = 5
LOG_INGEST_WAIT_SEC = 600                # workspace monitoring lag before 05 reads the logs
FOOTER_SAMPLE_FILES = 20                 # parquet footers inspected per table/variant
COMMIT_LOOKBACK_DAYS = 30
REFRAME_BUCKET_MINUTES = 5

# --- Cost conversion ---
# CU-seconds per semantic-model CPU-second. Calibrate: run 05_vorder_report, then compare the
# lab models' CU in the Capacity Metrics app over the run window with the summed CpuTimeMs.
CU_PER_CPU_SECOND = None
PRICE_PER_CU_HOUR = 0.18                 # PAYG list; use your region / reservation rate
ONELAKE_PRICE_GB_MONTH = 0.023
# Override the Spark maintenance estimate with a Capacity Metrics figure for 02_vorder_copy (variant C only).
VORDER_COPY_CU_SECONDS = None
# How often you'd re-sync a V-Ordered copy in production; None = once per estimated reframe.
COPY_SYNCS_PER_DAY = None
