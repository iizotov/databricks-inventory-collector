# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Inventory Collector
# MAGIC **Read-only workspace inventory and audit tool**
# MAGIC
# MAGIC This notebook enumerates all objects in a Databricks workspace and writes structured JSON output
# MAGIC for inventory, audit, compliance, capacity planning, or architectural review purposes.
# MAGIC All operations are strictly **read-only**.
# MAGIC
# MAGIC ### What it collects
# MAGIC | Group | Objects |
# MAGIC |---|---|
# MAGIC | **Compute** | Clusters, pools, policies, init scripts, instance profiles, libraries |
# MAGIC | **Unity Catalog** | Catalogs, schemas, tables/views/functions/volumes + grants at every level, credentials, connections |
# MAGIC | **Workflows** | Jobs, DLT pipelines, recent run history |
# MAGIC | **Workspace** | Notebook tree (paths, languages), repos |
# MAGIC | **SQL Analytics** | SQL warehouses, saved queries (with full SQL), dashboards (legacy + Lakeview), alerts, query history |
# MAGIC | **ML** | MLflow experiments + runs (params/metrics), registered models (UC + legacy), feature engineering |
# MAGIC | **Security** | Users, groups, service principals, tokens, secret scopes/ACLs, workspace config |
# MAGIC | **Serving** | Model serving endpoints, vector search, apps |
# MAGIC | **Sharing** | Delta Sharing providers/recipients/shares, clean rooms |
# MAGIC | **Quality** | Notification destinations |
# MAGIC | **Usage** | Cluster events, policy compliance |
# MAGIC
# MAGIC ### Output
# MAGIC Default: `/tmp/inventory_collector` (driver local). For persistent storage set `output_path` to a
# MAGIC Unity Catalog Volume, e.g. `/Volumes/<catalog>/<schema>/<volume>/inventory_collector`.
# MAGIC
# MAGIC ### Resume
# MAGIC Writes `.checkpoint.json`. If interrupted, re-run and completed collectors are skipped.
# MAGIC
# MAGIC ### Disclaimer
# MAGIC This software is provided "as is", without warranty of any kind, express or implied. Use at your
# MAGIC own risk. This project is **not affiliated with, endorsed by, or associated with Amazon Web Services
# MAGIC (AWS), Databricks, or any of their affiliates**. See LICENSE file for full terms.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.40.0" --upgrade --no-deps --quiet

# COMMAND ----------

# Verify SDK version (pip cell above auto-restarts Python)
try:
    from importlib.metadata import version as _pkg_version
    print(f"databricks-sdk version: {_pkg_version('databricks-sdk')}")
except Exception:
    print("databricks-sdk version: unknown")

# COMMAND ----------

# Widget configuration
dbutils.widgets.text("output_path", "/tmp/inventory_collector", "Output path")
dbutils.widgets.dropdown("history_days", "30", ["7", "14", "30", "60", "90"], "History window (days)")
dbutils.widgets.dropdown("max_workers", "4", ["1", "2", "4", "8", "12"], "Parallel workers")
dbutils.widgets.multiselect(
    "collectors", "all",
    ["all", "compute", "unity_catalog", "workflows", "workspace",
     "sql_analytics", "ml", "security", "serving", "sharing", "quality", "usage"],
    "Collector groups"
)

# COMMAND ----------

# ============================================================================
# ALL DEFINITIONS — utilities, base collector, all 11 collectors, runner
# ============================================================================

import json
import os
import time
import logging
import hashlib
import tempfile
from abc import ABC
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

ALL_COLLECTOR_GROUPS = [
    "compute", "unity_catalog", "workflows", "workspace",
    "sql_analytics", "ml", "security", "serving", "sharing", "quality", "usage",
]

@dataclass
class Settings:
    output_dir: str
    history_days: int
    max_workers: int
    enabled_collectors: list

def _parse_widgets():
    raw = dbutils.widgets.get("collectors").split(",")
    enabled = list(ALL_COLLECTOR_GROUPS) if "all" in raw else [c.strip() for c in raw if c.strip() in ALL_COLLECTOR_GROUPS]
    return Settings(
        output_dir=dbutils.widgets.get("output_path"),
        history_days=int(dbutils.widgets.get("history_days")),
        max_workers=int(dbutils.widgets.get("max_workers")),
        enabled_collectors=enabled,
    )

settings = _parse_widgets()
print(f"Output:     {settings.output_dir}")
print(f"History:    {settings.history_days} days")
print(f"Workers:    {settings.max_workers}")
print(f"Collectors: {', '.join(settings.enabled_collectors)}")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s", datefmt="%H:%M:%S")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("databricks.sdk").setLevel(logging.WARNING)
logger = logging.getLogger("collector")

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

class SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, "as_dict"):
            return obj.as_dict()
        if hasattr(obj, "__dataclass_fields__"):
            return {k: getattr(obj, k) for k in obj.__dataclass_fields__}
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

def to_dict(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    return obj

def to_dict_list(iterator):
    return [to_dict(item) for item in iterator]

def atomic_write(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path) or ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, cls=SafeEncoder)
            f.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

def safe_api_call(func, *args, default=None, label=None, **kwargs):
    call_label = label or getattr(func, "__qualname__", str(func))
    try:
        return func(*args, **kwargs), None
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e)[:500]}"
        logger.warning(f"API call failed [{call_label}]: {msg}")
        return default, msg

def safe_list_all(func, *args, label=None, **kwargs):
    call_label = label or getattr(func, "__qualname__", str(func))
    try:
        return to_dict_list(func(*args, **kwargs)), None
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e)[:500]}"
        logger.warning(f"List call failed [{call_label}]: {msg}")
        return [], msg

# ---------------------------------------------------------------------------
# Base collector
# ---------------------------------------------------------------------------

class BaseCollector(ABC):
    name: str = ""
    description: str = ""

    def __init__(self, client: WorkspaceClient, settings: Settings):
        self.client = client
        self.settings = settings
        self.output_dir = os.path.join(settings.output_dir, self.name)
        self.errors: list = []
        self._stats: dict = {}

    def collect(self) -> dict:
        raise NotImplementedError

    def _save(self, filename, data):
        path = os.path.join(self.output_dir, filename)
        atomic_write(path, data)
        return path

    def _record_error(self, context, error):
        self.errors.append({"context": context, "error": error})

    def _safe_list(self, func, *args, label=None, **kwargs):
        lbl = label or f"{self.name}.{getattr(func, '__name__', '?')}"
        items, err = safe_list_all(func, *args, label=lbl, **kwargs)
        if err:
            self._record_error(lbl, err)
        return items

    def _safe_get(self, func, *args, label=None, **kwargs):
        lbl = label or f"{self.name}.{getattr(func, '__name__', '?')}"
        result, err = safe_api_call(func, *args, label=lbl, **kwargs)
        if err:
            self._record_error(lbl, err)
            return None
        return to_dict(result)

    def _try_list(self, api_ref, method_name, *args, label=None, **kwargs):
        """Like _safe_list but resolves API + method by name so missing APIs
        (on older SDK versions or different workspace tiers) are caught gracefully.

        api_ref: either an API object (e.g. self.client.apps) or a string
                 attribute name (e.g. "notification_destinations") resolved on self.client.
        """
        lbl = label or f"{self.name}.{method_name}"
        if isinstance(api_ref, str):
            api_obj = getattr(self.client, api_ref, None)
            if api_obj is None:
                self._record_error(lbl, f"API '{api_ref}' not available in this SDK version")
                return []
        else:
            api_obj = api_ref
        fn = getattr(api_obj, method_name, None)
        if fn is None:
            self._record_error(lbl, f"Method '{method_name}' not available in this SDK version")
            return []
        return self._safe_list(fn, *args, label=lbl, **kwargs)

    def _try_get(self, api_ref, method_name, *args, label=None, **kwargs):
        """Like _safe_get but resolves API + method by name."""
        lbl = label or f"{self.name}.{method_name}"
        if isinstance(api_ref, str):
            api_obj = getattr(self.client, api_ref, None)
            if api_obj is None:
                self._record_error(lbl, f"API '{api_ref}' not available in this SDK version")
                return None
        else:
            api_obj = api_ref
        fn = getattr(api_obj, method_name, None)
        if fn is None:
            self._record_error(lbl, f"Method '{method_name}' not available in this SDK version")
            return None
        return self._safe_get(fn, *args, label=lbl, **kwargs)

    def get_summary(self):
        summary = {"name": self.name, "description": self.description}
        summary.update(self._stats)
        if self.errors:
            summary["errors"] = self.errors
            summary["error_count"] = len(self.errors)
        return summary

# ---------------------------------------------------------------------------
# Compute collector
# ---------------------------------------------------------------------------

class ComputeCollector(BaseCollector):
    name = "compute"
    description = "Clusters, instance pools, cluster policies, global init scripts, instance profiles"

    def collect(self):
        clusters = self._collect_clusters()
        policies = self._safe_list(self.client.cluster_policies.list, label="cluster_policies.list")
        pools = self._safe_list(self.client.instance_pools.list, label="instance_pools.list")
        init_scripts = self._safe_list(self.client.global_init_scripts.list, label="global_init_scripts.list")
        instance_profiles = self._safe_list(self.client.instance_profiles.list, label="instance_profiles.list")
        libraries = self._collect_cluster_libraries(clusters)
        self._save("clusters.json", clusters)
        self._save("cluster_policies.json", policies)
        self._save("instance_pools.json", pools)
        self._save("global_init_scripts.json", init_scripts)
        self._save("instance_profiles.json", instance_profiles)
        self._save("cluster_libraries.json", libraries)
        families = self._safe_list(self.client.policy_families.list, label="policy_families.list")
        if families:
            self._save("policy_families.json", families)
        self._stats = {
            "clusters": len(clusters), "cluster_policies": len(policies),
            "instance_pools": len(pools), "global_init_scripts": len(init_scripts),
            "instance_profiles": len(instance_profiles),
        }

    def _collect_clusters(self):
        clusters = self._safe_list(self.client.clusters.list, label="clusters.list")
        detailed = []
        for c in clusters:
            cid = c.get("cluster_id")
            if cid:
                d = self._safe_get(self.client.clusters.get, cluster_id=cid, label=f"clusters.get({cid})")
                detailed.append(d or c)
            else:
                detailed.append(c)
        return detailed

    def _collect_cluster_libraries(self, clusters):
        libraries = {}
        for c in clusters:
            cid = c.get("cluster_id")
            if cid:
                s = self._safe_get(self.client.libraries.cluster_status, cluster_id=cid, label=f"libraries({cid})")
                if s:
                    libraries[cid] = s
        return libraries

# ---------------------------------------------------------------------------
# Unity Catalog collector
# ---------------------------------------------------------------------------

class UnityCatalogCollector(BaseCollector):
    name = "unity_catalog"
    description = "Catalogs, schemas, tables, volumes, functions, grants, credentials, connections"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._catalog_cp_path = os.path.join(self.output_dir, ".catalog_checkpoint.json")
        self._completed_catalogs = self._load_catalog_cp()

    def _load_catalog_cp(self):
        if os.path.exists(self._catalog_cp_path):
            try:
                with open(self._catalog_cp_path) as f:
                    return set(json.load(f).get("completed", []))
            except Exception:
                pass
        return set()

    def _save_catalog_cp(self):
        atomic_write(self._catalog_cp_path, {"completed": sorted(self._completed_catalogs)})

    def collect(self):
        metastores = self._safe_list(self.client.metastores.list, label="metastores.list")
        self._save("metastores.json", metastores)
        catalogs = self._safe_list(self.client.catalogs.list, label="catalogs.list")
        self._save("catalogs.json", catalogs)
        storage_creds = self._safe_list(self.client.storage_credentials.list, label="storage_credentials.list")
        self._save("storage_credentials.json", storage_creds)
        ext_locations = self._safe_list(self.client.external_locations.list, label="external_locations.list")
        self._save("external_locations.json", ext_locations)
        connections = self._safe_list(self.client.connections.list, label="connections.list")
        self._save("connections.json", connections)
        for ms in metastores:
            ms_id = ms.get("metastore_id")
            if ms_id:
                sys_schemas = self._safe_list(self.client.system_schemas.list, metastore_id=ms_id, label=f"system_schemas({ms_id})")
                if sys_schemas:
                    self._save("system_schemas.json", sys_schemas)
        bindings = []
        for cat in catalogs:
            cn = cat.get("name")
            if cn:
                r = self._safe_get(self.client.workspace_bindings.get_bindings, securable_type="catalog", securable_name=cn, label=f"workspace_bindings({cn})")
                if r:
                    bindings.append({"catalog": cn, "bindings": r})
        if bindings:
            self._save("workspace_bindings.json", bindings)

        cat_names = [c.get("name") for c in catalogs if c.get("name")]
        totals = {"schemas": 0, "tables": 0, "views": 0, "functions": 0, "volumes": 0}
        workers = min(self.settings.max_workers, len(cat_names) or 1)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for cn in cat_names:
                if cn in self._completed_catalogs:
                    logger.info(f"Skipping completed catalog: {cn}")
                    continue
                futures[pool.submit(self._collect_catalog, cn)] = cn
            for future in as_completed(futures):
                cn = futures[future]
                try:
                    stats = future.result()
                    for k in totals:
                        totals[k] += stats.get(k, 0)
                    self._completed_catalogs.add(cn)
                    self._save_catalog_cp()
                except Exception as e:
                    self._record_error(f"catalog({cn})", f"{type(e).__name__}: {e}")
        self._stats = {
            "metastores": len(metastores), "catalogs": len(catalogs), **totals,
            "storage_credentials": len(storage_creds),
            "external_locations": len(ext_locations), "connections": len(connections),
        }

    def _collect_catalog(self, catalog_name):
        logger.info(f"Collecting catalog: {catalog_name}")
        cat_data = {"catalog_name": catalog_name, "grants": self._get_grants("catalog", catalog_name), "schemas": []}
        schemas_raw = self._safe_list(self.client.schemas.list, catalog_name=catalog_name, label=f"schemas.list({catalog_name})")
        stats = {"schemas": 0, "tables": 0, "views": 0, "functions": 0, "volumes": 0}
        for sr in schemas_raw:
            sn = sr.get("name", "")
            full_schema = f"{catalog_name}.{sn}"
            stats["schemas"] += 1
            schema_data = {**sr, "grants": self._get_grants("schema", full_schema), "tables": [], "functions": [], "volumes": []}
            tables_raw = self._safe_list(self.client.tables.list, catalog_name=catalog_name, schema_name=sn, label=f"tables.list({full_schema})")
            for t in tables_raw:
                tt = t.get("table_type", "")
                full_table = t.get("full_name", f"{full_schema}.{t.get('name', '')}")
                if tt == "VIEW":
                    stats["views"] += 1
                else:
                    stats["tables"] += 1
                t["grants"] = self._get_grants("table", full_table)
            schema_data["tables"] = tables_raw
            fns = self._safe_list(self.client.functions.list, catalog_name=catalog_name, schema_name=sn, label=f"functions.list({full_schema})")
            schema_data["functions"] = fns
            stats["functions"] += len(fns)
            vols = self._safe_list(self.client.volumes.list, catalog_name=catalog_name, schema_name=sn, label=f"volumes.list({full_schema})")
            for v in vols:
                fv = v.get("full_name", f"{full_schema}.{v.get('name', '')}")
                v["grants"] = self._get_grants("volume", fv)
            schema_data["volumes"] = vols
            stats["volumes"] += len(vols)
            cat_data["schemas"].append(schema_data)
        safe_name = catalog_name.replace("/", "_").replace("\\", "_")
        self._save(f"catalog__{safe_name}.json", cat_data)
        logger.info(f"Catalog {catalog_name}: {stats}")
        return stats

    def _get_grants(self, securable_type, full_name):
        # Use the REST API directly — the SDK's SecurableType enum serialises
        # incorrectly (SECURABLETYPE.CATALOG) while the API wants lowercase "catalog".
        result, err = safe_api_call(
            self.client.api_client.do,
            "GET",
            f"/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}",
            label=f"grants.get({securable_type},{full_name})",
        )
        if err:
            if "PERMISSION_DENIED" not in err and "NOT_FOUND" not in err:
                self._record_error(f"grants({full_name})", err)
            return []
        if isinstance(result, dict):
            return result.get("privilege_assignments", [])
        return []

# ---------------------------------------------------------------------------
# Workflows & Workspace collectors
# ---------------------------------------------------------------------------

class WorkflowsCollector(BaseCollector):
    name = "workflows"
    description = "Jobs, DLT pipelines, and recent run history"

    def collect(self):
        jobs = self._safe_list(self.client.jobs.list, expand_tasks=True, label="jobs.list")
        self._save("jobs.json", jobs)
        cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=self.settings.history_days)).timestamp() * 1000)
        runs = []
        try:
            for run in self.client.jobs.list_runs(expand_tasks=False, start_time_from=cutoff_ms):
                runs.append(to_dict(run))
        except Exception as e:
            self._record_error("jobs.list_runs", f"{type(e).__name__}: {e}")
        self._save("job_runs.json", runs)
        pipelines_raw = self._safe_list(self.client.pipelines.list_pipelines, label="pipelines.list_pipelines")
        pipelines = []
        for p in pipelines_raw:
            pid = p.get("pipeline_id")
            if pid:
                d = self._safe_get(self.client.pipelines.get, pipeline_id=pid, label=f"pipelines.get({pid})")
                pipelines.append(d or p)
            else:
                pipelines.append(p)
        self._save("pipelines.json", pipelines)
        self._stats = {"jobs": len(jobs), "job_runs": len(runs), "pipelines": len(pipelines)}


class WorkspaceCollector(BaseCollector):
    name = "workspace"
    description = "Notebook tree (paths, languages, types), repos"

    def collect(self):
        tree = self._collect_tree()
        self._save("workspace_tree.json", tree)
        repos = self._safe_list(self.client.repos.list, label="repos.list")
        self._save("repos.json", repos)
        git_creds = self._safe_list(self.client.git_credentials.list, label="git_credentials.list")
        self._save("git_credentials.json", git_creds)
        stats = {"notebooks": 0, "directories": 0, "files": 0, "libraries": 0}
        languages = {}
        for item in tree:
            ot = item.get("object_type", "")
            if ot == "NOTEBOOK":
                stats["notebooks"] += 1
                lang = item.get("language", "UNKNOWN")
                languages[lang] = languages.get(lang, 0) + 1
            elif ot == "DIRECTORY":
                stats["directories"] += 1
            elif ot == "FILE":
                stats["files"] += 1
            elif ot == "LIBRARY":
                stats["libraries"] += 1
        if languages:
            self._save("notebook_languages.json", languages)
        self._stats = {**stats, "repos": len(repos), "git_credentials": len(git_creds)}

    def _collect_tree(self):
        all_objects, dirs_to_visit, visited = [], ["/"], set()
        while dirs_to_visit and len(all_objects) < 500_000:
            path = dirs_to_visit.pop()
            if path in visited:
                continue
            visited.add(path)
            try:
                for obj in self.client.workspace.list(path=path):
                    item = to_dict(obj)
                    all_objects.append(item)
                    if item.get("object_type") == "DIRECTORY" and item.get("path") and item["path"] not in visited:
                        dirs_to_visit.append(item["path"])
            except Exception as e:
                self._record_error(f"workspace.list({path})", f"{type(e).__name__}: {e}")
            if len(all_objects) % 5000 == 0 and all_objects:
                logger.info(f"Workspace tree: {len(all_objects)} objects, {len(dirs_to_visit)} dirs remaining")
        return all_objects

# ---------------------------------------------------------------------------
# SQL Analytics collector
# ---------------------------------------------------------------------------

class SqlAnalyticsCollector(BaseCollector):
    name = "sql_analytics"
    description = "SQL warehouses, saved queries, dashboards, alerts, query history"

    def collect(self):
        warehouses = self._safe_list(self.client.warehouses.list, label="warehouses.list")
        detailed_wh = []
        for wh in warehouses:
            wid = wh.get("id")
            if wid:
                d = self._safe_get(self.client.warehouses.get, id=wid, label=f"warehouses.get({wid})")
                detailed_wh.append(d or wh)
            else:
                detailed_wh.append(wh)
        self._save("warehouses.json", detailed_wh)
        queries = self._safe_list(self.client.queries.list, label="queries.list")
        # Enrich with full query details (SQL text, visualizations, schedule)
        detailed_queries = []
        for q in queries:
            qid = q.get("id")
            if qid:
                d = self._safe_get(self.client.queries.get, id=qid, label=f"queries.get({qid})")
                detailed_queries.append(d or q)
            else:
                detailed_queries.append(q)
        self._save("queries.json", detailed_queries)
        dashboards = self._safe_list(self.client.dashboards.list, label="dashboards.list")
        self._save("dashboards_legacy.json", dashboards)
        lakeview = self._try_list("lakeview", "list", label="lakeview.list")
        self._save("dashboards_lakeview.json", lakeview)
        alerts = self._safe_list(self.client.alerts.list, label="alerts.list")
        self._save("alerts.json", alerts)
        data_sources = self._safe_list(self.client.data_sources.list, label="data_sources.list")
        if data_sources:
            self._save("data_sources.json", data_sources)
        qh = self._collect_query_history()
        self._stats = {
            "warehouses": len(detailed_wh), "queries": len(queries),
            "dashboards": len(dashboards), "lakeview_dashboards": len(lakeview),
            "alerts": len(alerts), "query_history_entries": len(qh),
        }

    def _collect_query_history(self):
        from databricks.sdk.service.sql import QueryFilter, TimeRange
        cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=self.settings.history_days)).timestamp() * 1000)
        history, next_token = [], None
        try:
            while True:
                kwargs = dict(filter_by=QueryFilter(query_start_time_range=TimeRange(start_time_ms=cutoff_ms)), include_metrics=False)
                if next_token:
                    kwargs["page_token"] = next_token
                resp = self.client.query_history.list(**kwargs)
                for entry in (resp.res or []):
                    history.append(to_dict(entry))
                if len(history) >= 100_000:
                    self._record_error("query_history", "Hit 100k cap.")
                    break
                if not resp.has_next_page or not resp.next_page_token:
                    break
                next_token = resp.next_page_token
        except Exception as e:
            self._record_error("query_history.list", f"{type(e).__name__}: {e}")
        self._save("query_history.json", history)
        return history

# ---------------------------------------------------------------------------
# ML & Serving collectors
# ---------------------------------------------------------------------------

class MLCollector(BaseCollector):
    name = "ml"
    description = "MLflow experiments, registered models (legacy + UC), feature engineering"

    def collect(self):
        experiments = self._safe_list(self.client.experiments.list_experiments, label="experiments.list_experiments")
        self._save("experiments.json", experiments)

        # Collect runs per experiment (params, metrics, artifacts metadata)
        all_runs = []
        for exp in experiments:
            exp_id = exp.get("experiment_id")
            if not exp_id:
                continue
            try:
                for run in self.client.experiments.search_runs(experiment_ids=[exp_id]):
                    all_runs.append(to_dict(run))
                    if len(all_runs) >= 50_000:
                        self._record_error("experiment_runs", "Hit 50k run cap.")
                        break
            except Exception as e:
                self._record_error(f"search_runs({exp_id})", f"{type(e).__name__}: {e}")
            if len(all_runs) >= 50_000:
                break
        if all_runs:
            self._save("experiment_runs.json", all_runs)

        uc_models = self._safe_list(self.client.registered_models.list, label="registered_models.list")
        self._save("uc_registered_models.json", uc_models)
        all_versions = []
        for m in uc_models:
            fn = m.get("full_name")
            if fn:
                vs = self._safe_list(self.client.model_versions.list, full_name=fn, label=f"model_versions.list({fn})")
                all_versions.extend(vs)
        if all_versions:
            self._save("uc_model_versions.json", all_versions)
        legacy_models = []
        try:
            for m in self.client.model_registry.list_models():
                legacy_models.append(to_dict(m))
            self._save("legacy_registered_models.json", legacy_models)
        except Exception as e:
            self._record_error("model_registry.list", f"{type(e).__name__}: {e}")
        features = self._try_list("feature_engineering", "list_features", label="feature_engineering.list_features")
        if features:
            self._save("features.json", features)
        materialized = self._try_list("feature_engineering", "list_materialized_features", label="feature_engineering.list_materialized_features")
        if materialized:
            self._save("materialized_features.json", materialized)
        online_stores = self._try_list("feature_store", "list_online_stores", label="feature_store.list_online_stores")
        if online_stores:
            self._save("online_stores.json", online_stores)
        self._stats = {
            "experiments": len(experiments), "experiment_runs": len(all_runs),
            "uc_registered_models": len(uc_models),
            "uc_model_versions": len(all_versions), "legacy_registered_models": len(legacy_models),
            "features": len(features) + len(materialized),
        }


class ServingCollector(BaseCollector):
    name = "serving"
    description = "Model serving endpoints, vector search endpoints/indexes, apps"

    def collect(self):
        endpoints = self._safe_list(self.client.serving_endpoints.list, label="serving_endpoints.list")
        self._save("serving_endpoints.json", endpoints)
        vs_endpoints = self._try_list("vector_search_endpoints", "list_endpoints", label="vector_search_endpoints.list")
        self._save("vector_search_endpoints.json", vs_endpoints)
        vs_indexes = []
        for ep in vs_endpoints:
            name = ep.get("name")
            if name:
                idxs = self._try_list("vector_search_indexes", "list_indexes", endpoint_name=name, label=f"vs_indexes({name})")
                vs_indexes.extend(idxs)
        if vs_indexes:
            self._save("vector_search_indexes.json", vs_indexes)
        apps = self._try_list("apps", "list", label="apps.list")
        self._save("apps.json", apps)
        self._stats = {
            "serving_endpoints": len(endpoints), "vector_search_endpoints": len(vs_endpoints),
            "vector_search_indexes": len(vs_indexes), "apps": len(apps),
        }

# ---------------------------------------------------------------------------
# Security collector
# ---------------------------------------------------------------------------

class SecurityCollector(BaseCollector):
    name = "security"
    description = "Users, groups, service principals, tokens, secrets, permissions, IP access lists"

    def collect(self):
        users = self._safe_list(self.client.users.list, label="users.list")
        groups = self._safe_list(self.client.groups.list, label="groups.list")
        sps = self._safe_list(self.client.service_principals.list, label="service_principals.list")
        self._save("users.json", users)
        self._save("groups.json", groups)
        self._save("service_principals.json", sps)
        tokens = self._safe_list(self.client.token_management.list, label="token_management.list")
        self._save("tokens.json", tokens)
        scopes = self._collect_secrets()
        ip_lists = self._safe_list(self.client.ip_access_lists.list, label="ip_access_lists.list")
        self._save("ip_access_lists.json", ip_lists)
        self._collect_workspace_conf()
        self._collect_group_details(groups)
        self._stats = {
            "users": len(users), "groups": len(groups), "service_principals": len(sps),
            "tokens": len(tokens), "secret_scopes": len(scopes), "ip_access_lists": len(ip_lists),
        }

    def _collect_secrets(self):
        scopes_raw = self._safe_list(self.client.secrets.list_scopes, label="secrets.list_scopes")
        scopes = []
        for s in scopes_raw:
            sn = s.get("name", "")
            if not sn:
                scopes.append(s)
                continue
            keys = self._safe_list(self.client.secrets.list_secrets, scope=sn, label=f"secrets.list_secrets({sn})")
            acls = self._safe_list(self.client.secrets.list_acls, scope=sn, label=f"secrets.list_acls({sn})")
            scopes.append({**s, "secrets": keys, "acls": acls})
        self._save("secret_scopes.json", scopes)
        return scopes

    def _collect_workspace_conf(self):
        keys = [
            "enableDcs", "enableDbfsFileBrowser", "enableExportNotebook",
            "enableNotebookTableClipboard", "enableResultsDownloading",
            "enableTokensConfig", "enableUploadDataUis", "enableVerboseAuditLogs",
            "enableWebTerminal", "enableWorkspaceFilesystem", "maxTokenLifetimeDays",
            "storeInteractiveNotebookResultsInCustomerAccount",
        ]
        conf = {}
        for k in keys:
            r = self._safe_get(self.client.workspace_conf.get_status, keys=k, label=f"workspace_conf.get({k})")
            if r and isinstance(r, dict):
                conf.update(r)
        self._save("workspace_conf.json", conf)

    def _collect_group_details(self, groups):
        details = {}
        for g in groups:
            gid = g.get("id")
            gname = g.get("display_name", gid)
            if gid:
                d = self._safe_get(self.client.groups.get, id=gid, label=f"groups.get({gname})")
                if d:
                    details[gname] = d
        if details:
            self._save("group_details.json", details)

# ---------------------------------------------------------------------------
# Sharing, Quality & Usage collectors
# ---------------------------------------------------------------------------

class SharingCollector(BaseCollector):
    name = "sharing"
    description = "Delta Sharing providers/recipients/shares, clean rooms"

    def collect(self):
        providers = self._safe_list(self.client.providers.list, label="providers.list")
        self._save("providers.json", providers)
        recipients = self._safe_list(self.client.recipients.list, label="recipients.list")
        self._save("recipients.json", recipients)
        # Method name varies across SDK versions: list_shares (newer) vs list (older)
        _shares_fn = getattr(self.client.shares, "list_shares", None) or getattr(self.client.shares, "list", None)
        shares_raw = self._safe_list(_shares_fn, label="shares.list") if _shares_fn else []
        shares = []
        for s in shares_raw:
            sn = s.get("name")
            if sn:
                d = self._safe_get(self.client.shares.get, name=sn, label=f"shares.get({sn})")
                shares.append(d or s)
            else:
                shares.append(s)
        self._save("shares.json", shares)
        rooms = self._safe_list(self.client.clean_rooms.list, label="clean_rooms.list")
        detailed_rooms = []
        for r in rooms:
            rn = r.get("name")
            if rn:
                d = self._safe_get(self.client.clean_rooms.get, name=rn, label=f"clean_rooms.get({rn})")
                if d and hasattr(self.client, "clean_room_assets"):
                    assets = self._try_list("clean_room_assets", "list", clean_room_name=rn, label=f"cr_assets({rn})")
                    d["assets"] = assets
                detailed_rooms.append(d or r)
            else:
                detailed_rooms.append(r)
        self._save("clean_rooms.json", detailed_rooms)
        self._stats = {"providers": len(providers), "recipients": len(recipients), "shares": len(shares), "clean_rooms": len(detailed_rooms)}


class QualityCollector(BaseCollector):
    name = "quality"
    description = "Notification destinations"

    def collect(self):
        destinations = self._try_list("notification_destinations", "list", label="notification_destinations.list")
        self._save("notification_destinations.json", destinations)
        self._stats = {"notification_destinations": len(destinations)}


class UsageCollector(BaseCollector):
    name = "usage"
    description = "Cluster events, policy compliance"

    def collect(self):
        clusters = self._safe_list(self.client.clusters.list, label="clusters.list(for_events)")
        cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=self.settings.history_days)).timestamp() * 1000)
        all_events = []
        for c in clusters:
            cid = c.get("cluster_id")
            if not cid:
                continue
            try:
                for e in self.client.clusters.events(cluster_id=cid, start_time=cutoff_ms):
                    ev = to_dict(e)
                    ev["cluster_id"] = cid
                    ev["cluster_name"] = c.get("cluster_name", "")
                    all_events.append(ev)
            except Exception as e:
                if "INVALID_STATE" not in str(e):
                    self._record_error(f"clusters.events({cid})", f"{type(e).__name__}: {e}")
        self._save("cluster_events.json", all_events)
        compliance = {}
        policies = self._safe_list(self.client.cluster_policies.list, label="cluster_policies.list(compliance)")
        for p in policies:
            pid = p.get("policy_id")
            if pid:
                entries = self._try_list("policy_compliance_for_clusters", "list_compliance", policy_id=pid, label=f"compliance({pid})")
                if entries:
                    compliance[pid] = entries
        if compliance:
            self._save("policy_compliance.json", compliance)
        self._stats = {"cluster_events": len(all_events), "policy_compliance_entries": sum(len(v) for v in compliance.values())}

# ---------------------------------------------------------------------------
# Runner — checkpoint, parallel orchestration, summary
# ---------------------------------------------------------------------------

COLLECTOR_REGISTRY = {
    "compute": ComputeCollector, "unity_catalog": UnityCatalogCollector,
    "workflows": WorkflowsCollector, "workspace": WorkspaceCollector,
    "sql_analytics": SqlAnalyticsCollector, "ml": MLCollector,
    "security": SecurityCollector, "serving": ServingCollector,
    "sharing": SharingCollector, "quality": QualityCollector, "usage": UsageCollector,
}

class Checkpoint:
    def __init__(self, output_dir, settings):
        self.path = os.path.join(output_dir, ".checkpoint.json")
        self.settings_hash = hashlib.sha256(f"{','.join(sorted(settings.enabled_collectors))}".encode()).hexdigest()[:16]
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    return json.load(f)
            except Exception:
                pass
        return self._fresh()

    def _fresh(self):
        return {"version": 1, "settings_hash": self.settings_hash, "started_at": datetime.now(timezone.utc).isoformat(), "collectors": {}}

    def settings_changed(self):
        return self.data.get("settings_hash") != self.settings_hash

    def has_progress(self):
        return any(c.get("status") in ("completed", "in_progress", "failed") for c in self.data.get("collectors", {}).values())

    def is_completed(self, name):
        return self.data["collectors"].get(name, {}).get("status") == "completed"

    def mark_in_progress(self, name):
        self.data["collectors"][name] = {"status": "in_progress", "started_at": datetime.now(timezone.utc).isoformat()}
        self._save()

    def mark_completed(self, name, summary):
        self.data["collectors"][name] = {"status": "completed", "finished_at": datetime.now(timezone.utc).isoformat(), "summary": summary}
        self._save()

    def mark_failed(self, name, error):
        self.data["collectors"][name] = {"status": "failed", "finished_at": datetime.now(timezone.utc).isoformat(), "error": error}
        self._save()

    def reset(self):
        self.data = self._fresh()
        self._save()

    def _save(self):
        atomic_write(self.path, self.data)

    def get_completed_summaries(self):
        return {n: i.get("summary", {}) for n, i in self.data.get("collectors", {}).items() if i.get("status") == "completed"}


def _classify_error(err_str):
    """Classify an API error as 'warn' (not available) vs 'fail' (real problem)."""
    warn_patterns = [
        "not available in the pricing tier",
        "is disabled for the current",
        "not in SDK",
        "not available in this SDK",
        "No API found for",
        "Could not handle RPC class",
        "requires enrollment",
        "Delta Sharing is disabled",
        "Enable external delta sharing",
    ]
    for pat in warn_patterns:
        if pat in err_str:
            return "warn"
    return "fail"


def run_preflight(client, settings):
    """Check API permissions before collection. Returns (results_dict, ok_count, warn_count, fail_count)."""
    checks = [
        # (label, collector_group, callable)
        ("Identity: current_user",         None,              lambda: client.current_user.me()),
        ("Compute: clusters.list",         "compute",         lambda: list(client.clusters.list())),
        ("Compute: cluster_policies.list", "compute",         lambda: list(client.cluster_policies.list())),
        ("Compute: instance_pools.list",   "compute",         lambda: list(client.instance_pools.list())),
        ("UC: catalogs.list",              "unity_catalog",   lambda: list(client.catalogs.list())),
        ("UC: schemas.list",               "unity_catalog",   lambda: list(client.schemas.list(catalog_name="workspace"))),
        ("UC: tables.list",                "unity_catalog",   lambda: list(client.tables.list(catalog_name="workspace", schema_name="default"))),
        ("UC: grants (REST)",              "unity_catalog",   lambda: client.api_client.do("GET", "/api/2.1/unity-catalog/permissions/catalog/workspace")),
        ("UC: storage_credentials.list",   "unity_catalog",   lambda: list(client.storage_credentials.list())),
        ("UC: external_locations.list",    "unity_catalog",   lambda: list(client.external_locations.list())),
        ("UC: metastores.list",            "unity_catalog",   lambda: list(client.metastores.list())),
        ("Workflows: jobs.list",           "workflows",       lambda: list(client.jobs.list())),
        ("Workflows: pipelines.list",      "workflows",       lambda: list(client.pipelines.list_pipelines())),
        ("Workspace: workspace.list(/)",   "workspace",       lambda: list(client.workspace.list(path="/"))),
        ("Workspace: repos.list",          "workspace",       lambda: list(client.repos.list())),
        ("SQL: warehouses.list",           "sql_analytics",   lambda: list(client.warehouses.list())),
        ("SQL: queries.list",              "sql_analytics",   lambda: list(client.queries.list())),
        ("SQL: query_history.list",        "sql_analytics",   lambda: client.query_history.list()),
        ("ML: experiments.list",           "ml",              lambda: list(client.experiments.list_experiments())),
        ("ML: registered_models.list",     "ml",              lambda: list(client.registered_models.list())),
        ("ML: model_registry.list",        "ml",              lambda: list(client.model_registry.list_models())),
        ("Serving: serving_endpoints",     "serving",         lambda: list(client.serving_endpoints.list())),
        ("Security: users.list",           "security",        lambda: list(client.users.list())),
        ("Security: groups.list",          "security",        lambda: list(client.groups.list())),
        ("Security: service_principals",   "security",        lambda: list(client.service_principals.list())),
        ("Security: token_management",     "security",        lambda: list(client.token_management.list())),
        ("Security: secrets.list_scopes",  "security",        lambda: list(client.secrets.list_scopes())),
        ("Security: ip_access_lists",      "security",        lambda: list(client.ip_access_lists.list())),
        ("Sharing: providers.list",        "sharing",         lambda: list(client.providers.list())),
        ("Sharing: recipients.list",       "sharing",         lambda: list(client.recipients.list())),
    ]

    # APIs that may not exist on all SDK versions
    optional_api_checks = [
        ("Serving: apps.list",                 "serving",       "apps", "list"),
        ("Serving: vector_search_endpoints",   "serving",       "vector_search_endpoints", "list_endpoints"),
        ("SQL: lakeview.list",                 "sql_analytics", "lakeview", "list"),
        ("Quality: notification_destinations", "quality",       "notification_destinations", "list"),
        ("ML: feature_engineering",            "ml",            "feature_engineering", "list_features"),
        ("ML: feature_store",                  "ml",            "feature_store", "list_online_stores"),
        ("Sharing: clean_rooms.list",          "sharing",       "clean_rooms", "list"),
        ("Usage: policy_compliance",           "usage",         "policy_compliance_for_clusters", "list_compliance"),
    ]

    results = {}
    ok_count = 0
    warn_count = 0
    fail_count = 0

    enabled = set(settings.enabled_collectors)

    for label, group, fn in checks:
        if group and group not in enabled:
            results[label] = {"status": "skip"}
            continue
        try:
            fn()
            results[label] = {"status": "ok"}
            ok_count += 1
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:200]}"
            status = _classify_error(err)
            results[label] = {"status": status, "error": err}
            if status == "warn":
                warn_count += 1
            else:
                fail_count += 1

    for label, group, api_name, method_name in optional_api_checks:
        if group and group not in enabled:
            results[label] = {"status": "skip"}
            continue
        api_obj = getattr(client, api_name, None)
        if api_obj is None:
            results[label] = {"status": "warn", "error": f"API '{api_name}' not in this SDK version"}
            warn_count += 1
            continue
        fn = getattr(api_obj, method_name, None)
        if fn is None:
            results[label] = {"status": "warn", "error": f"Method '{method_name}' not in this SDK version"}
            warn_count += 1
            continue
        try:
            if "compliance" in method_name:
                pass  # needs policy_id, just confirm method exists
            else:
                list(fn())
            results[label] = {"status": "ok"}
            ok_count += 1
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:200]}"
            status = _classify_error(err)
            results[label] = {"status": status, "error": err}
            if status == "warn":
                warn_count += 1
            else:
                fail_count += 1

    # Save to output
    os.makedirs(settings.output_dir, exist_ok=True)
    preflight_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok_count,
        "warn": warn_count,
        "fail": fail_count,
        "checks": results,
    }
    atomic_write(os.path.join(settings.output_dir, "preflight.json"), preflight_data)

    # Print summary
    parts = [f"{ok_count} ok"]
    if warn_count:
        parts.append(f"{warn_count} unavailable")
    if fail_count:
        parts.append(f"{fail_count} FAILED")
    print(f"\nPreflight: {', '.join(parts)}")
    for label, info in results.items():
        status = info["status"]
        if status == "ok":
            print(f"  ok   {label}")
        elif status == "warn":
            print(f"  --   {label}  (not available)")
        elif status == "fail":
            print(f"  FAIL {label}: {info.get('error', '')[:100]}")
    print()

    return results, ok_count, warn_count, fail_count


def run_collection(settings):
    """Main entry point: run all enabled collectors with checkpointing."""
    client = WorkspaceClient()

    me = client.current_user.me()
    print(f"Connected as: {me.user_name}")

    # Preflight permission checks
    run_preflight(client, settings)

    os.makedirs(settings.output_dir, exist_ok=True)
    checkpoint = Checkpoint(settings.output_dir, settings)

    if checkpoint.has_progress():
        if checkpoint.settings_changed():
            print("Settings changed since last run - starting fresh.")
            checkpoint.reset()
        else:
            completed = [n for n, i in checkpoint.data["collectors"].items() if i.get("status") == "completed"]
            if completed:
                print(f"Resuming - skipping {len(completed)} completed: {', '.join(completed)}")

    collectors = [COLLECTOR_REGISTRY[n](client, settings) for n in settings.enabled_collectors if n in COLLECTOR_REGISTRY]
    to_run = [c for c in collectors if not checkpoint.is_completed(c.name)]
    skipped = [c for c in collectors if checkpoint.is_completed(c.name)]

    if skipped:
        print(f"Skipping {len(skipped)} completed: {', '.join(c.name for c in skipped)}")
    if not to_run:
        print("All collectors already completed!")
        return checkpoint.get_completed_summaries()

    print(f"\nRunning {len(to_run)} collector(s) with {settings.max_workers} workers...\n")

    start_time = time.time()

    def run_one(collector):
        name = collector.name
        t0 = time.time()
        checkpoint.mark_in_progress(name)
        try:
            collector.collect()
            summary = collector.get_summary()
            checkpoint.mark_completed(name, summary)
            elapsed = time.time() - t0
            err_count = summary.get("error_count", 0)
            err_str = f" ({err_count} errors)" if err_count else ""
            print(f"  done {name:<20s} {elapsed:6.1f}s{err_str}")
            return name, summary
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            checkpoint.mark_failed(name, error_msg)
            print(f"  FAIL {name:<20s} {time.time()-t0:6.1f}s  {error_msg[:80]}")
            return name, {"name": name, "error": error_msg}

    results = {}
    with ThreadPoolExecutor(max_workers=settings.max_workers) as pool:
        futures = {pool.submit(run_one, c): c for c in to_run}
        for future in as_completed(futures):
            name, summary = future.result()
            results[name] = summary

    print(f"\nCollection finished in {time.time() - start_time:.0f}s")

    all_summaries = checkpoint.get_completed_summaries()
    summary_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "collectors": all_summaries,
        "totals": {k: sum(s.get(k, 0) for s in all_summaries.values())
                   for k in set().union(*(s.keys() for s in all_summaries.values()))
                   if all(isinstance(s.get(k, 0), (int, float)) for s in all_summaries.values()) and k != "error_count"},
    }
    atomic_write(os.path.join(settings.output_dir, "summary.json"), summary_data)
    return all_summaries

print("Ready.")

# COMMAND ----------

summaries = run_collection(settings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

rows = ""
for name, info in sorted(summaries.items()):
    details = []
    item_count = 0
    for k, v in info.items():
        if k in ("name", "description", "errors", "error_count"):
            continue
        if isinstance(v, (int, float)):
            item_count += v
            details.append(f"{k}={v}")
    err = info.get("error_count", 0)
    err_html = f'<span style="color:red">{err}</span>' if err else '<span style="color:green">0</span>'
    rows += f"<tr><td><b>{name}</b></td><td style='text-align:right'>{item_count:.0f}</td><td style='text-align:right'>{err_html}</td><td>{', '.join(details[:8])}</td></tr>\n"

displayHTML(f"""
<h3>Collection Summary</h3>
<table style="border-collapse:collapse; width:100%">
<tr style="background:#f0f0f0"><th style="text-align:left;padding:6px">Collector</th><th style="padding:6px">Items</th><th style="padding:6px">Errors</th><th style="text-align:left;padding:6px">Details</th></tr>
{rows}
</table>
<p style="color:gray; margin-top:12px">Output: <code>{settings.output_dir}</code> | Summary: <code>{settings.output_dir}/summary.json</code></p>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download archive

# COMMAND ----------

import shutil, base64 as _b64

_archive_name = "inventory_collector_output"
_archive_local = shutil.make_archive(
    os.path.join("/tmp", _archive_name), "zip", settings.output_dir
)
_archive_size_mb = os.path.getsize(_archive_local) / (1024 * 1024)
_file_count = sum(len(files) for _, _, files in os.walk(settings.output_dir))

# Upload to a UC Volume (workspace.default) — create it if needed
_volume_path = None
_download_url = None
try:
    from databricks.sdk import WorkspaceClient as _WC
    _wc = _WC()
    _vol_name = "inventory_collector"
    _vol_catalog = "workspace"
    _vol_schema = "default"
    # Create volume if it doesn't exist
    try:
        _wc.volumes.read(f"{_vol_catalog}.{_vol_schema}.{_vol_name}")
    except Exception:
        try:
            from databricks.sdk.service.catalog import VolumeType
            _vt = VolumeType.MANAGED
        except Exception:
            class _VT:
                value = "MANAGED"
            _vt = _VT()
        _wc.volumes.create(
            catalog_name=_vol_catalog,
            schema_name=_vol_schema,
            name=_vol_name,
            volume_type=_vt,
        )
        print(f"Created volume: {_vol_catalog}.{_vol_schema}.{_vol_name}")
    _volume_base = f"/Volumes/{_vol_catalog}/{_vol_schema}/{_vol_name}"
    _dest = f"{_volume_base}/{_archive_name}.zip"
    shutil.copy2(_archive_local, _dest)
    _volume_path = _dest
    print(f"Archive saved to: {_dest}")
except Exception as _vol_err:
    print(f"Could not save to UC Volume: {_vol_err}")

# Generate inline base64 download link as universal fallback
# (works on serverless, shared clusters, any environment)
if _archive_size_mb <= 100:
    with open(_archive_local, "rb") as _zf:
        _b64_data = _b64.b64encode(_zf.read()).decode()
    _download_url = f"data:application/zip;base64,{_b64_data}"

_volume_html = f'<p>UC Volume: <code>{_volume_path}</code></p>' if _volume_path else ""
if _download_url:
    displayHTML(f"""
    <h3>Download</h3>
    <p>Archive: <b>{_archive_size_mb:.1f} MB</b> ({_file_count} files)</p>
    <a href="{_download_url}" download="{_archive_name}.zip"
       style="display:inline-block; padding:10px 24px; background:#1b6acb; color:white;
              text-decoration:none; border-radius:4px; font-size:14px; font-weight:bold;">
       Download ZIP
    </a>
    {_volume_html}
    """)
else:
    displayHTML(f"""
    <h3>Download</h3>
    <p>Archive: <b>{_archive_size_mb:.1f} MB</b> ({_file_count} files) — too large for inline download.</p>
    {_volume_html}
    <p>Retrieve from the driver or UC Volume path above.</p>
    """)
