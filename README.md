# Databricks Inventory Collector

Read-only notebook that inventories all objects in a Databricks workspace and outputs structured JSON for audit, compliance, capacity planning, or architectural review.

## What it collects

| Group | Objects |
|---|---|
| **Compute** | Clusters, pools, policies, init scripts, instance profiles, libraries |
| **Unity Catalog** | Catalogs, schemas, tables, views, functions, volumes, grants (at every level), storage credentials, external locations, connections, workspace bindings |
| **Workflows** | Jobs, DLT pipelines, recent job runs |
| **Workspace** | Notebook tree (paths, languages, types), repos, git credentials |
| **SQL Analytics** | SQL warehouses, saved queries (with full SQL definitions), dashboards (legacy + Lakeview), alerts, query history |
| **ML** | MLflow experiments + runs (parameters, metrics), registered models (UC + legacy), model versions, feature engineering, online stores |
| **Security** | Users, groups, service principals, tokens, secret scopes + ACLs, workspace config, IP access lists, group membership |
| **Serving** | Model serving endpoints, vector search endpoints + indexes, apps |
| **Sharing** | Delta Sharing providers, recipients, shares, clean rooms |
| **Quality** | Notification destinations |
| **Usage** | Cluster events, policy compliance |

## How to use

1. In your Databricks workspace, click **Workspace** in the left sidebar
2. Navigate to your target folder (e.g. **Shared** or your user folder)
3. Click the **...** menu > **Import**
4. Upload `inventory_collector.py` and click **Import**
5. Open the notebook and click **Run All**

By default the notebook collects all object types, looks back 30 days for historical data (job runs, query history, cluster events), runs 4 parallel workers, and writes output to `/tmp/inventory_collector` on the driver node. The last cell provides a ZIP download link.

## Output

Results are written as JSON files organized by collector group. A `summary.json` with aggregated stats and a `preflight.json` with API permission check results are included. Interrupted runs can be resumed — completed collectors are automatically skipped.

## Disclaimer

This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement.

**This project is not affiliated with, endorsed by, or associated with Amazon Web Services (AWS), Databricks, Inc., or any of their subsidiaries or affiliates.**

All product names, trademarks, and registered trademarks are property of their respective owners.

See [LICENSE](LICENSE) for full terms.
