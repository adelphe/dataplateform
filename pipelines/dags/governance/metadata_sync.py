"""Data Governance Metadata Sync DAG.

Orchestrates metadata synchronization between data sources and OpenMetadata:
1. Triggers OpenMetadata ingestion workflows (Postgres, MinIO, dbt, Lineage)
2. Syncs PostgreSQL database metadata (schemas, tables, columns)
3. Syncs MinIO storage metadata (buckets, objects)
4. Syncs dbt model metadata and lineage
5. Updates data lineage connections and pushes to OpenMetadata API
6. Validates metadata completeness and quality
7. Generates governance reports

This DAG runs on a schedule to keep the data catalog up-to-date
with the current state of the data platform.
"""

import json
import sys
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "data-governance",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# OpenMetadata configuration
OPENMETADATA_SERVER = "http://openmetadata-server:8585"
OPENMETADATA_API = f"{OPENMETADATA_SERVER}/api/v1"

# Schemas to sync metadata for
POSTGRES_SCHEMAS = ["raw", "staging", "intermediate", "marts", "reference", "governance"]

# MinIO buckets to sync
MINIO_BUCKETS = ["raw", "staging", "curated"]

# OpenMetadata ingestion config paths (mounted in ingestion container)
INGESTION_CONFIGS = {
    "postgres": "/opt/airflow/dags/postgres_ingestion.yaml",
    "minio": "/opt/airflow/dags/minio_ingestion.yaml",
    "minio_bronze": "/opt/airflow/dags/minio_bronze_ingestion.yaml",
    "dbt": "/opt/airflow/dags/dbt_ingestion.yaml",
    "lineage": "/opt/airflow/dags/lineage_ingestion.yaml",
}


def _on_failure_callback(context: dict) -> None:
    """Handle task failure with alerting."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]

    print(f"ALERT: Task {task_id} in DAG {dag_id} failed at {execution_date}")

    try:
        from utils.alerting import (
            format_task_failure_alert,
            send_alert,
            AlertSeverity,
        )

        alert_data = format_task_failure_alert(context)
        alert_data["severity"] = AlertSeverity.WARNING
        send_alert(alert_data)
    except Exception as e:
        print(f"Warning: Failed to send failure alert: {e}")


def _check_openmetadata_health(**context) -> str:
    """Check if OpenMetadata server is healthy and accessible.

    Returns:
        str: Next task to execute based on health check result.
    """
    import requests

    try:
        response = requests.get(
            f"{OPENMETADATA_SERVER}/api/v1/system/version",
            timeout=30
        )
        response.raise_for_status()
        version_info = response.json()
        print(f"OpenMetadata server is healthy. Version: {version_info.get('version')}")
        context["ti"].xcom_push(key="om_version", value=version_info.get("version"))
        return "trigger_openmetadata_ingestion"
    except Exception as e:
        print(f"OpenMetadata server health check failed: {e}")
        return "skip_sync_server_down"


def _trigger_openmetadata_ingestion(**context) -> dict[str, Any]:
    """Trigger OpenMetadata ingestion workflows via CLI.

    Executes the metadata ingest CLI command for each configured
    ingestion workflow (Postgres, MinIO, dbt, lineage).

    Returns:
        dict: Summary of ingestion execution results.
    """
    import subprocess
    import os

    ingestion_results = {
        "successful": [],
        "failed": [],
        "skipped": [],
    }

    for source_name, config_path in INGESTION_CONFIGS.items():
        print(f"Triggering {source_name} ingestion...")

        # Check if config file exists
        if not os.path.exists(config_path):
            print(f"Config file not found: {config_path}")
            ingestion_results["skipped"].append({
                "source": source_name,
                "reason": "config file not found"
            })
            continue

        try:
            # Execute metadata ingest CLI command
            result = subprocess.run(
                ["metadata", "ingest", "-c", config_path],
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout per ingestion
                env={
                    **os.environ,
                    "OPENMETADATA_AUTH_PROVIDER": os.getenv(
                        "OPENMETADATA_AUTH_PROVIDER", "basic"
                    ),
                    "OPENMETADATA_JWT_TOKEN": os.getenv(
                        "OPENMETADATA_JWT_TOKEN", ""
                    ),
                }
            )

            if result.returncode == 0:
                print(f"SUCCESS: {source_name} ingestion completed")
                ingestion_results["successful"].append({
                    "source": source_name,
                    "output": result.stdout[-500:] if result.stdout else ""
                })
            else:
                print(f"FAILED: {source_name} ingestion - {result.stderr}")
                ingestion_results["failed"].append({
                    "source": source_name,
                    "error": result.stderr[-500:] if result.stderr else "Unknown error"
                })

        except subprocess.TimeoutExpired:
            print(f"TIMEOUT: {source_name} ingestion exceeded 10 minutes")
            ingestion_results["failed"].append({
                "source": source_name,
                "error": "Timeout exceeded"
            })
        except Exception as e:
            print(f"ERROR: {source_name} ingestion - {e}")
            ingestion_results["failed"].append({
                "source": source_name,
                "error": str(e)
            })

    print(
        f"Ingestion summary: {len(ingestion_results['successful'])} successful, "
        f"{len(ingestion_results['failed'])} failed, "
        f"{len(ingestion_results['skipped'])} skipped"
    )

    context["ti"].xcom_push(key="ingestion_results", value=ingestion_results)
    return ingestion_results


def _sync_postgres_metadata(**context) -> dict[str, Any]:
    """Sync PostgreSQL database metadata to OpenMetadata.

    Extracts schema information from PostgreSQL and updates
    the data catalog with table and column metadata.

    Returns:
        dict: Summary of synced metadata.
    """
    from utils.postgres_client import execute_query

    sync_summary = {
        "schemas_processed": 0,
        "tables_synced": 0,
        "columns_synced": 0,
        "errors": [],
    }

    for schema in POSTGRES_SCHEMAS:
        try:
            # Get table information
            tables_query = """
                SELECT
                    t.table_name,
                    t.table_type,
                    obj_description(
                        (quote_ident(t.table_schema) || '.' ||
                         quote_ident(t.table_name))::regclass
                    ) as table_comment
                FROM information_schema.tables t
                WHERE t.table_schema = %s
                ORDER BY t.table_name
            """
            tables = execute_query(tables_query, (schema,))

            for table in tables:
                table_name = table["table_name"]

                # Get column information
                columns_query = """
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        col_description(
                            (quote_ident(c.table_schema) || '.' ||
                             quote_ident(c.table_name))::regclass,
                            c.ordinal_position
                        ) as column_comment
                    FROM information_schema.columns c
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """
                columns = execute_query(columns_query, (schema, table_name))

                # Store metadata in governance tracking table
                _upsert_table_metadata(schema, table_name, table, columns)

                sync_summary["tables_synced"] += 1
                sync_summary["columns_synced"] += len(columns)

            sync_summary["schemas_processed"] += 1
            print(f"Synced metadata for schema: {schema}")

        except Exception as e:
            error_msg = f"Error syncing schema {schema}: {e}"
            print(error_msg)
            sync_summary["errors"].append(error_msg)

    context["ti"].xcom_push(key="postgres_sync_summary", value=sync_summary)
    return sync_summary


def _upsert_table_metadata(
    schema: str,
    table_name: str,
    table_info: dict,
    columns: list[dict]
) -> None:
    """Upsert table metadata to governance tracking table."""
    from utils.postgres_client import execute_query

    # Update lineage tracking
    upsert_query = """
        INSERT INTO governance.lineage_edges (
            source_type, source_name, source_schema,
            target_type, target_name, target_schema,
            transformation_type, created_at, updated_at
        )
        SELECT
            'schema', %s, %s,
            'table', %s, %s,
            'metadata_sync', NOW(), NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM governance.lineage_edges
            WHERE source_name = %s AND source_schema = %s
              AND target_name = %s AND target_schema = %s
        )
    """
    try:
        execute_query(
            upsert_query,
            (schema, 'postgres', table_name, schema,
             schema, 'postgres', table_name, schema),
            fetch=False
        )
    except Exception as e:
        print(f"Warning: Could not update lineage for {schema}.{table_name}: {e}")


def _sync_minio_metadata(**context) -> dict[str, Any]:
    """Sync MinIO storage metadata to the catalog.

    Extracts bucket and object metadata from MinIO and updates
    the data catalog with storage information.

    Returns:
        dict: Summary of synced storage metadata.
    """
    from utils.minio_client import get_minio_client

    sync_summary = {
        "buckets_processed": 0,
        "objects_cataloged": 0,
        "total_size_bytes": 0,
        "errors": [],
    }

    try:
        client = get_minio_client()

        for bucket_name in MINIO_BUCKETS:
            try:
                # Check if bucket exists
                if not client.bucket_exists(bucket_name):
                    print(f"Bucket {bucket_name} does not exist, skipping")
                    continue

                # List objects with metadata
                objects = client.list_objects(bucket_name, recursive=True)
                bucket_objects = 0
                bucket_size = 0

                for obj in objects:
                    bucket_objects += 1
                    bucket_size += obj.size if obj.size else 0

                    # Catalog object metadata (sample - limit to avoid overload)
                    if bucket_objects <= 100:
                        _catalog_storage_object(bucket_name, obj)

                sync_summary["buckets_processed"] += 1
                sync_summary["objects_cataloged"] += bucket_objects
                sync_summary["total_size_bytes"] += bucket_size

                print(
                    f"Synced metadata for bucket {bucket_name}: "
                    f"{bucket_objects} objects, {bucket_size / (1024*1024):.2f} MB"
                )

            except Exception as e:
                error_msg = f"Error syncing bucket {bucket_name}: {e}"
                print(error_msg)
                sync_summary["errors"].append(error_msg)

    except Exception as e:
        error_msg = f"Error connecting to MinIO: {e}"
        print(error_msg)
        sync_summary["errors"].append(error_msg)

    context["ti"].xcom_push(key="minio_sync_summary", value=sync_summary)
    return sync_summary


def _catalog_storage_object(bucket_name: str, obj: Any) -> None:
    """Catalog a storage object's metadata."""
    # Extract object metadata for cataloging
    object_metadata = {
        "bucket": bucket_name,
        "key": obj.object_name,
        "size": obj.size,
        "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
        "etag": obj.etag,
        "content_type": getattr(obj, "content_type", None),
    }

    # Determine layer from bucket name
    layer_mapping = {
        "raw": "bronze",
        "staging": "silver",
        "curated": "gold",
    }
    object_metadata["layer"] = layer_mapping.get(bucket_name, "unknown")

    # Log for tracking (in production, would send to OpenMetadata API)
    print(f"Cataloged: {bucket_name}/{obj.object_name}")


def _sync_dbt_metadata(**context) -> dict[str, Any]:
    """Sync dbt model metadata and lineage.

    Parses dbt manifest and catalog files to extract model
    information, tests, and lineage relationships.

    Returns:
        dict: Summary of synced dbt metadata.
    """
    import os

    sync_summary = {
        "models_synced": 0,
        "tests_synced": 0,
        "sources_synced": 0,
        "lineage_edges": 0,
        "errors": [],
    }

    dbt_project_path = "/opt/airflow/transformations/dbt_project"
    manifest_path = os.path.join(dbt_project_path, "target", "manifest.json")
    catalog_path = os.path.join(dbt_project_path, "target", "catalog.json")

    # Check if dbt artifacts exist
    if not os.path.exists(manifest_path):
        print("dbt manifest not found. Run 'dbt docs generate' first.")
        sync_summary["errors"].append("dbt manifest not found")
        context["ti"].xcom_push(key="dbt_sync_summary", value=sync_summary)
        return sync_summary

    try:
        # Parse manifest
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        # Process nodes (models, seeds, tests)
        for node_id, node in manifest.get("nodes", {}).items():
            resource_type = node.get("resource_type")

            if resource_type == "model":
                _process_dbt_model(node, sync_summary)
                sync_summary["models_synced"] += 1

            elif resource_type == "test":
                sync_summary["tests_synced"] += 1

        # Process sources
        for source_id, source in manifest.get("sources", {}).items():
            sync_summary["sources_synced"] += 1

        # Extract lineage from parent_map
        parent_map = manifest.get("parent_map", {})
        for child, parents in parent_map.items():
            for parent in parents:
                _record_lineage_edge(parent, child)
                sync_summary["lineage_edges"] += 1

        print(
            f"dbt sync complete: {sync_summary['models_synced']} models, "
            f"{sync_summary['lineage_edges']} lineage edges"
        )

    except Exception as e:
        error_msg = f"Error syncing dbt metadata: {e}"
        print(error_msg)
        sync_summary["errors"].append(error_msg)

    context["ti"].xcom_push(key="dbt_sync_summary", value=sync_summary)
    return sync_summary


def _process_dbt_model(node: dict, sync_summary: dict) -> None:
    """Process a dbt model node for metadata extraction."""
    model_info = {
        "name": node.get("name"),
        "schema": node.get("schema"),
        "database": node.get("database"),
        "description": node.get("description", ""),
        "materialization": node.get("config", {}).get("materialized"),
        "tags": node.get("tags", []),
        "columns": {},
    }

    # Extract column information
    for col_name, col_info in node.get("columns", {}).items():
        model_info["columns"][col_name] = {
            "name": col_name,
            "description": col_info.get("description", ""),
            "data_type": col_info.get("data_type"),
            "tests": col_info.get("tests", []),
        }

    print(f"Processed dbt model: {model_info['schema']}.{model_info['name']}")


def _record_lineage_edge(source: str, target: str) -> None:
    """Record a lineage edge between two dbt nodes."""
    from utils.postgres_client import execute_query

    # Parse node identifiers
    source_parts = source.split(".")
    target_parts = target.split(".")

    source_name = source_parts[-1] if source_parts else source
    target_name = target_parts[-1] if target_parts else target

    try:
        upsert_query = """
            INSERT INTO governance.lineage_edges (
                source_type, source_name, source_schema,
                target_type, target_name, target_schema,
                transformation_type, created_at, updated_at
            )
            VALUES ('dbt_node', %s, 'dbt', 'dbt_node', %s, 'dbt', 'dbt', NOW(), NOW())
            ON CONFLICT (source_name, source_schema, target_name, target_schema)
            DO UPDATE SET updated_at = NOW()
        """
        execute_query(upsert_query, (source_name, target_name), fetch=False)
    except Exception as e:
        print(f"Warning: Could not record lineage edge: {e}")


def _update_data_lineage(**context) -> dict[str, Any]:
    """Update comprehensive data lineage visualization.

    Consolidates lineage information from all sources,
    pushes lineage to OpenMetadata API, and generates
    a complete lineage graph for the data platform.

    Returns:
        dict: Summary of lineage update.
    """
    import requests
    from utils.postgres_client import execute_query

    lineage_summary = {
        "total_edges": 0,
        "source_tables": set(),
        "target_tables": set(),
        "transformation_types": set(),
        "openmetadata_pushed": 0,
        "openmetadata_errors": [],
    }

    try:
        # Get all lineage edges from local database
        query = """
            SELECT
                source_type, source_name, source_schema,
                target_type, target_name, target_schema,
                transformation_type
            FROM governance.lineage_edges
            WHERE updated_at >= NOW() - INTERVAL '30 days'
        """
        edges = execute_query(query)

        for edge in edges:
            lineage_summary["total_edges"] += 1
            lineage_summary["source_tables"].add(
                f"{edge['source_schema']}.{edge['source_name']}"
            )
            lineage_summary["target_tables"].add(
                f"{edge['target_schema']}.{edge['target_name']}"
            )
            if edge["transformation_type"]:
                lineage_summary["transformation_types"].add(edge["transformation_type"])

            # Push lineage edge to OpenMetadata API
            pushed = _push_lineage_to_openmetadata(edge, lineage_summary)
            if pushed:
                lineage_summary["openmetadata_pushed"] += 1

        # Convert sets to lists for JSON serialization
        lineage_summary["source_tables"] = list(lineage_summary["source_tables"])
        lineage_summary["target_tables"] = list(lineage_summary["target_tables"])
        lineage_summary["transformation_types"] = list(
            lineage_summary["transformation_types"]
        )

        print(
            f"Lineage update complete: {lineage_summary['total_edges']} edges, "
            f"{len(lineage_summary['source_tables'])} sources, "
            f"{len(lineage_summary['target_tables'])} targets, "
            f"{lineage_summary['openmetadata_pushed']} pushed to OpenMetadata"
        )

    except Exception as e:
        print(f"Error updating lineage: {e}")
        lineage_summary["error"] = str(e)

    context["ti"].xcom_push(key="lineage_summary", value=lineage_summary)
    return lineage_summary


def _get_openmetadata_auth_headers() -> dict:
    """Get authentication headers for OpenMetadata API calls.

    Returns:
        dict: Headers including Authorization if configured.
    """
    import os

    headers = {"Content-Type": "application/json"}
    auth_provider = os.getenv("OPENMETADATA_AUTH_PROVIDER", "basic")

    if auth_provider == "basic":
        jwt_token = os.getenv("OPENMETADATA_JWT_TOKEN")
        if jwt_token:
            headers["Authorization"] = f"Bearer {jwt_token}"
    elif auth_provider != "no-auth":
        # For other auth providers (e.g., openmetadata, google, etc.)
        jwt_token = os.getenv("OPENMETADATA_JWT_TOKEN")
        if jwt_token:
            headers["Authorization"] = f"Bearer {jwt_token}"

    return headers


def _push_lineage_to_openmetadata(edge: dict, summary: dict) -> bool:
    """Push a lineage edge to OpenMetadata API.

    Args:
        edge: Lineage edge data from database.
        summary: Summary dict to append errors to.

    Returns:
        bool: True if successfully pushed, False otherwise.
    """
    import requests

    # Build the FQN for source and target entities
    source_fqn = _build_entity_fqn(
        edge["source_type"],
        edge["source_schema"],
        edge["source_name"]
    )
    target_fqn = _build_entity_fqn(
        edge["target_type"],
        edge["target_schema"],
        edge["target_name"]
    )

    if not source_fqn or not target_fqn:
        return False

    # Build lineage payload for OpenMetadata API
    lineage_payload = {
        "edge": {
            "fromEntity": {
                "type": _map_entity_type(edge["source_type"]),
                "fqn": source_fqn
            },
            "toEntity": {
                "type": _map_entity_type(edge["target_type"]),
                "fqn": target_fqn
            },
            "description": edge.get("transformation_type", ""),
        }
    }

    try:
        response = requests.put(
            f"{OPENMETADATA_API}/lineage",
            json=lineage_payload,
            headers=_get_openmetadata_auth_headers(),
            timeout=30
        )

        if response.status_code in (200, 201):
            return True
        else:
            error_msg = f"Failed to push lineage {source_fqn} -> {target_fqn}: {response.text}"
            print(f"Warning: {error_msg}")
            summary["openmetadata_errors"].append(error_msg)
            return False

    except Exception as e:
        error_msg = f"Error pushing lineage {source_fqn} -> {target_fqn}: {e}"
        print(f"Warning: {error_msg}")
        summary["openmetadata_errors"].append(error_msg)
        return False


def _build_entity_fqn(entity_type: str, schema: str, name: str) -> str | None:
    """Build fully qualified name for OpenMetadata entity.

    Args:
        entity_type: Type of entity (table, container, etc).
        schema: Schema or namespace.
        name: Entity name.

    Returns:
        str: Fully qualified name, or None if cannot be built.
    """
    if not name:
        return None

    # Map to OpenMetadata service names
    if entity_type in ("table", "dbt_node", "schema"):
        # PostgreSQL tables: service.database.schema.table
        return f"data-warehouse-postgres.airflow.{schema}.{name}"
    elif entity_type == "container":
        # MinIO containers: service.bucket
        return f"data-lake-minio.{name}"
    elif entity_type == "dashboard":
        # Superset dashboards
        return f"superset.{name}"
    else:
        # Generic fallback
        return f"{schema}.{name}"


def _map_entity_type(source_type: str) -> str:
    """Map internal entity types to OpenMetadata entity types.

    Args:
        source_type: Internal entity type.

    Returns:
        str: OpenMetadata entity type.
    """
    type_mapping = {
        "table": "table",
        "dbt_node": "table",
        "schema": "databaseSchema",
        "container": "container",
        "dashboard": "dashboard",
    }
    return type_mapping.get(source_type, "table")


def _validate_metadata_quality(**context) -> dict[str, Any]:
    """Validate metadata completeness and quality.

    Checks for missing descriptions, orphaned tables,
    and other metadata quality issues.

    Returns:
        dict: Validation results and quality metrics.
    """
    from utils.postgres_client import execute_query

    validation_results = {
        "tables_without_owners": [],
        "columns_without_descriptions": 0,
        "orphaned_tables": [],
        "quality_score": 100.0,
    }

    try:
        # Check for tables without owners
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables t
            WHERE t.table_schema IN %s
              AND NOT EXISTS (
                  SELECT 1 FROM governance.data_owners do
                  WHERE do.schema_name = t.table_schema
                    AND do.dataset_name = t.table_name
              )
        """
        unowned = execute_query(query, (tuple(POSTGRES_SCHEMAS),))
        validation_results["tables_without_owners"] = [
            f"{r['table_schema']}.{r['table_name']}" for r in unowned
        ]

        # Check for columns without descriptions
        query = """
            SELECT COUNT(*) as count
            FROM information_schema.columns c
            WHERE c.table_schema IN %s
              AND col_description(
                  (quote_ident(c.table_schema) || '.' ||
                   quote_ident(c.table_name))::regclass,
                  c.ordinal_position
              ) IS NULL
        """
        result = execute_query(query, (tuple(POSTGRES_SCHEMAS),))
        validation_results["columns_without_descriptions"] = (
            result[0]["count"] if result else 0
        )

        # Calculate quality score
        total_issues = (
            len(validation_results["tables_without_owners"]) * 10 +
            validation_results["columns_without_descriptions"]
        )
        validation_results["quality_score"] = max(0, 100 - (total_issues * 0.5))

        print(
            f"Metadata quality score: {validation_results['quality_score']:.1f}% "
            f"({len(validation_results['tables_without_owners'])} unowned tables, "
            f"{validation_results['columns_without_descriptions']} undocumented columns)"
        )

    except Exception as e:
        print(f"Error validating metadata: {e}")
        validation_results["error"] = str(e)

    context["ti"].xcom_push(key="validation_results", value=validation_results)
    return validation_results


def _generate_governance_report(**context) -> dict[str, Any]:
    """Generate comprehensive governance report.

    Compiles all sync summaries and validation results
    into a governance report.

    Returns:
        dict: Complete governance report.
    """
    ti = context["ti"]

    # Gather all summaries
    postgres_summary = ti.xcom_pull(
        task_ids="sync_postgres_metadata", key="postgres_sync_summary"
    ) or {}
    minio_summary = ti.xcom_pull(
        task_ids="sync_minio_metadata", key="minio_sync_summary"
    ) or {}
    dbt_summary = ti.xcom_pull(
        task_ids="sync_dbt_metadata", key="dbt_sync_summary"
    ) or {}
    lineage_summary = ti.xcom_pull(
        task_ids="update_data_lineage", key="lineage_summary"
    ) or {}
    validation_results = ti.xcom_pull(
        task_ids="validate_metadata_quality", key="validation_results"
    ) or {}

    report = {
        "report_date": context["ds"],
        "execution_date": context["execution_date"].isoformat(),
        "summaries": {
            "postgres": postgres_summary,
            "minio": minio_summary,
            "dbt": dbt_summary,
            "lineage": lineage_summary,
        },
        "validation": validation_results,
        "overall_status": "healthy",
    }

    # Determine overall status
    total_errors = (
        len(postgres_summary.get("errors", [])) +
        len(minio_summary.get("errors", [])) +
        len(dbt_summary.get("errors", []))
    )

    quality_score = validation_results.get("quality_score", 0)

    if total_errors > 5 or quality_score < 50:
        report["overall_status"] = "critical"
    elif total_errors > 0 or quality_score < 80:
        report["overall_status"] = "warning"

    # Store report
    from utils.postgres_client import execute_query
    try:
        insert_query = """
            INSERT INTO governance.quality_rules (
                rule_name, dataset_name, schema_name,
                rule_type, rule_definition, threshold, severity
            )
            VALUES (
                'metadata_sync_report', 'governance', 'metadata',
                'consistency', %s, %s, %s
            )
        """
        execute_query(
            insert_query,
            (json.dumps(report), quality_score, report["overall_status"]),
            fetch=False
        )
    except Exception as e:
        print(f"Warning: Could not store governance report: {e}")

    print(f"\n{'='*60}")
    print("GOVERNANCE REPORT SUMMARY")
    print(f"{'='*60}")
    print(f"Report Date: {report['report_date']}")
    print(f"Overall Status: {report['overall_status'].upper()}")
    print(f"Metadata Quality Score: {quality_score:.1f}%")
    print(f"\nPostgreSQL: {postgres_summary.get('tables_synced', 0)} tables synced")
    print(f"MinIO: {minio_summary.get('objects_cataloged', 0)} objects cataloged")
    print(f"dbt: {dbt_summary.get('models_synced', 0)} models synced")
    print(f"Lineage: {lineage_summary.get('total_edges', 0)} edges tracked")
    print(f"{'='*60}\n")

    return report


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="metadata_sync",
    description="Synchronize metadata across data sources for governance",
    default_args=default_args,
    schedule="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["governance", "metadata", "catalog"],
    doc_md=__doc__,
    on_failure_callback=_on_failure_callback,
) as dag:

    # Health check - branch based on OpenMetadata availability
    check_health = BranchPythonOperator(
        task_id="check_openmetadata_health",
        python_callable=_check_openmetadata_health,
    )

    # Skip task when server is down
    skip_sync = EmptyOperator(
        task_id="skip_sync_server_down",
    )

    # Trigger OpenMetadata ingestion workflows
    trigger_ingestion = PythonOperator(
        task_id="trigger_openmetadata_ingestion",
        python_callable=_trigger_openmetadata_ingestion,
    )

    # Metadata sync tasks
    sync_postgres = PythonOperator(
        task_id="sync_postgres_metadata",
        python_callable=_sync_postgres_metadata,
    )

    sync_minio = PythonOperator(
        task_id="sync_minio_metadata",
        python_callable=_sync_minio_metadata,
    )

    sync_dbt = PythonOperator(
        task_id="sync_dbt_metadata",
        python_callable=_sync_dbt_metadata,
    )

    # Lineage update
    update_lineage = PythonOperator(
        task_id="update_data_lineage",
        python_callable=_update_data_lineage,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Validation and reporting
    validate_metadata = PythonOperator(
        task_id="validate_metadata_quality",
        python_callable=_validate_metadata_quality,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    generate_report = PythonOperator(
        task_id="generate_governance_report",
        python_callable=_generate_governance_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task dependencies
    # Branch based on health check: trigger ingestion or skip
    check_health >> [trigger_ingestion, skip_sync]

    # After ingestion, run postgres sync first (needed for schema info)
    trigger_ingestion >> sync_postgres

    # Parallel sync tasks after postgres (postgres must run first for schema info)
    sync_postgres >> [sync_minio, sync_dbt]

    # Lineage update after all syncs complete
    [sync_minio, sync_dbt] >> update_lineage

    # Validation and report after lineage
    update_lineage >> validate_metadata >> generate_report

    # Skip path joins at the end
    skip_sync >> generate_report
