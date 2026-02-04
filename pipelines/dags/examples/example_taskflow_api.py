"""Example DAG demonstrating the modern Airflow TaskFlow API.

This DAG uses the @task decorator, XCom for passing data between tasks,
dynamic task generation, and task groups for logical organization.
"""

from datetime import datetime

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="example_taskflow_api",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "taskflow"],
    default_args={"owner": "data-platform", "retries": 1},
    description="Demonstrate TaskFlow API with @task decorator",
)
def example_taskflow_api():

    @task()
    def extract():
        """Simulate data extraction returning a dataset."""
        data = {
            "users": [
                {"id": 1, "name": "Alice", "score": 85},
                {"id": 2, "name": "Bob", "score": 92},
                {"id": 3, "name": "Charlie", "score": 78},
            ]
        }
        print(f"Extracted {len(data['users'])} records")
        return data

    @task()
    def transform(raw_data: dict):
        """Transform the extracted data by computing derived fields."""
        users = raw_data["users"]
        avg_score = sum(u["score"] for u in users) / len(users)

        for user in users:
            user["above_average"] = user["score"] > avg_score
            user["grade"] = (
                "A" if user["score"] >= 90 else
                "B" if user["score"] >= 80 else
                "C"
            )

        result = {"users": users, "stats": {"count": len(users), "avg_score": avg_score}}
        print(f"Transformed {len(users)} records, avg score: {avg_score:.1f}")
        return result

    @task()
    def validate(transformed_data: dict):
        """Validate the transformed data."""
        users = transformed_data["users"]
        stats = transformed_data["stats"]

        assert stats["count"] > 0, "No records found"
        assert all("grade" in u for u in users), "Missing grade field"

        print(f"Validation passed for {stats['count']} records")
        return {"valid": True, "record_count": stats["count"]}

    @task_group(group_id="load_group")
    def load_data(transformed_data: dict):
        """Task group that loads data to multiple destinations."""

        @task()
        def load_to_warehouse(data: dict):
            """Simulate loading to data warehouse."""
            count = data["stats"]["count"]
            print(f"Loaded {count} records to warehouse")
            return {"destination": "warehouse", "records": count}

        @task()
        def load_to_cache(data: dict):
            """Simulate loading to cache."""
            count = data["stats"]["count"]
            print(f"Loaded {count} records to cache")
            return {"destination": "cache", "records": count}

        load_to_warehouse(transformed_data)
        load_to_cache(transformed_data)

    @task()
    def generate_report(validation_result: dict):
        """Generate a summary report."""
        print(f"Report: Processed {validation_result['record_count']} records")
        print(f"Validation: {'PASSED' if validation_result['valid'] else 'FAILED'}")
        return "report_complete"

    # Define task flow
    raw = extract()
    transformed = transform(raw)
    validation = validate(transformed)
    load_data(transformed)
    generate_report(validation)


example_taskflow_api()
