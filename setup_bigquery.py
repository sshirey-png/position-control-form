"""
Setup script to create the Position Control Form requests table in BigQuery.
Run this once to create the table.

Usage:
    python setup_bigquery.py
"""

import time
from google.cloud import bigquery

PROJECT_ID = 'talent-demo-482004'
DATASET_ID = 'position_control_form'
TABLE_ID = 'requests'


def create_requests_table():
    """Create the position_control_form.requests table in BigQuery."""

    client = bigquery.Client(project=PROJECT_ID)
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists")
    except Exception:
        dataset = client.create_dataset(dataset_ref)
        print(f"Created dataset {DATASET_ID}")

    # Define schema
    schema = [
        bigquery.SchemaField("request_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("submitted_at", "TIMESTAMP"),
        bigquery.SchemaField("requestor_name", "STRING"),
        bigquery.SchemaField("requestor_email", "STRING"),
        bigquery.SchemaField("request_type", "STRING"),
        bigquery.SchemaField("hours_status", "STRING"),
        bigquery.SchemaField("position_title", "STRING"),
        bigquery.SchemaField("reports_to", "STRING"),
        bigquery.SchemaField("requested_amount", "STRING"),
        bigquery.SchemaField("employee_name", "STRING"),
        bigquery.SchemaField("justification", "STRING"),
        bigquery.SchemaField("sped_reviewed", "STRING"),
        bigquery.SchemaField("school_year", "STRING"),
        bigquery.SchemaField("duration", "STRING"),
        bigquery.SchemaField("payment_dates", "STRING"),
        bigquery.SchemaField("ceo_approval", "STRING"),
        bigquery.SchemaField("finance_approval", "STRING"),
        bigquery.SchemaField("talent_approval", "STRING"),
        bigquery.SchemaField("hr_approval", "STRING"),
        bigquery.SchemaField("final_status", "STRING"),
        bigquery.SchemaField("offer_sent", "DATE"),
        bigquery.SchemaField("offer_signed", "DATE"),
        bigquery.SchemaField("admin_notes", "STRING"),
        bigquery.SchemaField("position_id", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("is_archived", "BOOL"),
    ]

    # Check if table already exists
    try:
        client.get_table(full_table_id)
        print(f"Table {full_table_id} already exists")
        return True
    except Exception:
        pass

    # Create table
    table = bigquery.Table(full_table_id, schema=schema)
    table = client.create_table(table)
    print(f"Created table {full_table_id}")

    # Wait for table to be available
    time.sleep(2)

    # Set default for is_archived
    try:
        client.query(
            f"ALTER TABLE `{full_table_id}` ALTER COLUMN is_archived SET DEFAULT FALSE"
        ).result()
        print("Set default value for is_archived column")
    except Exception as e:
        print(f"Note: Could not set default for is_archived: {e}")

    print("\nSetup complete! You can now run the Position Control Form app.")
    return True


if __name__ == "__main__":
    create_requests_table()
