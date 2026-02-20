"""
Import existing Google Form responses into the position_control_form.requests BigQuery table.
One-time migration script.
"""

import csv
import uuid
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = 'talent-demo-482004'
DATASET_ID = 'position_control_form'
TABLE_ID = 'requests'

CSV_PATH = r'C:\Users\sshirey\bigquery-dashboards\HR Talent Position Control Form_ 25-26 (Responses) - Form Responses 1.csv'


def parse_timestamp(ts_str):
    """Parse Google Form timestamp like '9/18/2025 14:44:18'."""
    if not ts_str:
        return None
    try:
        return datetime.strptime(ts_str.strip(), '%m/%d/%Y %H:%M:%S')
    except ValueError:
        try:
            return datetime.strptime(ts_str.strip(), '%m/%d/%Y')
        except ValueError:
            return None


def parse_date(d_str):
    """Parse date strings like '10/21/2025' or '12/1/2025'."""
    if not d_str or d_str.strip() in ('', 'N/A'):
        return None
    try:
        return datetime.strptime(d_str.strip(), '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None


def clean_request_type(raw_type):
    """Normalize request type to match our dropdown options."""
    if not raw_type:
        return ''
    raw = raw_type.strip()
    # Strip off the "(CEO & Finance Approval)" suffix
    if 'New Hire - Vacancy' in raw or 'New Hire- Vacancy' in raw:
        return 'New Hire - Vacancy'
    if 'New Hire - Replacement' in raw:
        return 'New Hire - Replacement'
    if 'Additional Comp' in raw:
        return 'Additional Comp (Stipend)'
    if 'Status Change' in raw:
        return 'Status Change'
    if 'Title/Role Change' in raw:
        return 'Title/Role Change'
    if 'Salary Adjustment' in raw:
        return 'Salary Adjustment'
    if 'Temp Hire' in raw:
        return 'Temp Hire'
    if 'Before/After School' in raw:
        return 'Before/After School'
    if 'Supervisor Change' in raw:
        return 'Supervisor Change'
    # Multi-type entries — take the first
    if ',' in raw:
        return clean_request_type(raw.split(',')[0])
    return raw


def clean_hours_status(raw):
    """Normalize hours/status."""
    if not raw:
        return ''
    raw = raw.strip()
    if 'Full-Time' in raw or 'Full Time' in raw:
        return 'Full-Time (40 hrs)'
    if 'Part-Time' in raw or 'Part Time' in raw:
        return 'Part-Time (29 hrs or less)'
    if 'Seasonal' in raw or 'Temp' in raw:
        return 'Seasonal Temp (6mo or less)'
    return raw


def clean_approval(raw):
    """Normalize approval values."""
    if not raw:
        return 'Pending'
    raw = raw.strip()
    if raw.lower() == 'approved':
        return 'Approved'
    if raw.lower() == 'denied':
        return 'Denied'
    if raw.lower() in ('n/a', ''):
        return 'Pending'
    return 'Pending'


def clean_final_status(raw):
    """Normalize final status."""
    if not raw:
        return 'Pending'
    raw = raw.strip()
    if raw.lower() == 'approved':
        return 'Approved'
    if raw.lower() == 'denied':
        return 'Denied'
    if raw.lower() == 'withdrawn':
        return 'Withdrawn'
    return 'Pending'


def clean_sped(raw):
    """Normalize SPED reviewed field."""
    if not raw:
        return 'N/A'
    raw = raw.strip()
    if raw.lower() == 'yes':
        return 'Yes'
    if raw.lower() == 'no':
        return 'No'
    return 'N/A'


def import_csv():
    """Read CSV and insert rows into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    full_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip header row

        rows_inserted = 0
        for row_num, row in enumerate(reader, start=2):
            if len(row) < 20:
                print(f"  Skipping row {row_num}: not enough columns ({len(row)})")
                continue

            # Map CSV columns to our schema
            timestamp = row[0]
            requestor = row[1].strip()
            email = row[2].strip().lower()
            request_type = row[3]
            hours_status = row[4]
            position_title = row[5].strip()
            reports_to = row[6].strip()
            requested_amount = row[7].strip()
            employee_name = row[8].strip()
            justification = row[9].strip()
            sped_reviewed = row[10]
            duration = row[11].strip()
            payment_dates = row[12].strip()
            ceo_approval = row[13]
            finance_approval = row[14]
            talent_approval = row[15]
            hr_approval = row[16]
            final_status = row[17]
            offer_sent = row[18] if len(row) > 18 else ''
            offer_signed = row[19] if len(row) > 19 else ''

            request_id = str(uuid.uuid4())[:8].upper()
            submitted_at = parse_timestamp(timestamp) or datetime.now()

            query = f"""
            INSERT INTO `{full_table}` (
                request_id, submitted_at, requestor_name, requestor_email,
                request_type, hours_status, position_title, reports_to,
                requested_amount, employee_name, justification, sped_reviewed,
                school_year, duration, payment_dates,
                ceo_approval, finance_approval, talent_approval, hr_approval,
                final_status, offer_sent, offer_signed, admin_notes,
                position_id, updated_at, updated_by, is_archived
            ) VALUES (
                @request_id, @submitted_at, @requestor_name, @requestor_email,
                @request_type, @hours_status, @position_title, @reports_to,
                @requested_amount, @employee_name, @justification, @sped_reviewed,
                @school_year, @duration, @payment_dates,
                @ceo_approval, @finance_approval, @talent_approval, @hr_approval,
                @final_status, @offer_sent, @offer_signed, @admin_notes,
                @position_id, @updated_at, @updated_by, @is_archived
            )
            """

            offer_sent_date = parse_date(offer_sent)
            offer_signed_date = parse_date(offer_signed)

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("request_id", "STRING", request_id),
                    bigquery.ScalarQueryParameter("submitted_at", "TIMESTAMP", submitted_at),
                    bigquery.ScalarQueryParameter("requestor_name", "STRING", requestor),
                    bigquery.ScalarQueryParameter("requestor_email", "STRING", email),
                    bigquery.ScalarQueryParameter("request_type", "STRING", clean_request_type(request_type)),
                    bigquery.ScalarQueryParameter("hours_status", "STRING", clean_hours_status(hours_status)),
                    bigquery.ScalarQueryParameter("position_title", "STRING", position_title),
                    bigquery.ScalarQueryParameter("reports_to", "STRING", reports_to),
                    bigquery.ScalarQueryParameter("requested_amount", "STRING", requested_amount),
                    bigquery.ScalarQueryParameter("employee_name", "STRING", employee_name),
                    bigquery.ScalarQueryParameter("justification", "STRING", justification),
                    bigquery.ScalarQueryParameter("sped_reviewed", "STRING", clean_sped(sped_reviewed)),
                    bigquery.ScalarQueryParameter("school_year", "STRING", "25-26 SY"),
                    bigquery.ScalarQueryParameter("duration", "STRING", duration),
                    bigquery.ScalarQueryParameter("payment_dates", "STRING", payment_dates),
                    bigquery.ScalarQueryParameter("ceo_approval", "STRING", clean_approval(ceo_approval)),
                    bigquery.ScalarQueryParameter("finance_approval", "STRING", clean_approval(finance_approval)),
                    bigquery.ScalarQueryParameter("talent_approval", "STRING", clean_approval(talent_approval)),
                    bigquery.ScalarQueryParameter("hr_approval", "STRING", clean_approval(hr_approval)),
                    bigquery.ScalarQueryParameter("final_status", "STRING", clean_final_status(final_status)),
                    bigquery.ScalarQueryParameter("offer_sent", "DATE", offer_sent_date),
                    bigquery.ScalarQueryParameter("offer_signed", "DATE", offer_signed_date),
                    bigquery.ScalarQueryParameter("admin_notes", "STRING", "Imported from Google Form"),
                    bigquery.ScalarQueryParameter("position_id", "STRING", ""),
                    bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", submitted_at),
                    bigquery.ScalarQueryParameter("updated_by", "STRING", "CSV Import"),
                    bigquery.ScalarQueryParameter("is_archived", "BOOL", False),
                ]
            )

            try:
                client.query(query, job_config=job_config).result()
                rows_inserted += 1
                print(f"  Row {row_num}: {requestor} - {position_title} ({clean_request_type(request_type)}) [OK]")
            except Exception as e:
                print(f"  Row {row_num}: ERROR - {e}")

    print(f"\nDone! Imported {rows_inserted} requests.")


if __name__ == "__main__":
    import_csv()
