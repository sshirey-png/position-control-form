"""
Position Control Request Form - Flask Backend
FirstLine Schools
"""

import os
import json
import uuid
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify, send_file, session, redirect, url_for
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from google.cloud import bigquery
from authlib.integrations.flask_client import OAuth

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY') or os.urandom(32)
# Trust proxy headers (required for Cloud Run to detect https)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
CORS(app)

# Configuration
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', 'talent-demo-482004')
DATASET_ID = 'position_control_form'
TABLE_ID = 'requests'

# Position Control table (for creating positions when approved)
PC_DATASET_ID = 'talent_grow_observations'
PC_TABLE_ID = 'position_control'

# Email Configuration
SMTP_EMAIL = os.environ.get('SMTP_EMAIL', 'talent@firstlineschools.org')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
TALENT_TEAM_EMAIL = 'talent@firstlineschools.org'
HR_EMAIL = 'hr@firstlineschools.org'
APP_URL = os.environ.get('APP_URL', 'https://position-control-form-965913991496.us-central1.run.app')

# Role-based admin permissions — by job title (from BigQuery staff_master_list)
# Maps job titles to permission sets. No hardcoded emails.
TITLE_ROLES = {
    'Chief People Officer': {
        'role': 'super_admin',
        'can_approve': ['ceo_approval', 'finance_approval', 'talent_approval', 'hr_approval'],
        'can_edit_final': True,
        'can_create_position': True,
    },
    'Chief Executive Officer': {
        'role': 'ceo',
        'can_approve': ['ceo_approval'],
        'can_edit_final': True,
        'can_create_position': False,
    },
    'Chief Operating Officer': {
        'role': 'finance',
        'can_approve': ['finance_approval'],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'Manager Finance': {
        'role': 'finance',
        'can_approve': ['finance_approval'],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'Manager Payroll': {
        'role': 'viewer',
        'can_approve': [],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'Chief HR Officer': {
        'role': 'hr',
        'can_approve': ['hr_approval', 'talent_approval'],
        'can_edit_final': True,
        'can_create_position': True,
    },
    'Manager, HR': {
        'role': 'hr',
        'can_approve': ['hr_approval'],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'Talent Operations Manager': {
        'role': 'viewer',
        'can_approve': [],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'Recruitment Manager': {
        'role': 'viewer',
        'can_approve': [],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'School Director': {
        'role': 'viewer',
        'can_approve': [],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'Manager - Benefits': {
        'role': 'viewer',
        'can_approve': [],
        'can_edit_final': False,
        'can_create_position': False,
    },
    'Chief Academic Officer': {
        'role': 'viewer',
        'can_approve': [],
        'can_edit_final': False,
        'can_create_position': False,
    },
}


def lookup_job_title(email):
    """Look up a user's job title from BigQuery staff_master_list."""
    if not email:
        return ''
    try:
        query = f"""
        SELECT Job_Title
        FROM `{PROJECT_ID}.{PC_DATASET_ID}.staff_master_list_with_function`
        WHERE LOWER(Email_Address) = @email
        AND Employment_Status IN ('Active', 'Leave of absence')
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email.lower())]
        )
        results = list(bq_client.query(query, job_config=job_config).result())
        if results:
            return results[0].Job_Title or ''
    except Exception as e:
        logger.error(f"Error looking up job title for {email}: {e}")
    return ''


def get_user_permissions(email):
    """Get the permissions for a user based on their job title from BigQuery."""
    job_title = session.get('user', {}).get('job_title', '')
    if not job_title:
        job_title = lookup_job_title(email)
    role_info = TITLE_ROLES.get(job_title)
    if not role_info:
        return None
    return {
        'role': role_info['role'],
        'title': job_title,
        'can_approve': role_info['can_approve'],
        'can_edit_final': role_info['can_edit_final'],
        'can_create_position': role_info['can_create_position'],
        'can_edit_notes': role_info['role'] != 'viewer',
        'can_edit_dates': role_info['role'] in ('super_admin', 'hr'),
        'can_archive': role_info['role'] != 'viewer',
        'can_delete': role_info['role'] == 'super_admin',
        'is_viewer': role_info['role'] == 'viewer',
    }


def is_admin_user(email):
    """Check if user has admin access based on job title."""
    job_title = session.get('user', {}).get('job_title', '')
    if not job_title:
        job_title = lookup_job_title(email)
    return job_title in TITLE_ROLES

# Request type options
REQUEST_TYPES = [
    'Open Position',
    'New Position',
    'Additional Comp (Stipend)',
    'Status Change',
    'Title/Role Change',
    'Salary Adjustment',
    'Temp Hire',
    'Before/After School',
    'Supervisor Change',
]

# Request types that require CEO and Finance approval
CEO_FINANCE_REQUIRED_TYPES = ['New Position']

# Position action config per request type
# 'create' = INSERT new row, 'update' = UPDATE existing employee position
# Types not listed here get no position button at all
POSITION_ACTION_TYPES = {
    'New Position': 'create',
    'Open Position': 'create',        # backend already handles update-if-linked
    'Temp Hire': 'create',
    'Before/After School': 'create',
    'Status Change': 'update',
    'Title/Role Change': 'update',
}

# Hours/Status options
HOURS_STATUS_OPTIONS = [
    'Full-Time (40 hrs)',
    'Part-Time (29 hrs or less)',
    'Seasonal Temp (6mo or less)',
]

# BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)


def ensure_is_archived_column():
    """One-time migration: add is_archived column if it doesn't exist."""
    try:
        full_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        table_ref = bq_client.get_table(full_table)
        existing_fields = [f.name for f in table_ref.schema]
        if 'is_archived' not in existing_fields:
            bq_client.query(f"ALTER TABLE `{full_table}` ADD COLUMN is_archived BOOL").result()
            bq_client.query(f"ALTER TABLE `{full_table}` ALTER COLUMN is_archived SET DEFAULT FALSE").result()
            bq_client.query(f"UPDATE `{full_table}` SET is_archived = FALSE WHERE TRUE").result()
            logger.info("Added is_archived column to requests table")
    except Exception as e:
        logger.error(f"Migration error (is_archived): {e}")


def ensure_hire_type_column():
    """One-time migration: add hire_type column if it doesn't exist."""
    try:
        full_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        table_ref = bq_client.get_table(full_table)
        existing_fields = [f.name for f in table_ref.schema]
        if 'hire_type' not in existing_fields:
            bq_client.query(f"ALTER TABLE `{full_table}` ADD COLUMN hire_type STRING").result()
            logger.info("Added hire_type column to requests table")
    except Exception as e:
        logger.error(f"Migration error (hire_type): {e}")


def ensure_employee_lookup_columns():
    """One-time migration: add employee_email, school, linked_position_id, candidate_email, candidate_position_id columns."""
    try:
        full_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        table_ref = bq_client.get_table(full_table)
        existing_fields = [f.name for f in table_ref.schema]
        for col in ['employee_email', 'school', 'linked_position_id', 'candidate_email', 'candidate_position_id', 'subject', 'grade_level']:
            if col not in existing_fields:
                bq_client.query(f"ALTER TABLE `{full_table}` ADD COLUMN {col} STRING").result()
                logger.info(f"Added {col} column to requests table")
    except Exception as e:
        logger.error(f"Migration error (employee_lookup_columns): {e}")


ensure_is_archived_column()
ensure_hire_type_column()
ensure_employee_lookup_columns()

# OAuth setup
oauth = OAuth(app)
if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    google = oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'}
    )
else:
    google = None


# ============ Email Functions ============

def send_email(to_email, subject, html_body, cc_emails=None):
    """Send an email using Gmail SMTP."""
    if not SMTP_PASSWORD:
        logger.warning("SMTP_PASSWORD not configured, skipping email")
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"FirstLine Schools Talent <{SMTP_EMAIL}>"
        msg['To'] = to_email
        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)

        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            recipients = [to_email] + (cc_emails or [])
            server.sendmail(SMTP_EMAIL, recipients, msg.as_string())

        logger.info(f"Email sent to {to_email}: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def send_request_confirmation(req):
    """Send confirmation email to requestor when they submit a request."""
    subject = f"Position Control Request Submitted - {req['position_title']}"
    html_body = f"""
    <div style="font-family: 'Open Sans', Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #002f60; padding: 20px; text-align: center;">
            <h1 style="color: white; margin: 0;">Position Control Request</h1>
        </div>
        <div style="padding: 30px; background-color: #f8f9fa;">
            <h2 style="color: #002f60;">Your request has been submitted!</h2>
            <p>Hi {req['requestor_name']},</p>
            <p>We've received your position control request. It will be reviewed by the {'CEO, Finance, Talent, and HR teams' if req.get('request_type') in CEO_FINANCE_REQUIRED_TYPES else 'Talent and HR teams'}.</p>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <p style="margin: 5px 0;"><strong>Request ID:</strong> {req['request_id']}</p>
                <p style="margin: 5px 0;"><strong>Type:</strong> {req['request_type']}</p>
                <p style="margin: 5px 0;"><strong>Position:</strong> {req['position_title']}</p>
                <p style="margin: 5px 0;"><strong>Hours/Status:</strong> {req['hours_status']}</p>
                <p style="margin: 5px 0;"><strong>School Year:</strong> {req['school_year']}</p>
            </div>

            <p><strong>What's next?</strong></p>
            <ul>
                <li>Your request will be reviewed by {'CEO, Finance, Talent, and HR' if req.get('request_type') in CEO_FINANCE_REQUIRED_TYPES else 'Talent and HR'}</li>
                <li>You can check the status of your request at any time on the portal</li>
                <li>You'll be notified once a final decision is made</li>
            </ul>

            <p style="color: #666; font-size: 0.9em; margin-top: 30px;">Questions? Contact <a href="mailto:talent@firstlineschools.org">talent@firstlineschools.org</a></p>
        </div>
        <div style="background-color: #002f60; padding: 15px; text-align: center;">
            <p style="color: white; margin: 0; font-size: 0.9em;">FirstLine Schools - Education For Life</p>
        </div>
    </div>
    """
    send_email(req['requestor_email'], subject, html_body)


def send_new_request_alert(req):
    """Send alert to HR/Talent team when a new request is submitted."""
    subject = f"New Position Control Request: {req['request_type']} - {req['position_title']}"
    html_body = f"""
    <div style="font-family: 'Open Sans', Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #002f60; padding: 20px; text-align: center;">
            <h1 style="color: white; margin: 0;">New Position Control Request</h1>
        </div>
        <div style="padding: 30px; background-color: #f8f9fa;">
            <h2 style="color: #e47727;">New request submitted!</h2>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <h3 style="color: #002f60; margin-top: 0;">Request Details</h3>
                <p style="margin: 5px 0;"><strong>Request ID:</strong> {req['request_id']}</p>
                <p style="margin: 5px 0;"><strong>Type:</strong> {req['request_type']}</p>
                <p style="margin: 5px 0;"><strong>Position:</strong> {req['position_title']}</p>
                <p style="margin: 5px 0;"><strong>Hours/Status:</strong> {req['hours_status']}</p>
                <p style="margin: 5px 0;"><strong>Reports To:</strong> {req.get('reports_to', 'Not specified')}</p>
                <p style="margin: 5px 0;"><strong>School Year:</strong> {req['school_year']}</p>
                {f"<p style='margin: 5px 0;'><strong>Employee:</strong> {req['employee_name']}</p>" if req.get('employee_name') else ""}
                {f"<p style='margin: 5px 0;'><strong>Requested Amount:</strong> {req['requested_amount']}</p>" if req.get('requested_amount') else ""}
            </div>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <h3 style="color: #002f60; margin-top: 0;">Requestor</h3>
                <p style="margin: 5px 0;"><strong>Name:</strong> {req['requestor_name']}</p>
                <p style="margin: 5px 0;"><strong>Email:</strong> {req['requestor_email']}</p>
            </div>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <h3 style="color: #002f60; margin-top: 0;">Justification</h3>
                <p style="margin: 5px 0;">{req['justification']}</p>
            </div>

            <div style="background-color: #fff3cd; border-radius: 8px; padding: 15px; margin: 20px 0;">
                <p style="margin: 0 0 15px 0;"><strong>Action Required:</strong> Please review and approve or deny this request.</p>
                <a href="{APP_URL}/?admin=true" style="display: inline-block; background-color: #e47727; color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; font-weight: 600;">Review in Admin Portal</a>
            </div>
        </div>
        <div style="background-color: #002f60; padding: 15px; text-align: center;">
            <p style="color: white; margin: 0; font-size: 0.9em;">FirstLine Schools - Education For Life</p>
        </div>
    </div>
    """
    cc_emails = [HR_EMAIL]

    # Always CC the CPO
    try:
        q = f"""
        SELECT Email_Address FROM `{PROJECT_ID}.{PC_DATASET_ID}.staff_master_list_with_function`
        WHERE Job_Title = 'Chief People Officer' AND Employment_Status IN ('Active', 'Leave of absence')
        LIMIT 1
        """
        rows = list(bq_client.query(q).result())
        if rows and rows[0].Email_Address:
            cpo_email = rows[0].Email_Address.lower()
            if cpo_email not in cc_emails and cpo_email != TALENT_TEAM_EMAIL:
                cc_emails.append(cpo_email)
    except Exception as e:
        logger.error(f"Error looking up CPO email: {e}")

    # If request type requires CEO/Finance approval, also notify those approvers
    # Look up emails for title-based roles that have CEO/Finance approval rights
    if req.get('request_type') in CEO_FINANCE_REQUIRED_TYPES:
        for title, role_info in TITLE_ROLES.items():
            if any(f in role_info['can_approve'] for f in ['ceo_approval', 'finance_approval']):
                # Look up the email for this title from BigQuery
                try:
                    q = f"""
                    SELECT Email_Address FROM `{PROJECT_ID}.{PC_DATASET_ID}.staff_master_list_with_function`
                    WHERE Job_Title = @title AND Employment_Status IN ('Active', 'Leave of absence')
                    LIMIT 1
                    """
                    jc = bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("title", "STRING", title)]
                    )
                    rows = list(bq_client.query(q, job_config=jc).result())
                    if rows and rows[0].Email_Address:
                        approver_email = rows[0].Email_Address.lower()
                        if approver_email not in cc_emails and approver_email != TALENT_TEAM_EMAIL:
                            cc_emails.append(approver_email)
                except Exception as e:
                    logger.error(f"Error looking up email for title {title}: {e}")

    send_email(TALENT_TEAM_EMAIL, subject, html_body, cc_emails=cc_emails)


def send_status_update(req, new_status):
    """Send status update email to requestor when final_status changes."""
    is_open_position = req.get('request_type') == 'Open Position'

    # Status-specific messaging
    if is_open_position:
        if new_status == 'Approved':
            status_label = 'Vacancy Confirmed'
            status_color = '#22c55e'
            message = f"The vacancy for <strong>{req.get('position_title', 'this position')}</strong> has been confirmed and will be posted."
        elif new_status == 'Denied':
            status_label = 'Vacancy Not Confirmed'
            status_color = '#ef4444'
            message = "After review, this vacancy has not been confirmed at this time."
        else:
            status_label = new_status
            status_color = '#e47727'
            message = f"Your submission status has been updated to: {new_status}"
    else:
        if new_status == 'Approved':
            status_label = 'Approved'
            status_color = '#22c55e'
            message = f"Your position control request for <strong>{req.get('position_title', 'this position')}</strong> has been approved."
        elif new_status == 'Denied':
            status_label = 'Denied'
            status_color = '#ef4444'
            message = f"After careful consideration, your request for <strong>{req.get('position_title', 'this position')}</strong> has not been approved at this time."
        elif new_status == 'Withdrawn':
            status_label = 'Withdrawn'
            status_color = '#ef4444'
            message = "Your position control request has been withdrawn."
        else:
            status_label = new_status
            status_color = '#e47727'
            message = f"Your request status has been updated to: {new_status}"

    subject = f"Position Control Request Update - {status_label}"
    html_body = f"""
    <div style="font-family: 'Open Sans', Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #002f60; padding: 20px; text-align: center;">
            <h1 style="color: white; margin: 0;">Position Control Request Update</h1>
        </div>
        <div style="padding: 30px; background-color: #f8f9fa;">
            <p>Hi {req.get('requestor_name', '')},</p>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0; text-align: center;">
                <p style="margin: 0 0 10px 0;">Your {req.get('request_type', '')} request</p>
                <p style="font-size: 1.5em; color: {status_color}; margin: 0; font-weight: bold;">{status_label}</p>
            </div>

            <p>{message}</p>

            <div style="background-color: white; border-radius: 8px; padding: 15px; margin: 20px 0;">
                <p style="margin: 5px 0;"><strong>Request ID:</strong> {req.get('request_id', '')}</p>
                <p style="margin: 5px 0;"><strong>Type:</strong> {req.get('request_type', '')}</p>
                <p style="margin: 5px 0;"><strong>Position:</strong> {req.get('position_title', '')}</p>
                <p style="margin: 5px 0;"><strong>Subject:</strong> {req.get('subject', '') or 'N/A'}</p>
                <p style="margin: 5px 0;"><strong>Grade Level:</strong> {req.get('grade_level', '') or 'N/A'}</p>
                <p style="margin: 5px 0;"><strong>School:</strong> {req.get('school', '')}</p>
            </div>

            <p style="color: #666; font-size: 0.9em; margin-top: 30px;">Questions? Contact talent@firstlineschools.org</p>
        </div>
        <div style="background-color: #002f60; padding: 15px; text-align: center;">
            <p style="color: white; margin: 0; font-size: 0.9em;">FirstLine Schools - Education For Life</p>
        </div>
    </div>
    """

    # CC list: talent, hr, payroll, benefits, ExDir of Teach and Learn, CPO
    cc_emails = [
        TALENT_TEAM_EMAIL,
        HR_EMAIL,
        'payroll@firstlineschools.org',
        'benefits@firstlineschools.org',
    ]

    # Look up ExDir of Teach and Learn by title
    for title in ['ExDir of Teach and Learn', 'Chief People Officer']:
        try:
            q = f"""
            SELECT Email_Address FROM `{PROJECT_ID}.{PC_DATASET_ID}.staff_master_list_with_function`
            WHERE Job_Title = @title AND Employment_Status IN ('Active', 'Leave of absence')
            LIMIT 1
            """
            jc = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("title", "STRING", title)]
            )
            rows = list(bq_client.query(q, job_config=jc).result())
            if rows and rows[0].Email_Address:
                email = rows[0].Email_Address.lower()
                if email not in cc_emails:
                    cc_emails.append(email)
        except Exception as e:
            logger.error(f"Error looking up {title} email: {e}")

    send_email(req.get('requestor_email', ''), subject, html_body, cc_emails=cc_emails)


# ============ BigQuery Functions ============

def get_full_table_id():
    """Get the fully qualified table ID."""
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def row_to_dict(row):
    """Convert a BigQuery row to a dictionary."""
    return {
        'request_id': row.request_id,
        'submitted_at': row.submitted_at.isoformat() if row.submitted_at else '',
        'requestor_name': row.requestor_name or '',
        'requestor_email': row.requestor_email or '',
        'request_type': row.request_type or '',
        'hours_status': row.hours_status or '',
        'position_title': row.position_title or '',
        'reports_to': row.reports_to or '',
        'requested_amount': row.requested_amount or '',
        'employee_name': row.employee_name or '',
        'justification': row.justification or '',
        'sped_reviewed': row.sped_reviewed or '',
        'school_year': row.school_year or '',
        'duration': row.duration or '',
        'payment_dates': row.payment_dates or '',
        'ceo_approval': row.ceo_approval or '',
        'finance_approval': row.finance_approval or '',
        'talent_approval': row.talent_approval or '',
        'hr_approval': row.hr_approval or '',
        'final_status': row.final_status or '',
        'offer_sent': row.offer_sent.isoformat() if row.offer_sent else '',
        'offer_signed': row.offer_signed.isoformat() if row.offer_signed else '',
        'admin_notes': row.admin_notes or '',
        'position_id': row.position_id or '',
        'updated_at': row.updated_at.isoformat() if row.updated_at else '',
        'updated_by': row.updated_by or '',
        'is_archived': bool(getattr(row, 'is_archived', False) or False),
        'hire_type': getattr(row, 'hire_type', '') or '',
        'employee_email': getattr(row, 'employee_email', '') or '',
        'school': getattr(row, 'school', '') or '',
        'linked_position_id': getattr(row, 'linked_position_id', '') or '',
        'candidate_email': getattr(row, 'candidate_email', '') or '',
        'candidate_position_id': getattr(row, 'candidate_position_id', '') or '',
        'subject': getattr(row, 'subject', '') or '',
        'grade_level': getattr(row, 'grade_level', '') or '',
    }


def read_all_requests():
    """Read all requests from BigQuery."""
    try:
        query = f"""
        SELECT * FROM `{get_full_table_id()}`
        ORDER BY submitted_at DESC
        """
        results = bq_client.query(query).result()
        return [row_to_dict(row) for row in results]
    except Exception as e:
        logger.error(f"Error reading requests: {e}")
        return []


def get_request_by_id(request_id):
    """Get a single request by ID."""
    try:
        query = f"""
        SELECT * FROM `{get_full_table_id()}`
        WHERE request_id = @request_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("request_id", "STRING", request_id)
            ]
        )
        results = bq_client.query(query, job_config=job_config).result()
        for row in results:
            return row_to_dict(row)
        return None
    except Exception as e:
        logger.error(f"Error getting request: {e}")
        return None


def append_request(request_data):
    """Insert a new request into BigQuery using SQL INSERT."""
    try:
        query = f"""
        INSERT INTO `{get_full_table_id()}` (
            request_id, submitted_at, requestor_name, requestor_email,
            request_type, hours_status, position_title, reports_to,
            requested_amount, employee_name, justification, sped_reviewed,
            school_year, duration, payment_dates,
            ceo_approval, finance_approval, talent_approval, hr_approval,
            final_status, offer_sent, offer_signed, admin_notes,
            position_id, updated_at, updated_by, is_archived, hire_type,
            employee_email, school, linked_position_id,
            candidate_email, candidate_position_id,
            subject, grade_level
        ) VALUES (
            @request_id, @submitted_at, @requestor_name, @requestor_email,
            @request_type, @hours_status, @position_title, @reports_to,
            @requested_amount, @employee_name, @justification, @sped_reviewed,
            @school_year, @duration, @payment_dates,
            @ceo_approval, @finance_approval, @talent_approval, @hr_approval,
            @final_status, @offer_sent, @offer_signed, @admin_notes,
            @position_id, @updated_at, @updated_by, @is_archived, @hire_type,
            @employee_email, @school, @linked_position_id,
            @candidate_email, @candidate_position_id,
            @subject, @grade_level
        )
        """

        submitted_at = datetime.fromisoformat(request_data['submitted_at']) if request_data.get('submitted_at') else datetime.now()
        updated_at = datetime.fromisoformat(request_data['updated_at']) if request_data.get('updated_at') else datetime.now()

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("request_id", "STRING", request_data.get('request_id', '')),
                bigquery.ScalarQueryParameter("submitted_at", "TIMESTAMP", submitted_at),
                bigquery.ScalarQueryParameter("requestor_name", "STRING", request_data.get('requestor_name', '')),
                bigquery.ScalarQueryParameter("requestor_email", "STRING", request_data.get('requestor_email', '')),
                bigquery.ScalarQueryParameter("request_type", "STRING", request_data.get('request_type', '')),
                bigquery.ScalarQueryParameter("hours_status", "STRING", request_data.get('hours_status', '')),
                bigquery.ScalarQueryParameter("position_title", "STRING", request_data.get('position_title', '')),
                bigquery.ScalarQueryParameter("reports_to", "STRING", request_data.get('reports_to', '')),
                bigquery.ScalarQueryParameter("requested_amount", "STRING", request_data.get('requested_amount', '')),
                bigquery.ScalarQueryParameter("employee_name", "STRING", request_data.get('employee_name', '')),
                bigquery.ScalarQueryParameter("justification", "STRING", request_data.get('justification', '')),
                bigquery.ScalarQueryParameter("sped_reviewed", "STRING", request_data.get('sped_reviewed', 'N/A')),
                bigquery.ScalarQueryParameter("school_year", "STRING", request_data.get('school_year', '')),
                bigquery.ScalarQueryParameter("duration", "STRING", request_data.get('duration', '')),
                bigquery.ScalarQueryParameter("payment_dates", "STRING", request_data.get('payment_dates', '')),
                bigquery.ScalarQueryParameter("ceo_approval", "STRING", request_data.get('ceo_approval', 'Pending')),
                bigquery.ScalarQueryParameter("finance_approval", "STRING", request_data.get('finance_approval', 'Pending')),
                bigquery.ScalarQueryParameter("talent_approval", "STRING", request_data.get('talent_approval', 'Pending')),
                bigquery.ScalarQueryParameter("hr_approval", "STRING", request_data.get('hr_approval', 'Pending')),
                bigquery.ScalarQueryParameter("final_status", "STRING", request_data.get('final_status', 'Pending')),
                bigquery.ScalarQueryParameter("offer_sent", "DATE", None),
                bigquery.ScalarQueryParameter("offer_signed", "DATE", None),
                bigquery.ScalarQueryParameter("admin_notes", "STRING", request_data.get('admin_notes', '')),
                bigquery.ScalarQueryParameter("position_id", "STRING", request_data.get('position_id', '')),
                bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", updated_at),
                bigquery.ScalarQueryParameter("updated_by", "STRING", request_data.get('updated_by', '')),
                bigquery.ScalarQueryParameter("is_archived", "BOOL", False),
                bigquery.ScalarQueryParameter("hire_type", "STRING", request_data.get('hire_type', '')),
                bigquery.ScalarQueryParameter("employee_email", "STRING", request_data.get('employee_email', '')),
                bigquery.ScalarQueryParameter("school", "STRING", request_data.get('school', '')),
                bigquery.ScalarQueryParameter("linked_position_id", "STRING", request_data.get('linked_position_id', '')),
                bigquery.ScalarQueryParameter("candidate_email", "STRING", request_data.get('candidate_email', '')),
                bigquery.ScalarQueryParameter("candidate_position_id", "STRING", request_data.get('candidate_position_id', '')),
                bigquery.ScalarQueryParameter("subject", "STRING", request_data.get('subject', '')),
                bigquery.ScalarQueryParameter("grade_level", "STRING", request_data.get('grade_level', '')),
            ]
        )

        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        logger.error(f"Error appending request: {e}")
        return False


def update_request(request_id, updates):
    """Update a request in BigQuery using DML."""
    try:
        set_clauses = []
        params = [bigquery.ScalarQueryParameter("request_id", "STRING", request_id)]

        for field, value in updates.items():
            param_name = f"param_{field}"

            if field in ['offer_sent', 'offer_signed']:
                if value:
                    set_clauses.append(f"{field} = @{param_name}")
                    params.append(bigquery.ScalarQueryParameter(param_name, "DATE", value))
                else:
                    set_clauses.append(f"{field} = NULL")
            elif field in ['updated_at', 'submitted_at']:
                set_clauses.append(f"{field} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "TIMESTAMP", datetime.fromisoformat(value)))
            elif field == 'is_archived':
                set_clauses.append(f"{field} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "BOOL", bool(value)))
            else:
                set_clauses.append(f"{field} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))

        if not set_clauses:
            return True

        query = f"""
        UPDATE `{get_full_table_id()}`
        SET {', '.join(set_clauses)}
        WHERE request_id = @request_id
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        bq_client.query(query, job_config=job_config).result()

        return True
    except Exception as e:
        logger.error(f"Error updating request: {e}")
        return False


def require_admin(f):
    """Decorator to require admin authentication (title-based)."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = session.get('user')
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        if not is_admin_user(user.get('email', '').lower()):
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function


# ============ Public Routes ============

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()


@app.route('/')
def index():
    """Serve the main HTML page."""
    return send_file(os.path.join(SCRIPT_DIR, 'index.html'))


@app.route('/api/requests', methods=['POST'])
def submit_request():
    """Submit a new position control request."""
    try:
        data = request.json

        # Validate required fields
        required_fields = ['requestor_name', 'requestor_email', 'request_type',
                          'justification', 'school_year']

        # Open Position: title/hours come from employee lookup, employee_email is required
        if data.get('request_type') == 'Open Position':
            required_fields.append('employee_email')
        else:
            required_fields.append('position_title')

        # hours_status is required for all types except stipends and Open Position
        if data.get('request_type') not in ('Additional Comp (Stipend)', 'Open Position'):
            required_fields.append('hours_status')

        for field in required_fields:
            if not data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400

        # Generate request ID and timestamps
        request_id = str(uuid.uuid4())[:8].upper()
        submitted_at = datetime.now().isoformat()

        # Build request record
        req = {
            'request_id': request_id,
            'submitted_at': submitted_at,
            'requestor_name': data.get('requestor_name', ''),
            'requestor_email': data.get('requestor_email', '').lower(),
            'request_type': data.get('request_type', ''),
            'hours_status': data.get('hours_status', ''),
            'position_title': data.get('position_title', ''),
            'reports_to': data.get('reports_to', ''),
            'requested_amount': data.get('requested_amount', ''),
            'employee_name': data.get('employee_name', ''),
            'justification': data.get('justification', ''),
            'sped_reviewed': data.get('sped_reviewed', 'N/A'),
            'school_year': data.get('school_year', ''),
            'duration': data.get('duration', ''),
            'payment_dates': data.get('payment_dates', ''),
            'hire_type': data.get('hire_type', ''),
            'employee_email': data.get('employee_email', ''),
            'school': data.get('school', ''),
            'linked_position_id': data.get('linked_position_id', ''),
            'candidate_email': data.get('candidate_email', ''),
            'candidate_position_id': data.get('candidate_position_id', ''),
            'ceo_approval': 'Pending',
            'finance_approval': 'Pending',
            'talent_approval': 'Pending',
            'hr_approval': 'Pending',
            'final_status': 'Pending',
            'admin_notes': '',
            'position_id': '',
            'updated_at': submitted_at,
            'updated_by': 'System',
        }

        if append_request(req):
            # Send email notifications
            send_request_confirmation(req)
            send_new_request_alert(req)

            return jsonify({
                'success': True,
                'request_id': request_id,
            })
        else:
            return jsonify({'error': 'Failed to save request'}), 500

    except Exception as e:
        logger.error(f"Error submitting request: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/requests/lookup', methods=['GET'])
def lookup_requests():
    """Look up requests by email."""
    email = request.args.get('email', '').lower().strip()

    if not email:
        return jsonify({'error': 'Email required'}), 400

    all_requests = read_all_requests()

    # Filter to requests by this email
    user_requests = [
        r for r in all_requests
        if r.get('requestor_email', '').lower() == email
    ]

    # Calculate summary stats
    total = len(user_requests)
    pending = len([r for r in user_requests if r.get('final_status') == 'Pending'])
    approved = len([r for r in user_requests if r.get('final_status') == 'Approved'])

    # Remove admin-only fields
    for r in user_requests:
        r.pop('admin_notes', None)

    return jsonify({
        'requests': user_requests,
        'total': total,
        'pending': pending,
        'approved': approved,
    })


@app.route('/api/staff/lookup', methods=['GET'])
def lookup_staff():
    """Look up staff info by email (for auto-fill)."""
    email = request.args.get('email', '').lower().strip()

    if not email:
        return jsonify({'error': 'Email required'}), 400

    all_requests = read_all_requests()

    for r in all_requests:
        if r.get('requestor_email', '').lower() == email:
            return jsonify({
                'found': True,
                'name': r.get('requestor_name', ''),
            })

    return jsonify({'found': False})


@app.route('/api/employee/lookup', methods=['GET'])
def lookup_employee():
    """Look up employee info from position_control table by email."""
    email = request.args.get('email', '').strip()

    if not email:
        return jsonify({'error': 'Email required'}), 400

    try:
        pc_table = f"{PROJECT_ID}.{PC_DATASET_ID}.{PC_TABLE_ID}"
        query = f"""
        SELECT position_id, school, job_category, job_title, subject, grade_level,
               first_name, last_name, email_address, current_status, employee_number
        FROM `{pc_table}`
        WHERE LOWER(TRIM(email_address)) = LOWER(TRIM(@email))
          AND current_status IN ('Active', 'Filled')
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        results = bq_client.query(query, job_config=job_config).result()

        for row in results:
            first = getattr(row, 'first_name', '') or ''
            last = getattr(row, 'last_name', '') or ''

            # Look up supervisor from staff_master_list_with_function
            reports_to = ''
            try:
                sml_table = f"{PROJECT_ID}.{PC_DATASET_ID}.staff_master_list_with_function"
                sup_query = f"""
                SELECT Supervisor_Name__Unsecured_
                FROM `{sml_table}`
                WHERE LOWER(TRIM(Email_Address)) = LOWER(TRIM(@email))
                LIMIT 1
                """
                sup_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("email", "STRING", email)
                    ]
                )
                sup_results = bq_client.query(sup_query, job_config=sup_config).result()
                for sup_row in sup_results:
                    reports_to = getattr(sup_row, 'Supervisor_Name__Unsecured_', '') or ''
            except Exception as e:
                logger.error(f"Error looking up supervisor: {e}")

            return jsonify({
                'found': True,
                'position_id': row.position_id or '',
                'school': row.school or '',
                'job_category': getattr(row, 'job_category', '') or '',
                'job_title': row.job_title or '',
                'first_name': first,
                'last_name': last,
                'employee_name': f"{first} {last}".strip(),
                'current_status': row.current_status or '',
                'subject': getattr(row, 'subject', '') or '',
                'grade_level': getattr(row, 'grade_level', '') or '',
                'reports_to': reports_to,
            })

        return jsonify({'found': False})
    except Exception as e:
        logger.error(f"Error looking up employee: {e}")
        return jsonify({'found': False, 'error': str(e)})


@app.route('/api/job-titles', methods=['GET'])
def get_job_titles():
    """Get distinct job titles from the position_control table for the dropdown."""
    try:
        pc_table = f"{PROJECT_ID}.{PC_DATASET_ID}.{PC_TABLE_ID}"
        query = f"""
        SELECT DISTINCT job_title
        FROM `{pc_table}`
        WHERE job_title IS NOT NULL AND job_title != ''
        ORDER BY job_title
        """
        results = bq_client.query(query).result()
        titles = [row.job_title for row in results]
        return jsonify({'titles': titles})
    except Exception as e:
        logger.error(f"Error fetching job titles: {e}")
        return jsonify({'titles': []})


@app.route('/api/schools', methods=['GET'])
def get_schools():
    """Get distinct schools from the position_control table for the dropdown."""
    try:
        pc_table = f"{PROJECT_ID}.{PC_DATASET_ID}.{PC_TABLE_ID}"
        query = f"""
        SELECT DISTINCT school
        FROM `{pc_table}`
        WHERE school IS NOT NULL AND school != ''
        ORDER BY school
        """
        results = bq_client.query(query).result()
        schools = [row.school for row in results]
        return jsonify({'schools': schools})
    except Exception as e:
        logger.error(f"Error fetching schools: {e}")
        return jsonify({'schools': []})


@app.route('/api/subjects', methods=['GET'])
def get_subjects():
    """Get distinct subjects from the position_control table for the dropdown."""
    try:
        pc_table = f"{PROJECT_ID}.{PC_DATASET_ID}.{PC_TABLE_ID}"
        query = f"""
        SELECT DISTINCT subject
        FROM `{pc_table}`
        WHERE subject IS NOT NULL AND TRIM(subject) != ''
        ORDER BY subject
        """
        results = bq_client.query(query).result()
        subjects = [row.subject for row in results]
        return jsonify({'subjects': subjects})
    except Exception as e:
        logger.error(f"Error fetching subjects: {e}")
        return jsonify({'subjects': []})


@app.route('/api/grade-levels', methods=['GET'])
def get_grade_levels():
    """Get distinct grade levels from the position_control table for the dropdown."""
    try:
        pc_table = f"{PROJECT_ID}.{PC_DATASET_ID}.{PC_TABLE_ID}"
        query = f"""
        SELECT DISTINCT grade_level
        FROM `{pc_table}`
        WHERE grade_level IS NOT NULL AND TRIM(grade_level) != ''
        ORDER BY grade_level
        """
        results = bq_client.query(query).result()
        grade_levels = [row.grade_level for row in results]
        return jsonify({'grade_levels': grade_levels})
    except Exception as e:
        logger.error(f"Error fetching grade levels: {e}")
        return jsonify({'grade_levels': []})


# ============ Auth Routes ============

@app.route('/login')
def login():
    """Initiate Google OAuth."""
    if not google:
        return jsonify({'error': 'OAuth not configured'}), 500
    redirect_uri = url_for('auth_callback', _external=True)
    # Force new-format Cloud Run URL so OAuth callback matches registered URI
    redirect_uri = redirect_uri.replace('daem7b6ydq-uc.a.run.app', '965913991496.us-central1.run.app')
    return google.authorize_redirect(redirect_uri)


@app.route('/auth/callback')
def auth_callback():
    """Handle OAuth callback."""
    if not google:
        return jsonify({'error': 'OAuth not configured'}), 500

    try:
        token = google.authorize_access_token()
        user_info = token.get('userinfo')

        if user_info:
            email = user_info.get('email', '').lower()
            job_title = lookup_job_title(email)
            session['user'] = {
                'email': email,
                'name': user_info.get('name'),
                'picture': user_info.get('picture'),
                'job_title': job_title,
            }

        # Redirect admins to admin view, everyone else to form
        if is_admin_user(email):
            return redirect('/?admin=true')
        return redirect('/')
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        return redirect('/?error=auth_failed')


@app.route('/logout')
def logout():
    """Clear session."""
    session.clear()
    return redirect('/')


@app.route('/api/auth/status')
def auth_status():
    """Check authentication status and return role-based permissions."""
    user = session.get('user')
    if user:
        email = user.get('email', '').lower()
        # Refresh job title if not in session (handles cached sessions)
        if not user.get('job_title'):
            user['job_title'] = lookup_job_title(email)
            session['user'] = user
        admin = is_admin_user(email)
        permissions = get_user_permissions(email) if admin else None
        return jsonify({
            'authenticated': True,
            'is_admin': admin,
            'user': user,
            'permissions': permissions,
            'ceo_finance_required_types': CEO_FINANCE_REQUIRED_TYPES,
            'position_action_types': POSITION_ACTION_TYPES,
        })
    return jsonify({'authenticated': False, 'is_admin': False, 'permissions': None})


# ============ Admin Routes ============

@app.route('/api/admin/requests', methods=['GET'])
@require_admin
def get_all_requests():
    """Get all requests (admin only)."""
    requests_list = read_all_requests()
    return jsonify({'requests': requests_list})


@app.route('/api/admin/requests/<request_id>', methods=['PATCH'])
@require_admin
def update_request_status(request_id):
    """Update a request (admin only, role-based permissions enforced)."""
    try:
        data = request.json
        user = session.get('user', {})
        email = user.get('email', '').lower()
        perms = get_user_permissions(email)

        if not perms:
            return jsonify({'error': 'No permissions configured for this user'}), 403

        # Get current request state (for detecting status changes)
        current_req = get_request_by_id(request_id)
        if not current_req:
            return jsonify({'error': 'Request not found'}), 404

        updates = {}

        # Handle approval fields — only allow fields the user has permission for
        for field in ['ceo_approval', 'finance_approval', 'talent_approval', 'hr_approval']:
            if field in data:
                if field not in perms['can_approve']:
                    return jsonify({'error': f'You do not have permission to set {field}'}), 403
                if data[field] not in ['Pending', 'Approved', 'Denied']:
                    return jsonify({'error': f'Invalid value for {field}'}), 400
                updates[field] = data[field]

        # Handle final status — only if user has permission
        if 'final_status' in data:
            if not perms['can_edit_final']:
                return jsonify({'error': 'You do not have permission to set final status'}), 403
            if data['final_status'] not in ['Pending', 'Approved', 'Denied', 'Withdrawn']:
                return jsonify({'error': 'Invalid final_status'}), 400
            updates['final_status'] = data['final_status']

        # Handle date fields — only HR and super admin
        for field in ['offer_sent', 'offer_signed']:
            if field in data:
                if not perms['can_edit_dates']:
                    return jsonify({'error': 'You do not have permission to edit offer dates'}), 403
                updates[field] = data[field]

        # Handle admin notes — all admins can add notes
        if 'admin_notes' in data:
            updates['admin_notes'] = data['admin_notes']

        # Handle request detail fields — all non-viewer admins can edit
        detail_fields = [
            'request_type', 'hours_status', 'position_title', 'reports_to',
            'school_year', 'employee_name', 'employee_email', 'school',
            'hire_type', 'justification', 'requested_amount', 'duration',
            'subject', 'grade_level'
        ]
        for field in detail_fields:
            if field in data:
                if perms['is_viewer']:
                    return jsonify({'error': 'Viewers cannot edit request details'}), 403
                updates[field] = data[field]

        # Always update audit fields
        updates['updated_at'] = datetime.now().isoformat()
        updates['updated_by'] = user.get('email', 'Unknown')

        if update_request(request_id, updates):
            # Send status update email when final_status changes
            new_status = updates.get('final_status')
            old_status = current_req.get('final_status')
            if new_status and new_status != old_status and new_status != 'Pending':
                # Merge updates into current request for email content
                updated_req = {**current_req, **updates}
                try:
                    send_status_update(updated_req, new_status)
                except Exception as e:
                    logger.error(f"Failed to send status update email: {e}")
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Request not found'}), 404

    except Exception as e:
        logger.error(f"Error updating request: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/admin/requests/<request_id>', methods=['DELETE'])
@require_admin
def delete_request(request_id):
    """Permanently delete a request (super admin only)."""
    try:
        email = session.get('user', {}).get('email', '').lower()
        perms = get_user_permissions(email)
        if not perms or not perms['can_delete']:
            return jsonify({'error': 'Only super admins can delete requests'}), 403
        query = f"""
        DELETE FROM `{get_full_table_id()}`
        WHERE request_id = @request_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("request_id", "STRING", request_id)
            ]
        )
        bq_client.query(query, job_config=job_config).result()
        logger.info(f"Deleted request {request_id}")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error deleting request: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/admin/requests/<request_id>/archive', methods=['PATCH'])
@require_admin
def archive_request(request_id):
    """Archive a request (admin only)."""
    try:
        query = f"""
        UPDATE `{get_full_table_id()}`
        SET is_archived = TRUE
        WHERE request_id = @request_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("request_id", "STRING", request_id)
            ]
        )
        bq_client.query(query, job_config=job_config).result()
        logger.info(f"Archived request {request_id}")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error archiving request: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/admin/requests/<request_id>/unarchive', methods=['PATCH'])
@require_admin
def unarchive_request(request_id):
    """Unarchive a request (admin only)."""
    try:
        query = f"""
        UPDATE `{get_full_table_id()}`
        SET is_archived = FALSE
        WHERE request_id = @request_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("request_id", "STRING", request_id)
            ]
        )
        bq_client.query(query, job_config=job_config).result()
        logger.info(f"Unarchived request {request_id}")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error unarchiving request: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/admin/stats', methods=['GET'])
@require_admin
def get_stats():
    """Get dashboard statistics (admin only)."""
    all_requests = read_all_requests()
    # Exclude archived from stats
    requests_list = [r for r in all_requests if not r.get('is_archived')]

    total = len(requests_list)
    pending = len([r for r in requests_list if r.get('final_status') == 'Pending'])
    approved = len([r for r in requests_list if r.get('final_status') == 'Approved'])
    denied = len([r for r in requests_list if r.get('final_status') == 'Denied'])

    # Awaiting offer: approved but no offer_sent date
    awaiting_offer = len([
        r for r in requests_list
        if r.get('final_status') == 'Approved' and not r.get('offer_sent')
    ])

    return jsonify({
        'total': total,
        'pending': pending,
        'approved': approved,
        'denied': denied,
        'awaiting_offer': awaiting_offer,
    })


@app.route('/api/admin/requests/<request_id>/create-position', methods=['POST'])
@require_admin
def create_position(request_id):
    """Create a position in the position_control table from an approved request."""
    try:
        email = session.get('user', {}).get('email', '').lower()
        perms = get_user_permissions(email)
        if not perms or not perms['can_create_position']:
            return jsonify({'error': 'You do not have permission to create positions'}), 403

        # Get the request
        req = get_request_by_id(request_id)
        if not req:
            return jsonify({'error': 'Request not found'}), 404

        if req.get('final_status') != 'Approved':
            return jsonify({'error': 'Request must be fully approved before creating a position'}), 400

        if req.get('position_id'):
            return jsonify({'error': 'Position already created for this request', 'position_id': req['position_id']}), 400

        user = session.get('user', {})
        now = datetime.now().isoformat()
        pc_table = f"{PROJECT_ID}.{PC_DATASET_ID}.{PC_TABLE_ID}"

        # Map school year to start_year format
        school_year = req.get('school_year', '')
        start_year = school_year.replace(' SY', '') if school_year else '25-26'

        linked_id = req.get('linked_position_id', '')
        request_type = req.get('request_type', '')
        action_type = POSITION_ACTION_TYPES.get(request_type)

        if not action_type:
            return jsonify({'error': f'No position action defined for request type "{request_type}"'}), 400

        # Path A: Linked position exists and request is Open Position — UPDATE existing row
        if linked_id and request_type == 'Open Position':
            position_id = linked_id

            pc_query = f"""
            UPDATE `{pc_table}` SET
                current_status = 'Open',
                candidate_name = '',
                employee_26_27 = '',
                status_26_27 = 'Open',
                notes = CONCAT(COALESCE(notes, ''), '\\nVacated via PCF request {request_id}'),
                updated_at = CURRENT_TIMESTAMP(),
                updated_by = @updated_by
            WHERE position_id = @linked_position_id
            """

            pc_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("linked_position_id", "STRING", linked_id),
                    bigquery.ScalarQueryParameter("updated_by", "STRING", user.get('email', 'system')),
                ]
            )

            bq_client.query(pc_query, job_config=pc_config).result()
            logger.info(f"Updated position {position_id} to Open from request {request_id}")

        # Path C: Update types (Status Change, Title/Role Change) — UPDATE existing employee position
        elif action_type == 'update':
            employee_email = req.get('employee_email', '')
            if not employee_email:
                return jsonify({'error': 'No employee email on this request; cannot look up position'}), 400

            # Find the employee's position by email
            lookup_query = f"""
            SELECT position_id FROM `{pc_table}`
            WHERE email_address = @employee_email
            LIMIT 1
            """
            lookup_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("employee_email", "STRING", employee_email),
                ]
            )
            lookup_results = list(bq_client.query(lookup_query, job_config=lookup_config).result())
            if not lookup_results:
                return jsonify({'error': f'No position found for employee {employee_email}'}), 404

            position_id = lookup_results[0].position_id
            note_suffix = f"Updated via PCF request {request_id}"

            if request_type == 'Status Change':
                new_status = req.get('hours_status', '')
                pc_query = f"""
                UPDATE `{pc_table}` SET
                    current_status = @new_status,
                    status_26_27 = @new_status,
                    notes = CONCAT(COALESCE(notes, ''), '\\n{note_suffix}'),
                    updated_at = CURRENT_TIMESTAMP(),
                    updated_by = @updated_by
                WHERE position_id = @position_id
                """
                pc_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("new_status", "STRING", new_status),
                        bigquery.ScalarQueryParameter("updated_by", "STRING", user.get('email', 'system')),
                        bigquery.ScalarQueryParameter("position_id", "STRING", position_id),
                    ]
                )
            elif request_type == 'Title/Role Change':
                pc_query = f"""
                UPDATE `{pc_table}` SET
                    job_title = @job_title,
                    subject = @subject,
                    grade_level = @grade_level,
                    notes = CONCAT(COALESCE(notes, ''), '\\n{note_suffix}'),
                    updated_at = CURRENT_TIMESTAMP(),
                    updated_by = @updated_by
                WHERE position_id = @position_id
                """
                pc_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("job_title", "STRING", req.get('position_title', '')),
                        bigquery.ScalarQueryParameter("subject", "STRING", req.get('subject', '')),
                        bigquery.ScalarQueryParameter("grade_level", "STRING", req.get('grade_level', '')),
                        bigquery.ScalarQueryParameter("updated_by", "STRING", user.get('email', 'system')),
                        bigquery.ScalarQueryParameter("position_id", "STRING", position_id),
                    ]
                )

            bq_client.query(pc_query, job_config=pc_config).result()
            logger.info(f"Updated position {position_id} ({request_type}) from request {request_id}")

        # Path B: Create types with no linked position — INSERT new row
        else:
            position_id = str(uuid.uuid4())

            pc_query = f"""
            INSERT INTO `{pc_table}` (
                position_id, school, job_title, current_status,
                start_year, notes, candidate_name, created_at, updated_at, updated_by
            ) VALUES (
                @position_id, @school, @job_title, @current_status,
                @start_year, @notes, @candidate_name, @created_at, @updated_at, @updated_by
            )
            """

            pc_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("position_id", "STRING", position_id),
                    bigquery.ScalarQueryParameter("school", "STRING", req.get('school', '')),
                    bigquery.ScalarQueryParameter("job_title", "STRING", req.get('position_title', '')),
                    bigquery.ScalarQueryParameter("current_status", "STRING", "Open"),
                    bigquery.ScalarQueryParameter("start_year", "STRING", start_year),
                    bigquery.ScalarQueryParameter("notes", "STRING", f"Created from PCF request {request_id}"),
                    bigquery.ScalarQueryParameter("candidate_name", "STRING", req.get('employee_name', '')),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now()),
                    bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", datetime.now()),
                    bigquery.ScalarQueryParameter("updated_by", "STRING", user.get('email', 'system')),
                ]
            )

            bq_client.query(pc_query, job_config=pc_config).result()
            logger.info(f"Created position {position_id} from request {request_id}")

        # Update the request with the position_id
        update_request(request_id, {
            'position_id': position_id,
            'updated_at': now,
            'updated_by': user.get('email', 'system'),
        })

        if action_type == 'update' or (linked_id and request_type == 'Open Position'):
            action = 'updated'
        else:
            action = 'created'
        cascade_request_id = None

        # Auto-create cascading Open Position request if candidate_position_id is set
        candidate_pos_id = req.get('candidate_position_id', '')
        if candidate_pos_id:
            try:
                # Query position_control for the candidate's current position details
                pc_query = f"""
                SELECT position_id, school, job_title, first_name, last_name, email_address
                FROM `{pc_table}`
                WHERE position_id = @candidate_position_id
                LIMIT 1
                """
                pc_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("candidate_position_id", "STRING", candidate_pos_id),
                    ]
                )
                pc_results = bq_client.query(pc_query, job_config=pc_config).result()
                candidate_row = None
                for row in pc_results:
                    candidate_row = row

                if candidate_row:
                    first = getattr(candidate_row, 'first_name', '') or ''
                    last = getattr(candidate_row, 'last_name', '') or ''
                    candidate_name = f"{first} {last}".strip()

                    cascade_request_id = str(uuid.uuid4())[:8].upper()
                    cascade_req = {
                        'request_id': cascade_request_id,
                        'submitted_at': now,
                        'requestor_name': req.get('requestor_name', ''),
                        'requestor_email': req.get('requestor_email', ''),
                        'request_type': 'Open Position',
                        'hours_status': '',
                        'position_title': getattr(candidate_row, 'job_title', '') or '',
                        'reports_to': '',
                        'requested_amount': '',
                        'employee_name': candidate_name,
                        'employee_email': getattr(candidate_row, 'email_address', '') or '',
                        'school': getattr(candidate_row, 'school', '') or '',
                        'linked_position_id': candidate_pos_id,
                        'justification': f"Auto-generated: {candidate_name} promoted/transferred to {req.get('position_title', '')} via request {request_id}",
                        'sped_reviewed': 'N/A',
                        'school_year': req.get('school_year', ''),
                        'duration': '',
                        'payment_dates': '',
                        'hire_type': '',
                        'candidate_email': '',
                        'candidate_position_id': '',
                        'ceo_approval': 'Pending',
                        'finance_approval': 'Pending',
                        'talent_approval': 'Pending',
                        'hr_approval': 'Pending',
                        'final_status': 'Pending',
                        'admin_notes': f'Cascading request from {request_id}',
                        'position_id': '',
                        'updated_at': now,
                        'updated_by': 'System',
                    }
                    append_request(cascade_req)
                    logger.info(f"Created cascading Open Position request {cascade_request_id} for candidate position {candidate_pos_id}")
            except Exception as e:
                logger.error(f"Error creating cascading request: {e}")

        return jsonify({
            'success': True,
            'position_id': position_id,
            'action': action,
            'cascade_request_id': cascade_request_id,
        })

    except Exception as e:
        logger.error(f"Error creating position: {e}")
        return jsonify({'error': f'Failed to create position: {str(e)}'}), 500


# ============ Health Check ============

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
