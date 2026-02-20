# Position Control Request Form - User Guide

## Overview

The Position Control Request Form is a web application for FirstLine Schools staff to submit staffing requests (new hires, stipends, role changes, etc.) and for administrators to manage the approval process.

## For Staff Members

### Submitting a Request

1. Navigate to the Position Control Request Form
2. Fill out **Your Information** (name and email)
3. Complete **Request Details**:
   - Select the type of request
   - Choose the school year (25-26 SY or 26-27 SY)
   - Select hours/status
   - Enter the position title
   - Optionally fill in Reports To, Requested Amount, and Employee Name
4. Provide a **Justification** explaining why this request is needed
5. If the request is SPED-related, check the SPED checkbox and answer the review question
6. For stipend requests, enter Requested Payment Dates
7. Click **Submit Request**

You'll receive a confirmation email with your Request ID.

### Checking Request Status

1. Scroll to the **My Requests** section
2. Enter your email address
3. Click **Look Up**
4. View your submitted requests and their approval status

## For Administrators

### Accessing the Admin Panel

1. Click **Admin Login** in the header
2. Sign in with your FirstLine Schools Google account
3. You'll be redirected to the admin view

### Managing Requests

The admin dashboard shows:
- **Stats Cards**: Total, Pending, Approved, Denied, and Awaiting Offer counts
- **Filters**: Search by name/email/position, filter by status or type, toggle archived
- **Requests Table**: All requests with approval status indicators

### Approval Icons

In the table, approvals are shown as compact icons:
- **C** = CEO, **F** = Finance, **T** = Talent, **H** = HR
- Green checkmark = Approved
- Red X = Denied
- Gray circle = Pending

### Editing a Request

1. Click **Edit** on any request row
2. The modal shows read-only request details and editable approval fields
3. Set each approval (CEO, Finance, Talent, HR) to Pending, Approved, or Denied
4. The Final Status auto-calculates:
   - All 4 approved = "Approved"
   - Any denied = "Denied"
5. Set Offer Sent / Offer Signed dates when applicable
6. Add Admin Notes
7. Click **Save Changes**

### Creating a Position

When a request is fully approved (Final Status = Approved):
1. Open the edit modal
2. A green "Create Position" button appears
3. Click it to create a new position in the Position Control table
4. The position ID will be linked to the request

### Archive / Delete

- **Archive**: Hides from default view but preserves data (can be unarchived)
- **Delete**: Permanently removes the request (requires double confirmation)

## Deployment

### Local Development

```bash
pip install -r requirements.txt
export FLASK_DEBUG=true
python app.py
```

### Create BigQuery Table

```bash
python setup_bigquery.py
```

### Deploy to Cloud Run

```bash
gcloud run deploy position-control-form --source . --region us-central1 --allow-unauthenticated
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GOOGLE_CLIENT_ID` | OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth client secret |
| `SMTP_PASSWORD` | Gmail app password for email notifications |
| `SECRET_KEY` | Flask session secret key |
| `GOOGLE_CLOUD_PROJECT` | GCP project ID (default: talent-demo-482004) |
