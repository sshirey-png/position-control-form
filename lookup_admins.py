from google.cloud import bigquery
client = bigquery.Client(project='talent-demo-482004')
query = """
SELECT first_name, last_name, email_address, job_title
FROM `talent-demo-482004.talent_grow_observations.position_control`
WHERE email_address IN (
    'sshirey@firstlineschools.org', 'brichardson@firstlineschools.org',
    'spence@firstlineschools.org', 'mtoussaint@firstlineschools.org',
    'csmith@firstlineschools.org', 'aleibfritz@firstlineschools.org',
    'rcain@firstlineschools.org', 'lhunter@firstlineschools.org'
)
ORDER BY job_title
"""
results = client.query(query).result()
for row in results:
    print(f"{row.email_address} | {row.first_name} {row.last_name} | {row.job_title}")
