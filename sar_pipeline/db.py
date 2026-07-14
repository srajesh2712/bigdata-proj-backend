import psycopg2
from datetime import datetime


from sar_pipeline.schema.schema import SafeFile

# Update these as per your local setup
DB_PARAMS = {
    'dbname': 'eo',
    'user': 'rajesh',
    'password': 'rajesh',
    'host': 'localhost',
    'port': '5432',
    'options':'-csearch_path=sar',
}

def connect_db():
    return psycopg2.connect(**DB_PARAMS)

def insert_start_time(action):
    conn = connect_db()
    start_time = datetime.now()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO sar_processing_log (start_time,action) VALUES (%s,%s) RETURNING id", (start_time,action))
        log_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
    return log_id, start_time

def update_end_time(log_id):
    conn = connect_db()
    end_time = datetime.now()
    with conn.cursor() as cur:
        cur.execute("UPDATE sar_processing_log SET end_time = %s WHERE id = %s", (end_time, log_id))
        conn.commit()
        conn.close()
    return end_time

def fetch_processing_files(session):
    pending_files = session.query(SafeFile).filter_by(active=True, status='pending').all()
    return pending_files


def update_processing_files_by_jobid(session,job_id, new_status='completed'):
    # Get all files with the given job_id
    files_to_update = session.query(SafeFile).filter_by(id=job_id).all()

    for file in files_to_update:
        print(f'updating {file}')
        file.status = new_status  # e.g., 'in_progress', 'completed', 'failed'

    session.commit()
    print(f"Updated {len(files_to_update)} files to status '{new_status}' for job_id '{job_id}'")

def insert_job(status):
    conn = connect_db()
    start_time = datetime.now()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO eo_jobs (status) VALUES (%s) RETURNING id", [status])
        job_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
    return job_id
