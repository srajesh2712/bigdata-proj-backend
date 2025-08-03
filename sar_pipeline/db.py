import psycopg2
from datetime import datetime

# Update these as per your local setup
DB_PARAMS = {
    'dbname': 'eo',
    'user': 'rajesh',
    'password': 'rajesh',
    'host': 'localhost',
    'port': '5432'
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

