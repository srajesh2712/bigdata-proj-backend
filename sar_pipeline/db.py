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

def insert_start_time(conn):
    start_time = datetime.now()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO sar_processing_log (start_time) VALUES (%s) RETURNING id", (start_time,))
        log_id = cur.fetchone()[0]
        conn.commit()
    return log_id, start_time

def update_end_time(conn, log_id):
    end_time = datetime.now()
    with conn.cursor() as cur:
        cur.execute("UPDATE sar_processing_log SET end_time = %s WHERE id = %s", (end_time, log_id))
        conn.commit()
    return end_time

