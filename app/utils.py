import psycopg2
from flask import json

conn_info = {
    "host": "pg_container",
    "database": "zoovbikes",
    "user": "postgres",
    "password": "pass"
}

def select_to_json(select_sql, args=None):
    conn = psycopg2.connect(**conn_info)
    cursor = conn.cursor()
    cursor.execute(select_sql, args)
    columns = [desc[0] for desc in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    cursor.close()
    conn.close()

    return results
