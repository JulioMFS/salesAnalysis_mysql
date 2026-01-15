import mysql.connector
from config import MYSQL_CONFIG

def get_connection():
    """Return a new MySQL connection."""
    return mysql.connector.connect(**MYSQL_CONFIG)

def execute_query(query, params=None, fetch=False):
    """
    Execute a single query safely.
    fetch=True -> returns all results.
    fetch=False -> commits the transaction.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        if fetch:
            result = cursor.fetchall()
        else:
            conn.commit()
            result = None
        return result
    except mysql.connector.Error as e:
        print("MySQL error:", e)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
