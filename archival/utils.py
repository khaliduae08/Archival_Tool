import pyodbc
from .models import DatabaseConnection

def get_connection(conn_type):
    """Return a pyodbc connection for the given type ('source' or 'destination')."""
    try:
        conn = DatabaseConnection.objects.get(name=conn_type)
    except DatabaseConnection.DoesNotExist:
        raise Exception(f"No {conn_type} connection configured.")

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={conn.server};"
        f"DATABASE={conn.database};"
        f"UID={conn.username};"
        f"PWD={conn.password};"
    )
    return pyodbc.connect(conn_str)


def run_test_script(table):
    """
    Temporary function to simulate script execution.
    In real scenario, use get_connection and execute the table.script.
    """
    # For now, just return success
    return {'status': 'success', 'message': f'Test script executed for {table.table_name}'}