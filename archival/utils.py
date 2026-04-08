import pyodbc

from mf_archival import settings
from .models import DatabaseConnection, ArchivalModule
from django.core.mail import send_mail

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

def notify_application_completion(application):

    modules = application.modules.all()
    if all(module.last_archival_date >= application.max_date for module in modules):
        subject = f"All modules archived for application: {application.name}"
        message = (
            f"The application '{application.name}' has successfully archived all its modules.\n\n"
            f"Modules:\n" + "\n".join([f"- {m.name} (last archived: {m.last_archival_date})" for m in modules])
        )
        recipient_list = getattr(settings, 'ARCHIVAL_NOTIFICATION_EMAILS', [])
        if recipient_list:
            send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, recipient_list, fail_silently=False)

            
def run_test_script(table):
    """
    Temporary function to simulate script execution.
    In real scenario, use get_connection and execute the table.script.
    """
    
    return {'status': 'success', 'message': f'Test script executed for {table.table_name}'}