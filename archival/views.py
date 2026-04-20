import json
import uuid
import pyodbc
from django.db import IntegrityError
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Q

from archival.core import archive_module, archive_table_batch
from .models import Application, ArchivalModule, ArchivalTable, AuditLog, DatabaseConnection, ArchivalTransaction, ArchivalTransactionDetail
from .utils import get_connection, notify_application_completion, run_test_script
from django.utils.dateparse import parse_date
from datetime import date
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User, Group
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.contrib.auth.models import Permission
from django.contrib.auth import logout

from archival import models


def custom_logout(request):
    logout(request)
    return redirect('login')

@login_required
def admin_dashboard(request):
    users = User.objects.all()
    groups = Group.objects.all()
    connections = DatabaseConnection.objects.all()
    applications = Application.objects.all()
    modules = ArchivalModule.objects.all()
    tables = ArchivalTable.objects.all()
    return render(request, 'admin/admin_dashboard.html', {
        'users': users,
        'groups': groups,
        'connections': connections,
        'applications': applications,
        'modules': modules,
        'tables': tables,
    })


def is_admin(user):
    return user.is_superuser or user.groups.filter(name='admin').exists()

@login_required
@user_passes_test(is_admin)
def group_add(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        group = Group.objects.create(name=name)
        perm_ids = request.POST.getlist('permissions')
        group.permissions.set(perm_ids)
        messages.success(request, 'Group added.')
        return redirect('group_list')
    # All permissions, ordered nicely
    permissions = Permission.objects.select_related('content_type').order_by('content_type__app_label', 'codename')
    return render(request, 'admin/group_form.html', {
        'action': 'Add',
        'available_perms': permissions,
        'chosen_perms': [],
        'group': None
    })

@login_required
@user_passes_test(is_admin)
def group_edit(request, pk):
    group = get_object_or_404(Group, pk=pk)
    if request.method == 'POST':
        group.name = request.POST.get('name')
        perm_ids = request.POST.getlist('permissions')
        group.permissions.set(perm_ids)
        group.save()
        messages.success(request, 'Group updated.')
        return redirect('group_list')
    all_perms = Permission.objects.select_related('content_type').order_by('content_type__app_label', 'codename')
    assigned_perms = group.permissions.all()
    available_perms = all_perms.exclude(id__in=[p.id for p in assigned_perms])
    return render(request, 'admin/group_form.html', {
        'action': 'Edit',
        'group': group,
        'available_perms': available_perms,
        'chosen_perms': assigned_perms
    })

@login_required
@user_passes_test(is_admin)
def group_delete(request, pk):
    group = get_object_or_404(Group, pk=pk)
    group.delete()
    messages.success(request, 'Group deleted.')
    return redirect('group_list')


@login_required
@user_passes_test(is_admin)
def user_list(request):
    users = User.objects.all()
    search = request.GET.get('search', '')
    if search:
        users = users.filter(username__icontains=search)
    return render(request, 'admin/user_list.html', {'users': users})

@login_required
@user_passes_test(is_admin)
def user_add(request):
    if request.method == 'POST':
        try:
            username = request.POST.get('username')
            password = request.POST.get('password')
            email = request.POST.get('email')
            is_superuser = request.POST.get('is_superuser') == 'on'
            is_active = request.POST.get('is_active') == 'on'
            if User.objects.filter(username=username).exists():
                messages.error(request, f'User with username "{username}" already exists.')
                return redirect('user_add')
            User.objects.create_user(username=username, password=password, email=email, is_active=is_active, is_superuser=is_superuser)
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add User',
                module='User Management',
                details=f"Added user '{username}' with email '{email}' and superuser status {is_superuser}",
                success=True
                )
            messages.success(request, 'User added.')
            return redirect('user_list')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add User',
                module='User Management',
                details=f"Error adding user: {str(e)}",
                success=False
                )
            messages.error(request, f'Error adding user: {e}')
       
    return render(request, 'admin/user_form.html', {'action': 'Add', 'form_user': None})

@login_required
@user_passes_test(is_admin)
def user_edit(request, pk):
    user = get_object_or_404(User, pk=pk)
    if request.method == 'POST':
        try:
            user.username = request.POST.get('username')
            user.email = request.POST.get('email')
            if request.POST.get('password'):
                user.set_password(request.POST.get('password'))
            user.is_superuser = request.POST.get('is_superuser') == 'on'
            user.is_active = request.POST.get('is_active') == 'on'
            user.save()
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add User',
                module='User Management',
                details=f"Updated user '{user.username}' with email '{user.email}' and superuser status {user.is_superuser}",
                success=True
                )
            messages.success(request, 'User updated.')
            return redirect('user_list')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add User',
                module='User Management',
                details=f"Error updating user: {str(e)}",
                success=False
                )
            messages.error(request, f'Error updating user: {e}')
    
    return render(request, 'admin/user_form.html', {'action': 'Edit', 'form_user': user})

@login_required
@user_passes_test(is_admin)
def user_delete(request, pk):
    user = get_object_or_404(User, pk=pk)
    try:
        user.delete()
        if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Delete User',
                module='User Management',
                details=f"Deleted user '{user.username}'",
                success=True
                )
        messages.success(request, 'User deleted.')
    except Exception as e:
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Delete User',
                module='User Management',
                details=f"Error deleting user: {str(e)}",
                success=False
            )
        messages.error(request, f'Error deleting user: {e}')
    return redirect('user_list')

@login_required
@user_passes_test(is_admin)
def group_list(request):
    groups = Group.objects.all()
    return render(request, 'admin/group_list.html', {'groups': groups})

@csrf_exempt
@login_required
def update_module_date(request, module_id):
    if request.method == 'POST':
        module = get_object_or_404(ArchivalModule, id=module_id)
        date_str = request.POST.get('archival_date')
        if date_str:
            parsed_date = parse_date(date_str)
            if parsed_date:
                module.last_archival_date = parsed_date
                module.save()
                if request.user:
                    AuditLog.objects.create(
                    user=request.user,
                    action='Update Module Date',
                    module='Last Archival Date Update',
                    details=f"Updated module '{module.name}' with new archival date: {date_str}",
                    success=True
                    )
                return JsonResponse({'status': 'success', 'new_date': date_str})
            else:
                if request.user:
                    AuditLog.objects.create(
                    user=request.user,
                    action='Update Module Date',
                    module='Last Archival Date Update',
                    details=f"Failed to update module '{module.name}' with invalid date format: {date_str}",
                    success=False
                    )
                return JsonResponse({'status': 'error', 'message': 'Invalid date format'}, status=400)
        return JsonResponse({'status': 'error', 'message': 'No date provided'}, status=400)
    return JsonResponse({'error': 'Method not allowed'}, status=405)

@login_required
def home(request):
    users = User.objects.all()
    groups = Group.objects.all()
    connections = DatabaseConnection.objects.all()
    applications = Application.objects.all()
    modules = ArchivalModule.objects.all()
    tables = ArchivalTable.objects.all()
    return render(request, 'admin/admin_dashboard.html', {
        'users': users,
        'groups': groups,
        'connections': connections,
        'applications': applications,
        'modules': modules,
        'tables': tables,
    })


# ----- Connection Management -----
@login_required
def connection_list(request):
    connections = DatabaseConnection.objects.all()
    search = request.GET.get('search', '')
    if search:
        connections = connections.filter(server__icontains=search) | connections.filter(database__icontains=search) | connections.filter(username__icontains=search)
    return render(request, 'archival/connection_list.html', {'connections': connections})

@login_required
def connection_add(request):
    if request.method == 'POST':          
        server = request.POST.get('server')
        database = request.POST.get('database')
        username = request.POST.get('username')
        password = request.POST.get('password')
        # Validate required fields
        if not all([server, database, username, password]):
            messages.error(request, 'All fields are required.')
            return redirect('connection_add')

        try:        
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                "Trusted_Connection=no;"
                "Connection Timeout=5;"  
            )            
            test_conn = pyodbc.connect(conn_str)
            test_conn.close()
        except pyodbc.Error as e:
            error_msg = str(e) 
            if "Login failed" in error_msg:
                messages.error(request, "Invalid username or password.")
            elif "cannot open database" in error_msg:
                messages.error(request, f"Database '{database}' does not exist or is inaccessible.")
            elif "Server is not found or not accessibler" in error_msg or "network-related" in error_msg:
                messages.error(request, "Please enter a valid server address. (Unable to reach the server)")
            else:
                messages.error(request, f"Unable to connect to the database. Please check the details. {error_msg}")
            return render(request, 'archival/connection_form.html', {'action': 'Add', 'form_data': request.POST})

        try:    
            DatabaseConnection.objects.create(
                server=server,
                database=database,
                username=username,
                password=password
            )
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add Connection',
                module='Connection Management',
                details=f"Added connection for server '{server}' and database '{database}'",
                success=True
                )
            messages.success(request, 'Connection added.')
            return redirect('connection_list')
        except IntegrityError:
            messages.error(request, 'A connection with the same server and database already exists.')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add Connection',
                module='Connection Management',
                details=f"Error adding connection for server '{server}' and database '{database}': {str(e)}",
                success=False
                )
            messages.error(request, f'Error adding connection: {e}')
    return render(request, 'archival/connection_form.html', {'action': 'Add'})

@login_required
def connection_edit(request, pk):
    connection = get_object_or_404(DatabaseConnection, pk=pk)
    if request.method == 'POST':
        connection.server = request.POST.get('server')
        connection.database = request.POST.get('database')
        connection.username = request.POST.get('username')
        new_password = request.POST.get('password')
        if new_password:   
            connection.password = new_password

        try:        
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={connection.server};"
                f"DATABASE={connection.database};"
                f"UID={connection.username};"
                f"PWD={connection.password};"
                "Trusted_Connection=no;"
                "Connection Timeout=5;"  
            )            
            test_conn = pyodbc.connect(conn_str)
            test_conn.close()
        except pyodbc.Error as e:
            error_msg = str(e) 
            if "Login failed" in error_msg:
                messages.error(request, "Invalid username or password.")
            elif "cannot open database" in error_msg:
                messages.error(request, f"Database '{connection.database}' does not exist or is inaccessible.")
            elif "Server is not found or not accessibler" in error_msg or "network-related" in error_msg:
                messages.error(request, "Please enter a valid server address. (Unable to reach the server)")
            else:
                messages.error(request, f"Unable to connect to the database. Please check the details. {error_msg}")
            return render(request, 'archival/connection_form.html', {'action': 'Edit', 'connection': connection})
        try:
            connection.save()
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Edit Connection',
                module='Connection Management',
                details=f"Edited connection for server '{connection.server}' and database '{connection.database}'",
                success=True
                )
            messages.success(request, 'Connection updated.')
            return redirect('connection_list')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Edit Connection',
                module='Connection Management',
                details=f"Error updating connection for server '{connection.server}' and database '{connection.database}': {str(e)}",
                success=False
                )
            messages.error(request, f'Error updating connection: {e}')
    return render(request, 'archival/connection_form.html', {'action': 'Edit', 'connection': connection})

@login_required
def connection_delete(request, pk):
    try:
        connection = get_object_or_404(DatabaseConnection, pk=pk)
        module=ArchivalModule.objects.filter(application__src_conn=connection).first() or ArchivalModule.objects.filter(application__dstn_conn=connection).first()
        if module:
            messages.error(request, f'Cannot delete connection because it is associated with application "{module.application.name}". Please update or delete the application first.')
            return redirect('connection_list')
        else:
            connection.delete()
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Delete Connection',
                module='Connection Management',
                details=f"Deleted connection for server '{connection.server}' and database '{connection.database}'",
                success=True
                )
            messages.success(request, 'Connection deleted.')
    except Exception as e:
        if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Delete Connection',
                module='Connection Management',
                details=f"Error deleting connection for server '{connection.server}' and database '{connection.database}': {str(e)}",
                success=False
                )
        messages.error(request, f'Error deleting connection: {e}')
    return redirect('connection_list')

# ---- Application CRUD ----
@login_required
def application_list(request):
    apps = Application.objects.all()
    connection = DatabaseConnection.objects.all()
    search = request.GET.get('search', '')
    if search:
        apps = apps.filter(name__icontains=search)

    return render(request, 'archival/application_list.html', {'apps': apps, 'connections': connection})

@login_required
def application_add(request):
    if request.method == 'POST':
        try:
            name = request.POST.get('name')
            source_id = request.POST.get('src_conn')
            dest_id = request.POST.get('dstn_conn')            
            max_date = request.POST.get('max_date')
            app = Application.objects.create(
            name=name,
            src_conn_id=source_id or None,
            dstn_conn_id=dest_id or None,            
            max_date=max_date
            )
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add Application',
                module='Application Management',
                details=f"Added application '{name}'",
                success=True
                )
            messages.success(request, 'Application added.')
            return redirect('application_list')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add Application',
                module='Application Management',
                details=f"Error adding application '{name}': {str(e)}",
                success=False
                )
            messages.error(request, f'Error adding application: {e}')
    conns = DatabaseConnection.objects.all()
    return render(request, 'archival/application_form.html', {
        'action': 'Add',
        'sources': conns,
        'dests': conns,
        'transfer_choices': Application.TRANSFER_CHOICES
    })

@login_required
def application_edit(request, pk):
    app = get_object_or_404(Application, pk=pk)
    if request.method == 'POST':
        try:
            app.name = request.POST.get('name')
            app.src_conn_id = request.POST.get('src_conn') or None
            app.dstn_conn_id = request.POST.get('dstn_conn') or None
            app.max_date = request.POST.get('max_date')
            app.save()
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Edit Application',
                module='Application Management',
                details=f"Edited application '{app.name}'",
                success=True
                )
            messages.success(request, 'Application updated.')
            return redirect('application_list')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Edit Application',
                module='Application Management',
                details=f"Error editing application '{app.name}': {str(e)}",
                success=False
                )
            messages.error(request, f'Error updating application: {e}')
    conns = DatabaseConnection.objects.all()
    return render(request, 'archival/application_form.html', {
        'action': 'Edit',
        'app': app,
        'sources': conns,
        'dests': conns,
        'transfer_choices': Application.TRANSFER_CHOICES
    })

@login_required
def application_delete(request, pk):
    app = get_object_or_404(Application, pk=pk)
    try:
        app.delete()
        if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Delete Application',
                module='Application Management',
                details=f"Deleted application '{app.name}'",
                success=True
                )
        messages.success(request, 'Application deleted.')
    except Exception as e:
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Delete Application',
                module='Application Management',
                details=f"Error deleting application '{app.name}': {str(e)}",
                success=False
            )
        messages.error(request, f'Error deleting application: {e}')
    
    return redirect('application_list')

#---- Module Run ----
@login_required
def module_run(request, app_id):
    # apps = Application.objects.all()
    apps= get_object_or_404(Application, pk=app_id)
    today = date.today().isoformat()
    return render(request, 'archival/module_run.html', {'apps': apps, 'today': today})

# ----- Module CRUD -----
@login_required
def module_list(request, app_id):
    app = get_object_or_404(Application, pk=app_id)
    try:
        modules = app.modules.all()
    except Exception as e:
        messages.error(request, f'Error fetching modules: {e}')
        modules = []
    search = request.GET.get('search', '')
    if search:
        modules = modules.filter(name__icontains=search)
    return render(request, 'archival/module_list.html', {'app': app, 'modules': modules})

@login_required
def module_add(request, app_id):
    app = get_object_or_404(Application, pk=app_id)
    if request.method == 'POST':
        name = request.POST.get('name')
        last_date = request.POST.get('last_archival_date')
        try:
            module = ArchivalModule.objects.create(
                application=app,
                name=name,
                last_archival_date=last_date
            )
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add Module',
                module='Module Management',
                details=f"Added module '{name}' to application '{app.name}'",
                success=True
                )
            messages.success(request, 'Module added.')
            return redirect('module_list', app_id=app.id)
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add Module',
                module='Module Management',
                details=f"Error adding module '{name}' to application '{app.name}': {str(e)}",
                success=False
                )
            messages.error(request, f'Error adding module: {e}')
    return render(request, 'archival/module_form.html', {'app': app, 'action': 'Add'})

@login_required
def module_edit(request, app_id, pk):
    module = get_object_or_404(ArchivalModule, pk=pk, application_id=app_id)
    if request.method == 'POST':
        try:
            module.name = request.POST.get('name')
            module.last_archival_date = request.POST.get('last_archival_date')
            module.save()
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Edit Module',
                module='Module Management',
                details=f"Edited module '{module.name}' in application '{module.application.name}'",
                success=True
                )
            messages.success(request, 'Module updated.')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Edit Module',
                module='Module Management',
                details=f"Error editing module '{module.name}' in application '{module.application.name}': {str(e)}",
                success=False
                )
            messages.error(request, f'Error updating module: {e}')
        return redirect('module_list', app_id=app_id)
    return render(request, 'archival/module_form.html', {'app': module.application, 'module': module, 'action': 'Edit'})

@login_required
def module_delete(request, app_id, pk):
    module = get_object_or_404(ArchivalModule, pk=pk, application_id=app_id)
    try:        
        module.delete()
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Delete Module',
                module='Module Management',
                details=f"Deleted module '{module.name}' from application '{module.application.name}'",
                success=True
            )
    except Exception as e:
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Delete Module',
                module='Module Management',
                details=f"Error deleting module '{module.name}' from application '{module.application.name}': {str(e)}",
                success=False
            )
        messages.error(request, f'Error deleting module: {e}')
    return redirect('module_list', app_id=app_id)


# ----- Table CRUD (nested under module) -----
@login_required
def table_list(request, module_id):
    module = get_object_or_404(ArchivalModule, pk=module_id)
    tables = module.tables.all()
    search = request.GET.get('search', '')
    if search:
        tables = tables.filter(table_name__icontains=search)
    return render(request, 'archival/table_list.html', {'module': module, 'tables': tables})

@login_required
def table_add(request, module_id):
    module = get_object_or_404(ArchivalModule, pk=module_id)
    if request.method == 'POST':
        try:
            table = ArchivalTable.objects.create(
            module=module,
            table_name=request.POST.get('table_name'),
            sequence=request.POST.get('sequence'),
            select_script=request.POST.get('select_script'),
            insert_script=request.POST.get('insert_script'),
            delete_script=request.POST.get('delete_script'),
            acct_sum=request.POST.get('acct_sum'),
            # identity_insert=request.POST.get('identity_insert') == 'on'
            )
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Add Table',
                module='Table Management',
                details=f"Added table '{table.table_name}' to module '{module.name}' in application '{module.application.name}'",
                success=True
                )
            messages.success(request, 'Table added.')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                    user=request.user,
                    action='Add Table',
                    module='Table Management',
                    details=f"Error adding table '{table.table_name}' to module '{module.name}' in application '{module.application.name}': {str(e)}",
                    success=False
                )
            messages.error(request, f'Error adding table: {e}')
        return redirect('table_list', module_id=module.id)
    return render(request, 'archival/table_form.html', {'module': module, 'action': 'Add'})

@login_required
def table_edit(request, module_id, pk):
    table = get_object_or_404(ArchivalTable, pk=pk, module_id=module_id)
    if request.method == 'POST':
        try:
            table.table_name = request.POST.get('table_name')
            table.sequence = request.POST.get('sequence')
            table.select_script = request.POST.get('select_script')
            table.insert_script = request.POST.get('insert_script')
            table.delete_script = request.POST.get('delete_script')
            table.acct_sum = request.POST.get('acct_sum')
            # table.identity_insert = request.POST.get('identity_insert') == 'on'
            table.save()
            if request.user:
                AuditLog.objects.create(
                user=request.user,
                action='Edit Table',
                module='Table Management',
                details=f"Edited table '{table.table_name}' in module '{table.module.name}' of application '{table.module.application.name}'",
                success=True
                )
            messages.success(request, 'Table updated.')
        except Exception as e:
            if request.user:
                AuditLog.objects.create(
                    user=request.user,
                    action='Edit Table',
                    module='Table Management',
                    details=f"Error editing table '{table.table_name}' in module '{table.module.name}' of application '{table.module.application.name}': {str(e)}",
                    success=False
                )

            messages.error(request, f'Error updating table: {e}')
        
        return redirect('table_list', module_id=module_id)
    return render(request, 'archival/table_form.html', {'module': table.module, 'table': table, 'action': 'Edit'})

@login_required
def table_delete(request, module_id, pk):
    table = get_object_or_404(ArchivalTable, pk=pk, module_id=module_id)
    try:
        table.delete()
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Delete Table',
                module='Table Management',
                details=f"Deleted table '{table.table_name}' from module '{table.module.name}' in application '{table.module.application.name}'",
                success=True
            )

        messages.success(request, 'Table deleted.')
    except Exception as e:
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Delete Table',
                module='Table Management',
                details=f"Error deleting table '{table.table_name}' from module '{table.module.name}' in application '{table.module.application.name}': {str(e)}",
                success=False
            )
        messages.error(request, f'Error deleting table: {e}')
    return redirect('table_list', module_id=module_id)


@login_required
def get_module_tables(request, module_id):    
    module = get_object_or_404(ArchivalModule, id=module_id)
    if module.status == 'In Progress':        
        return JsonResponse({'status': 'error', 'error': 'Module is currently running. Please wait until it completes.'}, status=400)
    module.status = 'In Progress'
    module.save()   
    tables = module.tables.all().values('id', 'table_name', 'sequence')
    return JsonResponse(list(tables), safe=False)

@csrf_exempt
@login_required
def run_table_script(request, table_id):
    if request.method == 'POST':
        table = get_object_or_404(ArchivalTable, id=table_id)
        archival_date = request.POST.get('archival_date')
        if not archival_date:
            return JsonResponse({'status': 'error', 'error': 'No archival date provided'}, status=400)

        result = archive_table_batch(table, archival_date, user=request.user)
        # print (f"Script execution result for table {table.table_name}: {result}")
        return JsonResponse(result)
    
    return JsonResponse({'error': 'Method not allowed'}, status=405)

# @csrf_exempt
# @login_required
# def complete_archival(request, module_id):
#     if request.method == 'POST':
#         archival_date = request.POST.get('archival_date')
#     if not archival_date:
#         return JsonResponse({'status': 'error', 'error': 'No date provided'}, status=400)
#     result = archive_module(module_id, archival_date)
#     return JsonResponse(result)

@login_required
def get_table_count(request, table_id):
    if request.method != 'GET':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    table = get_object_or_404(ArchivalTable, id=table_id)
    archival_date = request.GET.get('archival_date')
    if not archival_date:
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Get Table Count',
                module='Table Management',
                details=f"Failed to get count for table '{table.table_name}' in module '{table.module.name}' of application '{table.module.application.name}' due to missing archival date",
                success=False
            )
        return JsonResponse({'error': 'No archival date provided'}, status=400)

    select_sql = table.select_script.format(archival_date=archival_date)    
    count_sql = f"SELECT COUNT(*) FROM ({select_sql}) AS subquery"

    app = table.module.application
    try:
        src_conn = get_connection(app.src_conn.name)
        with src_conn.cursor() as cursor:
            cursor.execute(count_sql)
            count = cursor.fetchone()[0]
        src_conn.close()
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Get Table Count',
                module='Table Management',
                details=f"Retrieved count for table '{table.table_name}' in module '{table.module.name}' of application '{table.module.application.name}' for archival date {archival_date}: {count}",
                success=True
            )
        return JsonResponse({'status': 'success', 'count': count})
    except Exception as e:
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Get Table Count',
                module='Table Management',
                details=f"Error getting count for table '{table.table_name}' in module '{table.module.name}' of application '{table.module.application.name}': {str(e)}",
                success=False
            )
        return JsonResponse({'status': 'error', 'error': str(e)}, status=500)

@csrf_exempt
def update_module_date(request, module_id):
    if request.method == 'POST':
        module = get_object_or_404(ArchivalModule, id=module_id)
        date_str = request.POST.get('archival_date')
        if date_str:
            parsed_date = parse_date(date_str)
            if parsed_date:
                module.last_archival_date = parsed_date
                module.status = 'Completed'
                module.save()
                if request.user:
                    AuditLog.objects.create(
                    user=request.user,
                    action='Update Module Date',
                    module='Last Archival Date Update',
                    details=f"Updated module '{module.name}' with new archival date: {date_str}",
                    success=True
                    )
                notify_application_completion(module.application)
                return JsonResponse({'status': 'success', 'new_date': date_str})
            else:
                if request.user:
                    AuditLog.objects.create(
                    user=request.user,
                    action='Update Module Date',
                    module='Last Archival Date Update',
                    details=f"Failed to update module '{module.name}' with invalid date format: {date_str}",
                    success=False
                    )
                return JsonResponse({'status': 'error', 'message': 'Invalid date'}, status=400)
        return JsonResponse({'status': 'error', 'message': 'No date'}, status=400)
    return JsonResponse({'error': 'Method not allowed'}, status=405)

@csrf_exempt
@login_required
def reset_module_status(request, module_id):
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    module = get_object_or_404(ArchivalModule, id=module_id)
    module.status = 'nothing'
    module.save()
    return JsonResponse({'status': 'success'})


# ----- Archival History -----
@csrf_exempt
@login_required
def save_archival_history(request, module_id):
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    try:
        module = get_object_or_404(ArchivalModule, id=module_id)
        data = json.loads(request.body)
        archival_date = data.get('archival_date')
        total_time = data.get('total_time')
        results = data.get('results', [])
        if not archival_date:
            return JsonResponse({'status': 'error', 'error': 'Missing archival_date'}, status=400)
        transaction = ArchivalTransaction.objects.create(
            module=module,
            userName=request.user.username,
            archival_date=archival_date,
            total_execution_time=total_time
        )
        for res in results:
            ArchivalTransactionDetail.objects.create(
                transaction=transaction,
                table_name=res.get('table_name', ''),
                total_rows_archived=res.get('row_archived', 0),
                total_rows_inserted=res.get('row_inserted', 0),
                total_rows_deleted=res.get('row_deleted', 0),
                total_rows_merged=res.get('rows_merged', 0),
                execution_time=res.get('execution_time', 0),
                status=res.get('status', 'unknown'),
                error_message=res.get('error_message', '')
            )

        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Save Archival History',
                module='Archival History',
                details=f"Saved archival history for module '{module.name}' in application '{module.application.name}' for archival date {archival_date}",
                success=True
            )
    except Exception as e:
        if request.user:
            AuditLog.objects.create(
                user=request.user,
                action='Save Archival History',
                module='Archival History',
                details=f"Error saving archival history for module '{module.name}' in application '{module.application.name}': {str(e)}",
                success=False
            )    
        return JsonResponse({'status': 'error', 'error': str(e)}, status=500)

    return JsonResponse({'status': 'success'})

# def transaction_list(request, app_id):
#     application = get_object_or_404(Application, id=app_id)
#     transactions = ArchivalTransaction.objects.filter(module__application=application)
#     search = request.GET.get('search', '')
#     if search:
#         transactions = transactions.filter(userName__icontains=search) | transactions.filter(module__name__icontains=search)
#     # print(transactions)
#     return render(request, 'archival/Transaction.html', {'transactions': transactions})

def transaction_detail(request, transaction_id):
    transaction = get_object_or_404(ArchivalTransaction, id=transaction_id)
    details = transaction.details.all()
    search = request.GET.get('search', '')
    if search:
        details = details.filter(table_name__icontains=search)
    # print(details)
    return render(request, 'archival/transaction_details.html', {'transaction': transaction, 'details': details})

def transaction_list(request):
    applications = Application.objects.all()
    
    # Get selected app_id from GET, or default to first application if exists
    app_id = request.GET.get('app_id')
    selected_application = None
    transactions = ArchivalTransaction.objects.none()
    
    if app_id:
        selected_application = get_object_or_404(Application, id=app_id)
        transactions = ArchivalTransaction.objects.filter(module__application=selected_application)
    elif applications.exists():
        # Default to first application
        selected_application = applications.first()
        transactions = ArchivalTransaction.objects.filter(module__application=selected_application)
        app_id = selected_application.id  # for the hidden field or URL
    
    # Search filter (if any)
    search = request.GET.get('search', '')
    if search and transactions:
        transactions = transactions.filter(
            Q(userName__icontains=search) | Q(module__name__icontains=search)
        )
    
    context = {
        'applications': applications,
        'selected_application': selected_application,
        'transactions': transactions,
        'search': search,
    }
    return render(request, 'archival/transaction.html', context)


def audit_log(request):
    logs = AuditLog.objects.all().order_by('-timestamp')
    search = request.GET.get('search', '')
    if search:
        logs = logs.filter(
            Q(user__username__icontains=search) |
            Q(action__icontains=search) |
            Q(module__icontains=search) |
            Q(details__icontains=search)
           )
    return render(request, 'archival/audit_log.html', {'logs': logs, 'search': search})