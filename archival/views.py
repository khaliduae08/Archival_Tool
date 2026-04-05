import json
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt

from archival.core import archive_module, archive_table_batch
from .models import Application, ArchivalModule, ArchivalTable, DatabaseConnection, ArchivalTransaction, ArchivalTransactionDetail
from .utils import get_connection, run_test_script
from django.utils.dateparse import parse_date
from datetime import date
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User, Group
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.contrib.auth.models import Permission
from django.contrib.auth import logout


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
    return render(request, 'admin/user_list.html', {'users': users})

@login_required
@user_passes_test(is_admin)
def user_add(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        email = request.POST.get('email')
        group_id = request.POST.get('group')
        user = User.objects.create_user(username=username, password=password, email=email)
        if group_id:
            group = Group.objects.get(id=group_id)
            user.groups.add(group)
        messages.success(request, 'User added.')
        return redirect('user_list')
    groups = Group.objects.all()
    return render(request, 'admin/user_form.html', {'action': 'Add', 'groups': groups})

@login_required
@user_passes_test(is_admin)
def user_edit(request, pk):
    user = get_object_or_404(User, pk=pk)
    if request.method == 'POST':
        user.username = request.POST.get('username')
        user.email = request.POST.get('email')
        if request.POST.get('password'):
            user.set_password(request.POST.get('password'))
        group_id = request.POST.get('group')
        user.groups.clear()
        if group_id:
            group = Group.objects.get(id=group_id)
            user.groups.add(group)
        user.save()
        messages.success(request, 'User updated.')
        return redirect('user_list')
    groups = Group.objects.all()
    return render(request, 'admin/user_form.html', {'action': 'Edit', 'user': user, 'groups': groups})

@login_required
@user_passes_test(is_admin)
def user_delete(request, pk):
    user = get_object_or_404(User, pk=pk)
    user.delete()
    messages.success(request, 'User deleted.')
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
                return JsonResponse({'status': 'success', 'new_date': date_str})
            else:
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

@login_required
def get_module_tables(request, module_id):
    """Return list of tables for a module (used in popup)."""
    module = get_object_or_404(ArchivalModule, id=module_id)
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
                
        result = archive_table_batch(table, archival_date)
        print (f"Script execution result for table {table.table_name}: {result}")
        return JsonResponse(result)
    
    return JsonResponse({'error': 'Method not allowed'}, status=405)

@csrf_exempt
@login_required
def complete_archival(request, module_id):
    if request.method == 'POST':
        archival_date = request.POST.get('archival_date')
    if not archival_date:
        return JsonResponse({'status': 'error', 'error': 'No date provided'}, status=400)
    result = archive_module(module_id, archival_date)
    return JsonResponse(result)

@login_required
def get_table_count(request, table_id):
    if request.method != 'GET':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    table = get_object_or_404(ArchivalTable, id=table_id)
    archival_date = request.GET.get('archival_date')
    if not archival_date:
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
        return JsonResponse({'status': 'success', 'count': count})
    except Exception as e:
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
                module.save()
                return JsonResponse({'status': 'success', 'new_date': date_str})
            else:
                return JsonResponse({'status': 'error', 'message': 'Invalid date'}, status=400)
        return JsonResponse({'status': 'error', 'message': 'No date'}, status=400)
    return JsonResponse({'error': 'Method not allowed'}, status=405)

# ----- Connection Management -----
@login_required
def connection_list(request):
    connections = DatabaseConnection.objects.all()
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
        DatabaseConnection.objects.create(
            server=server,
            database=database,
            username=username,
            password=password
        )
        messages.success(request, 'Connection added.')
        return redirect('connection_list')
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
        connection.save()
        messages.success(request, 'Connection updated.')
        return redirect('connection_list')
    return render(request, 'archival/connection_form.html', {'action': 'Edit', 'connection': connection})

@login_required
def connection_delete(request, pk):
    connection = get_object_or_404(DatabaseConnection, pk=pk)
    connection.delete()
    messages.success(request, 'Connection deleted.')
    return redirect('connection_list')

# ---- Application CRUD ----
@login_required
def application_list(request):
    apps = Application.objects.all()
    connection = DatabaseConnection.objects.all()
    return render(request, 'archival/application_list.html', {'apps': apps, 'connections': connection})

@login_required
def application_add(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        source_id = request.POST.get('src_conn')
        dest_id = request.POST.get('dstn_conn')
        volume = request.POST.get('volume')
        select_session = request.POST.get('select_session')
        target_session = request.POST.get('target_session')
        transfer_method = request.POST.get('transfer_method')
        max_date = request.POST.get('max_date')
        app = Application.objects.create(
            name=name,
            src_conn_id=source_id or None,
            dstn_conn_id=dest_id or None,
            volume=volume,
            select_session=select_session,
            target_session=target_session,
            transfer_method=transfer_method,
            max_date=max_date
        )
        messages.success(request, 'Application added.')
        return redirect('application_list')
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
        app.name = request.POST.get('name')
        app.src_conn_id = request.POST.get('src_conn') or None
        app.dstn_conn_id = request.POST.get('dstn_conn') or None
        app.volume = request.POST.get('volume')
        app.select_session = request.POST.get('select_session')
        app.target_session = request.POST.get('target_session')
        app.transfer_method = request.POST.get('transfer_method')
        app.max_date = request.POST.get('max_date')
        app.save()
        messages.success(request, 'Application updated.')
        return redirect('application_list')
    sources = DatabaseConnection.objects.filter(name='source')
    dests = DatabaseConnection.objects.filter(name='destination')
    return render(request, 'archival/application_form.html', {
        'action': 'Edit',
        'app': app,
        'sources': sources,
        'dests': dests,
        'transfer_choices': Application.TRANSFER_CHOICES
    })

@login_required
def application_delete(request, pk):
    app = get_object_or_404(Application, pk=pk)
    app.delete()
    messages.success(request, 'Application deleted.')
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
    modules = app.modules.all()
    return render(request, 'archival/module_list.html', {'app': app, 'modules': modules})

@login_required
def module_add(request, app_id):
    app = get_object_or_404(Application, pk=app_id)
    if request.method == 'POST':
        name = request.POST.get('name')
        last_date = request.POST.get('last_archival_date')
        module = ArchivalModule.objects.create(
            application=app,
            name=name,
            last_archival_date=last_date
        )
        messages.success(request, 'Module added.')
        return redirect('module_list', app_id=app.id)
    return render(request, 'archival/module_form.html', {'app': app, 'action': 'Add'})

@login_required
def module_edit(request, app_id, pk):
    module = get_object_or_404(ArchivalModule, pk=pk, application_id=app_id)
    if request.method == 'POST':
        module.name = request.POST.get('name')
        module.last_archival_date = request.POST.get('last_archival_date')
        module.save()
        messages.success(request, 'Module updated.')
        return redirect('module_list', app_id=app_id)
    return render(request, 'archival/module_form.html', {'app': module.application, 'module': module, 'action': 'Edit'})

@login_required
def module_delete(request, app_id, pk):
    module = get_object_or_404(ArchivalModule, pk=pk, application_id=app_id)
    module.delete()
    messages.success(request, 'Module deleted.')
    return redirect('module_list', app_id=app_id)


# ----- Table CRUD (nested under module) -----
@login_required
def table_list(request, module_id):
    module = get_object_or_404(ArchivalModule, pk=module_id)
    tables = module.tables.all()
    return render(request, 'archival/table_list.html', {'module': module, 'tables': tables})

@login_required
def table_add(request, module_id):
    module = get_object_or_404(ArchivalModule, pk=module_id)
    if request.method == 'POST':
        table = ArchivalTable.objects.create(
            module=module,
            table_name=request.POST.get('table_name'),
            sequence=request.POST.get('sequence'),
            select_script=request.POST.get('select_script'),
            insert_script=request.POST.get('insert_script'),
            delete_script=request.POST.get('delete_script'),
            acct_sum=request.POST.get('acct_sum'),
            identity_insert=request.POST.get('identity_insert') == 'on'
        )
        messages.success(request, 'Table added.')
        return redirect('table_list', module_id=module.id)
    return render(request, 'archival/table_form.html', {'module': module, 'action': 'Add'})

@login_required
def table_edit(request, module_id, pk):
    table = get_object_or_404(ArchivalTable, pk=pk, module_id=module_id)
    if request.method == 'POST':
        table.table_name = request.POST.get('table_name')
        table.sequence = request.POST.get('sequence')
        table.select_script = request.POST.get('select_script')
        table.insert_script = request.POST.get('insert_script')
        table.delete_script = request.POST.get('delete_script')
        table.acct_sum = request.POST.get('acct_sum')
        table.identity_insert = request.POST.get('identity_insert') == 'on'
        table.save()
        print(f"Updated table: {table.table_name}, acct_sum: {table.acct_sum}, identity_insert: {table.identity_insert}")
        messages.success(request, 'Table updated.')
        return redirect('table_list', module_id=module_id)
    return render(request, 'archival/table_form.html', {'module': table.module, 'table': table, 'action': 'Edit'})

@login_required
def table_delete(request, module_id, pk):
    table = get_object_or_404(ArchivalTable, pk=pk, module_id=module_id)
    table.delete()
    messages.success(request, 'Table deleted.')
    return redirect('table_list', module_id=module_id)


# ----- Archival History -----
@csrf_exempt
@login_required
def save_archival_history(request, module_id):
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    module = get_object_or_404(ArchivalModule, id=module_id)
    data = json.loads(request.body)
    archival_date = data.get('archival_date')
    total_time = data.get('total_time')
    table_results = data.get('results', [])
    
    if not archival_date:
        return JsonResponse({'status': 'error', 'error': 'Missing archival_date'}, status=400)
    
    transaction = ArchivalTransaction.objects.create(
        module=module,
        userName=request.user.username,
        archival_date=archival_date,
        total_execution_time=total_time
    )
    for res in table_results:
        ArchivalTransactionDetail.objects.create(
            transaction=transaction,
            table_name=res['table_name'],
            row_count=res.get('row_count', 0),
            execution_time=res.get('execution_time', 0),
            status=res.get('status', 'unknown'),
            error_message=res.get('error_message', '')
        )
    return JsonResponse({'status': 'success'})

def transaction_list(request):
    transactions = ArchivalTransaction.objects.all()
    return render(request, 'archival/Transaction.html', {'transactions': transactions})

def transaction_detail(request, transaction_id):
    transaction = get_object_or_404(ArchivalTransaction, id=transaction_id)
    details = transaction.details.all()
    return render(request, 'archival/transaction_detail.html', {'transaction': transaction, 'details': details})

