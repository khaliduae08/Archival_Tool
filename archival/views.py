import json
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from .models import Application, ArchivalModule, ArchivalTable, DatabaseConnection
from .utils import run_test_script
from django.utils.dateparse import parse_date
from datetime import date

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
                return JsonResponse({'status': 'error', 'message': 'Invalid date format'}, status=400)
        return JsonResponse({'status': 'error', 'message': 'No date provided'}, status=400)
    return JsonResponse({'error': 'Method not allowed'}, status=405)

def home(request):
    modules = ArchivalModule.objects.all()
    today = date.today().isoformat()  # 'YYYY-MM-DD'
    return render(request, 'archival/home.html', {'modules': modules, 'today': today})

def get_module_tables(request, module_id):
    """Return list of tables for a module (used in popup)."""
    module = get_object_or_404(ArchivalModule, id=module_id)
    tables = module.tables.all().values('id', 'table_name', 'sequence')
    return JsonResponse(list(tables), safe=False)

@csrf_exempt
def run_table_script(request, table_id):
    if request.method == 'POST':
        table = get_object_or_404(ArchivalTable, id=table_id)
        archival_date = request.POST.get('archival_date')  # can be used later
        result = run_test_script(table)  # your test logic
        return JsonResponse(result)
    return JsonResponse({'error': 'Method not allowed'}, status=405)

# ----- Connection Management -----
def edit_connections(request):
    source = DatabaseConnection.objects.filter(name='source').first()
    dest = DatabaseConnection.objects.filter(name='destination').first()
    if request.method == 'POST':
        # Save source
        source = source or DatabaseConnection(name='source')
        source.server = request.POST.get('source_server')
        source.database = request.POST.get('source_database')
        source.username = request.POST.get('source_username')
        source.password = request.POST.get('source_password')
        source.save()
        # Save destination
        dest = dest or DatabaseConnection(name='destination')
        dest.server = request.POST.get('dest_server')
        dest.database = request.POST.get('dest_database')
        dest.username = request.POST.get('dest_username')
        dest.password = request.POST.get('dest_password')
        dest.save()
        messages.success(request, 'Connections updated successfully.')
        return redirect('connections')
    return render(request, 'archival/connections.html', {'source': source, 'dest': dest})
# ---- Application CRUD ----
def application_list(request):
    apps = Application.objects.all()
    return render(request, 'archival/application_list.html', {'apps': apps})

def application_add(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        source_id = request.POST.get('src_conn')
        dest_id = request.POST.get('dstn_conn')
        volume = request.POST.get('volume')
        select_session = request.POST.get('select_session')
        target_session = request.POST.get('target_session')
        transfer_method = request.POST.get('transfer_method')
        app = Application.objects.create(
            name=name,
            src_conn_id=source_id or None,
            dstn_conn_id=dest_id or None,
            volume=volume,
            select_session=select_session,
            target_session=target_session,
            transfer_method=transfer_method,
        )
        messages.success(request, 'Application added.')
        return redirect('application_list')
    sources = DatabaseConnection.objects.filter(name='source')
    dests = DatabaseConnection.objects.filter(name='destination')
    return render(request, 'archival/application_form.html', {
        'action': 'Add',
        'sources': sources,
        'dests': dests,
        'transfer_choices': Application.TRANSFER_CHOICES
    })

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

def application_delete(request, pk):
    app = get_object_or_404(Application, pk=pk)
    app.delete()
    messages.success(request, 'Application deleted.')
    return redirect('application_list')

# ---- Home view ----
def home(request):
    apps = Application.objects.all()
    today = date.today().isoformat()
    return render(request, 'archival/home.html', {'apps': apps, 'today': today})
# ----- Module CRUD -----
def module_list(request, app_id):
    app = get_object_or_404(Application, pk=app_id)
    modules = app.modules.all()
    return render(request, 'archival/module_list.html', {'app': app, 'modules': modules})

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

def module_edit(request, app_id, pk):
    module = get_object_or_404(ArchivalModule, pk=pk, application_id=app_id)
    if request.method == 'POST':
        module.name = request.POST.get('name')
        module.last_archival_date = request.POST.get('last_archival_date')
        module.save()
        messages.success(request, 'Module updated.')
        return redirect('module_list', app_id=app_id)
    return render(request, 'archival/module_form.html', {'app': module.application, 'module': module, 'action': 'Edit'})

def module_delete(request, app_id, pk):
    module = get_object_or_404(ArchivalModule, pk=pk, application_id=app_id)
    module.delete()
    messages.success(request, 'Module deleted.')
    return redirect('module_list', app_id=app_id)

# ----- Table CRUD (nested under module) -----
def table_list(request, module_id):
    module = get_object_or_404(ArchivalModule, pk=module_id)
    tables = module.tables.all()
    return render(request, 'archival/table_list.html', {'module': module, 'tables': tables})

def table_add(request, module_id):
    module = get_object_or_404(ArchivalModule, pk=module_id)
    if request.method == 'POST':
        table = ArchivalTable.objects.create(
            module=module,
            table_name=request.POST.get('table_name'),
            sequence=request.POST.get('sequence'),
            select_script=request.POST.get('select_script'),
            insert_script=request.POST.get('insert_script')
        )
        messages.success(request, 'Table added.')
        return redirect('table_list', module_id=module.id)
    return render(request, 'archival/table_form.html', {'module': module, 'action': 'Add'})

def table_edit(request, module_id, pk):
    table = get_object_or_404(ArchivalTable, pk=pk, module_id=module_id)
    if request.method == 'POST':
        table.table_name = request.POST.get('table_name')
        table.sequence = request.POST.get('sequence')
        table.select_script = request.POST.get('select_script')
        table.insert_script = request.POST.get('insert_script')
        table.save()
        messages.success(request, 'Table updated.')
        return redirect('table_list', module_id=module_id)
    return render(request, 'archival/table_form.html', {'module': table.module, 'table': table, 'action': 'Edit'})

def table_delete(request, module_id, pk):
    table = get_object_or_404(ArchivalTable, pk=pk, module_id=module_id)
    table.delete()
    messages.success(request, 'Table deleted.')
    return redirect('table_list', module_id=module_id)