from django.urls import path
from . import views

urlpatterns = [
    # Admin dashboard and management
    path('admin_dashboard/', views.admin_dashboard, name='admin_dashboard'),
    path('users/', views.user_list, name='user_list'),
    path('users/add/', views.user_add, name='user_add'),
    path('users/<int:pk>/edit/', views.user_edit, name='user_edit'),
    path('users/<int:pk>/delete/', views.user_delete, name='user_delete'),
    path('groups/', views.group_list, name='group_list'),
    path('groups/add/', views.group_add, name='group_add'),
    path('groups/<int:pk>/edit/', views.group_edit, name='group_edit'),
    path('groups/<int:pk>/delete/', views.group_delete, name='group_delete'),

    # Home page
    path('', views.home, name='home'),

    # Database connection management    
    path('connections/', views.connection_list, name='connection_list'),
    path('connections/add/', views.connection_add, name='connection_add'),
    path('connections/<int:pk>/edit/', views.connection_edit, name='connection_edit'),
    path('connections/<int:pk>/delete/', views.connection_delete, name='connection_delete'),

    # Application module
    path('applications/', views.application_list, name='application_list'),
    path('applications/add/', views.application_add, name='application_add'),
    path('applications/<int:pk>/edit/', views.application_edit, name='application_edit'),
    path('applications/<int:pk>/delete/', views.application_delete, name='application_delete'),

    # Module management
    path('applications/<int:app_id>/modules/', views.module_list, name='module_list'),
    path('applications/<int:app_id>/modules/add/', views.module_add, name='module_add'),
    path('applications/<int:app_id>/modules/<int:pk>/edit/', views.module_edit, name='module_edit'),
    path('applications/<int:app_id>/modules/<int:pk>/delete/', views.module_delete, name='module_delete'),
    path('applications/<int:app_id>/modules_run/', views.module_run, name='module_run'),

    # Table management under module
    path('modules/<int:module_id>/tables/', views.table_list, name='table_list'),
    path('modules/<int:module_id>/tables/add/', views.table_add, name='table_add'),
    path('modules/<int:module_id>/tables/<int:pk>/edit/', views.table_edit, name='table_edit'),
    path('modules/<int:module_id>/tables/<int:pk>/delete/', views.table_delete, name='table_delete'),

    # Archival history
    path('transactions/', views.transaction_list, name='transaction_list'),
    path('transactions/<int:transaction_id>/', views.transaction_detail, name='transaction_detail'),

    # Audit log
    path('audit_log/', views.audit_log, name='audit_log'),

    # AJAX endpoints
    path('api/get_tables/<int:module_id>/', views.get_module_tables, name='get_tables'),
    path('api/run_script/<int:table_id>/', views.run_table_script, name='run_script'),
    path('api/reset_module_status/<int:module_id>/', views.reset_module_status, name='reset_module_status'),
    path('api/update_module_date/<int:module_id>/', views.update_module_date, name='update_module_date'),
    path('api/count_table/<int:table_id>/', views.get_table_count, name='count_table'),
    path('api/save_archival_history/<int:module_id>/', views.save_archival_history, name='save_archival_history'),
]