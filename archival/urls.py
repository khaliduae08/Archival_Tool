from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('connections/', views.edit_connections, name='connections'),

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

    # Table management under module
    path('modules/<int:module_id>/tables/', views.table_list, name='table_list'),
    path('modules/<int:module_id>/tables/add/', views.table_add, name='table_add'),
    path('modules/<int:module_id>/tables/<int:pk>/edit/', views.table_edit, name='table_edit'),
    path('modules/<int:module_id>/tables/<int:pk>/delete/', views.table_delete, name='table_delete'),

    # AJAX endpoints
    path('api/get_tables/<int:module_id>/', views.get_module_tables, name='get_tables'),
    path('api/run_script/<int:table_id>/', views.run_table_script, name='run_script'),
]