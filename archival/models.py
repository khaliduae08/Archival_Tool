from django.db import models

class DatabaseConnection(models.Model):
    CONN_TYPES = (
        ('source', 'Source Database'),
        ('destination', 'Destination Database'),
    )
    name = models.CharField(max_length=20, choices=CONN_TYPES, unique=True)
    server = models.CharField(max_length=255)
    database = models.CharField(max_length=255)
    username = models.CharField(max_length=255, blank=True, null=True)
    password = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.server}/{self.database}"
    
class Application(models.Model):
    TRANSFER_CHOICES = (
        ('direct','Direct Insert'),
        ('bcp','Bulk Copy'),
        ('inline','Same server'),
    )
    name = models.CharField(max_length=100, unique=True)
    src_conn = models.ForeignKey(DatabaseConnection, on_delete=models.SET_NULL, null=True, blank=True, related_name='source_apps', limit_choices_to={'name': 'source'})
    dstn_conn = models.ForeignKey(DatabaseConnection, on_delete=models.SET_NULL, null=True, blank=True, related_name='dstn_apps', limit_choices_to={'name': 'destination'})
    volume = models.PositiveIntegerField(default=1000, help_text="Number of records per batch")
    select_session = models.PositiveSmallIntegerField(default=1, help_text="Number of parallel session for reading")
    target_session = models.PositiveSmallIntegerField(default=1, help_text="Number of parallel session for insert")
    transfer_method = models.CharField(max_length=20, choices=TRANSFER_CHOICES, default='direct')

    def __str__(self):
        return self.name

class ArchivalModule(models.Model):
    application = models.ForeignKey(Application, on_delete=models.SET_NULL, null=True, related_name='modules')
    name = models.CharField(max_length=200, unique=True)
    last_archival_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['application', 'name']

    def __str__(self):
        return f"{self.application.name} - {self.name}"

class ArchivalTable(models.Model):
    module = models.ForeignKey(ArchivalModule, on_delete=models.CASCADE, related_name='tables')
    table_name = models.CharField(max_length=200)
    sequence = models.PositiveIntegerField(help_text="Execution order")
    select_script = models.TextField(default='select * from', help_text="select script for source")
    insert_script = models.TextField(default='insert into', help_text="insert script for destination")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['sequence']
        unique_together = ['module', 'table_name']

    def __str__(self):
        return f"{self.module.name} - {self.table_name}"
    