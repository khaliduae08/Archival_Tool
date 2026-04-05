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
    max_date = models.DateField(null=True, blank=True, help_text="Max date for archival (optional)")
    created_at = models.DateTimeField(auto_now_add=True)

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
    ACCT_SUM_CHOICES = (
        ('Y', 'Yes'),
        ('N', 'No'),
    )
    module = models.ForeignKey(ArchivalModule, on_delete=models.CASCADE, related_name='tables')
    table_name = models.CharField(max_length=200)
    sequence = models.PositiveIntegerField(help_text="Execution order")
    select_script = models.TextField(default='select * from', help_text="select script for source")
    insert_script = models.TextField(default='insert into', help_text="insert script for destination")
    delete_script = models.TextField(default='delete from', help_text="Delete script for destination")
    acct_sum = models.CharField(max_length=1, choices=ACCT_SUM_CHOICES, default='N', help_text="Yes if account summary table?")
    identity_insert = models.BooleanField(default=False, help_text="Enable identity insert for this table?")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['sequence']
        unique_together = ['module', 'table_name']

    def __str__(self):
        return f"{self.module.name} - {self.table_name}"
    

class ArchivalTransaction(models.Model):
    module = models.ForeignKey(ArchivalModule, on_delete=models.CASCADE, related_name='transactions')
    userName = models.CharField(max_length=150)
    archival_date = models.DateField()
    total_execution_time = models.FloatField(help_text="Total time in seconds")
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.module.name} - {self.archival_date}"

class ArchivalTransactionDetail(models.Model):
    transaction = models.ForeignKey(ArchivalTransaction, on_delete=models.CASCADE, related_name='details')
    table_name = models.CharField(max_length=200)
    row_count = models.IntegerField(default=0)
    execution_time = models.FloatField(help_text="Time in seconds")
    archived_ids = models.TextField(blank=True, null=True, help_text="Comma-separated list of IDs (or range)")
    status = models.CharField(max_length=20, default='success')
    error_message = models.TextField(blank=True, null=True)
    
    def __str__(self):
        return f"{self.transaction.module.name} - {self.table_name}"
    


class TempArchivalIds(models.Model):
    run_id = models.CharField(max_length=100)  # unique per archival run
    module_id = models.IntegerField()
    table_name = models.CharField(max_length=200)
    archived_ids = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)