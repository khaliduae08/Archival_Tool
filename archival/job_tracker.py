import threading
 
_jobs = {}
_jobs_lock = threading.Lock()
 
 
def create_job(job_id):
    with _jobs_lock:
        _jobs[job_id] = {'status': 'running', 'result': None, 'progress': None}
 
 
def update_job(job_id, status, result=None):
    with _jobs_lock:
        existing = _jobs.get(job_id, {})
        _jobs[job_id] = {
            'status': status,
            'result': result,
            'progress': existing.get('progress')  
        }
 
 
def update_progress(job_id, progress):
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]['progress'] = progress
 
 
def get_job(job_id):
    with _jobs_lock:
        return _jobs.get(job_id)