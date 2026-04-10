import time
from django.utils.deprecation import MiddlewareMixin
from django.contrib.auth.models import AnonymousUser
from .models import AuditLog

class AuditLogMiddleware(MiddlewareMixin):
    def process_request(self, request):        
        if request.path.startswith('/static/') or request.path.startswith('/admin/'):
            return None
        request._audit_start = time.time()
        return None

    def process_response(self, request, response):        
        if hasattr(request, '_audit_start') and request.user and not isinstance(request.user, AnonymousUser):
            # Determine action based on HTTP method
            method = request.method
            if method == 'GET':
                action = 'VIEW'
            elif method == 'POST':                
                if 'delete' in request.path:
                    action = 'DELETE'
                elif 'edit' in request.path or 'update' in request.path:
                    action = 'UPDATE'
                else:
                    action = 'CREATE'
            elif method == 'PUT':
                action = 'UPDATE'
            elif method == 'DELETE':
                action = 'DELETE'
            else:
                action = 'VIEW'

            
            path_parts = request.path.strip('/').split('/')
            module = path_parts[0] if path_parts else 'unknown'

            AuditLog.objects.create(
                user=request.user,
                action=action,
                module=module,
                details=f"{request.method} {request.path}",
                ip_address=get_client_ip(request),
                user_agent=request.META.get('HTTP_USER_AGENT', '')[:255],
                success=response.status_code < 400,
                error_message=response.reason_phrase if response.status_code >= 400 else ''
            )
        return response

def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip