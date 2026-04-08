from django.conf import settings

def session_timeout(request):
    """Make SESSION_COOKIE_AGE available to all templates."""
    return {
        'SESSION_COOKIE_AGE': getattr(settings, 'SESSION_COOKIE_AGE', 1800)
    }