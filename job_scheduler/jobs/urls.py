"""
URL configuration for jobs app.
"""
from django.urls import path
from . import views

app_name = 'jobs'

urlpatterns = [
    # Health check endpoint
    path('health/', views.health_check, name='health_check'),
    
    # Dashboard endpoints
    path('dashboard/stats/', views.dashboard_stats, name='dashboard_stats'),
    path('dashboard/recent-activity/', views.recent_activity, name='recent_activity'),
]
