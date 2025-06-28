"""
URL configuration for job_scheduler project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from jobs.views import JobViewSet, JobRunViewSet, JobTemplateViewSet

# Create router and register viewsets
router = DefaultRouter()
router.register(r'jobs', JobViewSet, basename='job')
router.register(r'job-runs', JobRunViewSet, basename='jobrun')
router.register(r'job-templates', JobTemplateViewSet, basename='jobtemplate')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/v1/', include(router.urls)),
    path('api/v1/', include('jobs.urls')),
]

# Admin site customization
admin.site.site_header = "AgriSense Pro - Job Scheduler"
admin.site.site_title = "AgriSense Pro Admin"
admin.site.index_title = "Welcome to AgriSense Pro Job Management"
