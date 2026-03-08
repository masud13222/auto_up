from django.urls import path
from . import views

app_name = 'upload'

urlpatterns = [
    path('process/', views.process_media, name='process_media'),
]
