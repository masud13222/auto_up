from django.urls import path
from . import views

app_name = 'upload'

urlpatterns = [
    path('', views.index, name='index'),
    path('process/', views.process_movie, name='process_movie'),
    path('task/<int:pk>/', views.task_detail, name='task_detail'),
    path('task/<int:pk>/status/', views.task_status_api, name='task_status'),
]
